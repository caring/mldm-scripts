import { promises as fs } from 'fs';
import { connectMySQL, disconnectMySQL, getMySQLConnection } from '../../db/mysql';
import { connectPostgres, disconnectPostgres, getPostgresClient } from '../../db/postgres';
import { parseTimeParam, formatDate, getRelativeDescription } from '../../utils/time-utils';
import {
  fetchCallsForBatch,
  fetchTextsForBatch,
  fetchInquiriesForBatch,
  fetchInquiryLogsForBatch,
  fetchFormalAffirmationsForBatch,
  fetchLeadSendsForBatch,
} from './batch-fetchers';
import {
  ensureMigrationDir,
  appendBatch,
  appendRow,
  getMigrationStatePath,
  readBatches,
  readRows,
  readSummary,
  writeSummary,
  RowRecord,
} from '../../utils/file-utils';
import {
  parseMigrationArgs,
  printMigrationHelp,
  isHelpRequested,
  printMigrationOptions,
  MigrationCLIOptions,
  parseIds,
} from '../../utils/migration-cli';
import {
  buildContactHistorySummary,
  buildIdBatchClassificationFilename,
  buildIdBatchClassificationReport,
  chunkIds,
  ContactHistoryIdClassificationRow,
  classifyIdBasedCareRecipient,
  getIdMigrationAction,
  HistoryEvent,
  MMCareRecipientData,
} from './contact-history-helpers';

const MIGRATION_NAME = 'contact_history';
const ID_BASED_BATCH_SIZE = 1000;

interface DirCareRecipient {
  id: number;
  created_at: Date;
}

interface ResolvedCareSeekerInput {
  inputId: number;
  careRecipientId: number;
  created_at: Date;
}

/**
 * Main migration function
 */
async function migrateContactHistory() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:contact-history');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Contact History Migration ===\n');
  printMigrationOptions(options);

  try {
    await ensureMigrationDir(MIGRATION_NAME);

    if (options.report) {
      await generateReport();
      return;
    }

    if (options.retryFailed) {
      console.log('Retry failed functionality - to be implemented');
      return;
    }

    await runMigration(options);

  } catch (error) {
    console.error('Migration failed:', error);
    throw error;
  }
}

async function generateReport() {
  console.log('=== Migration Report ===\n');

  const summary = await readSummary();
  const migrationSummary = summary[MIGRATION_NAME];

  if (!migrationSummary) {
    console.log('No migration data found');
    return;
  }

  console.log('Status:', migrationSummary.status);
  if (migrationSummary.time_range) {
    console.log('Time range:');
    console.log(`  From: ${migrationSummary.time_range.from}`);
    if (migrationSummary.time_range.to) {
      console.log(`  To: ${migrationSummary.time_range.to}`);
    }
  }
  console.log();

  console.log('Progress:');
  console.log(`  Batches: ${migrationSummary.completed_batches} / ${migrationSummary.total_batches}`);
  console.log(`  Batch size: ${migrationSummary.batch_size}`);
  console.log();

  console.log('Results:');
  console.log(`  Total rows fetched: ${migrationSummary.total_rows_fetched}`);
  console.log(`  ✓ Success: ${migrationSummary.total_success}`);
  console.log(`  ⊘ Skipped: ${migrationSummary.total_skipped}`);
  console.log(`  ✗ Failed: ${migrationSummary.total_failed}`);
  console.log();

  console.log('Timestamps:');
  console.log(`  Started: ${migrationSummary.started_at}`);
  console.log(`  Last updated: ${migrationSummary.last_updated_at}`);
}

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
    // Check if --ids parameter is provided (care seeker IDs)
    const careSeekerIds = await parseIds(options);

    if (careSeekerIds && careSeekerIds.length > 0) {
      // ID-based migration
      await runIdBasedMigration(mysqlConn, pgClient, careSeekerIds, options);
      return;
    }

    // Time-based migration (default)
    const fromDate = parseTimeParam(options.from);
    const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

    console.log('Migration scope:');
    console.log(`  Care recipients created after: ${formatDate(fromDate)}`);
    if (toDate) {
      console.log(`  Care recipients created before: ${formatDate(toDate)}`);
      console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
    }
    console.log();

    // Load existing progress
    const batches = await readBatches(MIGRATION_NAME);

    // Read rows from all batch files
    const processedIds: Set<number> = new Set();
    for (const batch of batches) {
      const batchRows = await readRows(`${MIGRATION_NAME}/${batch.batch_id}`);
      batchRows.forEach((r: RowRecord) => processedIds.add(parseInt(r.source_id, 10)));
    }

    console.log('Existing progress:');
    console.log(`  Batches: ${batches.length}`);
    console.log(`  Care recipients processed: ${processedIds.size}`);
    console.log();

    // Initialize summary
    let summary = await readSummary();
    if (!summary[MIGRATION_NAME]) {
      summary[MIGRATION_NAME] = {
        status: 'in_progress',
        time_range: {
          from: formatDate(fromDate),
          to: toDate ? formatDate(toDate) : null,
          to_relative: toDate ? getRelativeDescription(fromDate, toDate) : undefined,
        },
        batch_size: options.batchSize,
        total_batches: 0,
        completed_batches: 0,
        total_rows_fetched: 0,
        total_success: 0,
        total_skipped: 0,
        total_duplicate: 0,
        total_failed: 0,
        started_at: formatDate(new Date()),
        last_updated_at: null,
      };
      await writeSummary(summary);
    }

    // Start batch processing
    // Calculate offset from actual fetched counts (not batch size, in case batch size changed)
    let offset = batches.reduce((sum, batch) => sum + batch.fetched_count, 0);
    let batchNumber = batches.length + 1;
    let hasMore = true;

    console.log(`Resuming from offset: ${offset} (${batches.length} batches completed)`);

    while (hasMore) {
      const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
      console.log(`\n=== Processing ${batchId} ===`);

      // Ensure batch directory exists
      if (!options.dryRun) {
        await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
      }

      // Fetch batch of care recipients from DIR
      const dirCareRecipients = await fetchCareRecipientBatch(
        mysqlConn,
        fromDate,
        toDate,
        options.batchSize,
        offset
      );

      if (dirCareRecipients.length === 0) {
        console.log('No more care recipients to process');
        hasMore = false;
        break;
      }

      console.log(`Fetched ${dirCareRecipients.length} care recipients from DIR`);

      // Filter out already processed
      const unprocessedCareRecipients = dirCareRecipients.filter(cr => !processedIds.has(cr.id));
      console.log(`Processing ${unprocessedCareRecipients.length} care recipients (${dirCareRecipients.length - unprocessedCareRecipients.length} already processed)`);

      if (unprocessedCareRecipients.length === 0) {
        console.log('All care recipients in this batch already processed, moving to next batch');
        offset += options.batchSize;
        batchNumber++;
        continue;
      }

      // OPTIMIZATION: Fetch all MM data for batch upfront (1 query instead of 1000)
      console.log('Fetching MM data for batch...');
      const mmDataMap = await fetchMMDataForBatch(pgClient, unprocessedCareRecipients);
      console.log(`✓ Fetched MM data for ${Object.keys(mmDataMap).length} care recipients`);

      // OPTIMIZATION: Fetch all events for batch (6 queries instead of 6000)
      console.log('Fetching history events for batch...');
      const allEventsMap = await fetchAllEventsForBatch(mysqlConn, unprocessedCareRecipients, mmDataMap);
      console.log(`✓ Fetched events for ${Object.keys(allEventsMap).length} care recipients`);

      // Track batch stats
      let batchSuccess = 0;
      let batchSkipped = 0;
      let batchFailed = 0;

      // Prepare bulk updates
      const bulkUpdates: Array<{
        id: string;
        summary: string;
        lastContactedAt: Date | null;
        lastDealSentAt: Date | null;
      }> = [];

      // Process each care recipient (in-memory, no DB queries!)
      for (const dirCr of unprocessedCareRecipients) {
        try {
          const mmInfo = mmDataMap[dirCr.id];
          const events = allEventsMap[dirCr.id] || [];

          // Check if care recipient exists in MM
          if (!mmInfo) {
            batchSkipped++;
            console.log(`  ⊘ Care recipient ${dirCr.id}: Not in MM`);

            if (!options.dryRun) {
              await appendRow(`${MIGRATION_NAME}/${batchId}`, {
                batch_id: batchId,
                source_id: dirCr.id.toString(),
                status: 'skipped' as any,
                reason: 'not_in_mm',
                error: undefined,
                processed_at: formatDate(new Date()),
              });
            }
            continue;
          }

          // Check if there are new events
          if (events.length === 0) {
            batchSkipped++;
            const reason = mmInfo.mldmMigratedModmonAt ? 'no_new_events' : 'no_events';
            console.log(`  ⊘ Care recipient ${dirCr.id}: ${reason}`);

            if (!options.dryRun) {
              await appendRow(`${MIGRATION_NAME}/${batchId}`, {
                batch_id: batchId,
                source_id: dirCr.id.toString(),
                status: 'skipped' as any,
                reason,
                error: undefined,
                processed_at: formatDate(new Date()),
              });
            }
            continue;
          }

          // Build summary
          const { summary, lastContactedAt, lastDealSentAt } = buildContactHistorySummary(events);

          // Add to bulk update
          bulkUpdates.push({
            id: mmInfo.id,
            summary,
            lastContactedAt,
            lastDealSentAt,
          });

          batchSuccess++;
          console.log(`  ✓ Care recipient ${dirCr.id}: Prepared for update (${events.length} events)`);

          if (!options.dryRun) {
            await appendRow(`${MIGRATION_NAME}/${batchId}`, {
              batch_id: batchId,
              source_id: dirCr.id.toString(),
              status: 'success' as any,
              reason: undefined,
              error: undefined,
              processed_at: formatDate(new Date()),
            });
          }

          processedIds.add(dirCr.id);

        } catch (error: any) {
          batchFailed++;
          console.error(`  ✗ Care recipient ${dirCr.id}: Failed -`, error.message);

          if (!options.dryRun) {
            await appendRow(`${MIGRATION_NAME}/${batchId}`, {
              batch_id: batchId,
              source_id: dirCr.id.toString(),
              status: 'failed' as any,
              reason: undefined,
              error: error.message,
              processed_at: formatDate(new Date()),
            });
          }
        }
      }

      // OPTIMIZATION: Bulk update MM (1 query instead of 1000)
      if (bulkUpdates.length > 0 && !options.dryRun) {
        console.log(`\nPerforming bulk update for ${bulkUpdates.length} care recipients...`);
        await bulkUpdateMM(pgClient, bulkUpdates);
        console.log(`✓ Bulk update complete`);
      }

      // Log batch with date range info
      if (!options.dryRun) {
        const firstCrDate = dirCareRecipients.length > 0 ? formatDate(dirCareRecipients[0].created_at) : null;
        const lastCrDate = dirCareRecipients.length > 0 ? formatDate(dirCareRecipients[dirCareRecipients.length - 1].created_at) : null;

        await appendBatch(MIGRATION_NAME, {
          batch_id: batchId,
          status: 'completed',
          query: {
            from: formatDate(fromDate),
            to: toDate ? formatDate(toDate) : null,
            last_created_at: lastCrDate,
            last_id: dirCareRecipients.length > 0 ? dirCareRecipients[dirCareRecipients.length - 1].id.toString() : null,
            limit: options.batchSize,
          },
          fetched_count: dirCareRecipients.length,
          started_at: formatDate(new Date()),
          completed_at: formatDate(new Date()),
        });

        console.log(`Date range: ${firstCrDate} → ${lastCrDate}`);
      }

      // Update summary
      summary = await readSummary();
      summary[MIGRATION_NAME].completed_batches = batchNumber;
      summary[MIGRATION_NAME].total_rows_fetched += dirCareRecipients.length;
      summary[MIGRATION_NAME].total_success += batchSuccess;
      summary[MIGRATION_NAME].total_skipped += batchSkipped;
      summary[MIGRATION_NAME].total_failed += batchFailed;
      summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
      await writeSummary(summary);

      console.log(`\nBatch ${batchId} Summary:`);
      console.log(`  ✓ Success: ${batchSuccess}`);
      console.log(`  ⊘ Skipped: ${batchSkipped}`);
      console.log(`  ✗ Failed: ${batchFailed}`);

      // Move to next batch
      offset += options.batchSize;
      batchNumber++;
    }

    // Mark as complete
    summary = await readSummary();
    summary[MIGRATION_NAME].status = 'completed';
    summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
    await writeSummary(summary);

    console.log('\n=== Migration Complete ===');
    console.log(`Total rows fetched: ${summary[MIGRATION_NAME].total_rows_fetched}`);
    console.log(`✓ Success: ${summary[MIGRATION_NAME].total_success}`);
    console.log(`⊘ Skipped: ${summary[MIGRATION_NAME].total_skipped}`);
    console.log(`✗ Failed: ${summary[MIGRATION_NAME].total_failed}`);

  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
  }
}

/**
 * Run migration for specific care seeker IDs
 */
async function runIdBasedMigration(
  mysqlConn: any,
  pgClient: any,
  careSeekerIds: number[],
  options: MigrationCLIOptions
) {
  console.log(`Migration scope: ${careSeekerIds.length} care seeker IDs (legacy contact IDs)`);
  console.log(`Fixed ID batch size: ${ID_BASED_BATCH_SIZE}`);
  console.log();

  const idBatches = chunkIds(careSeekerIds, ID_BASED_BATCH_SIZE);
  let totalPrepared = 0;
  let totalSkipped = 0;
  let totalDone = 0;
  let totalNotDone = 0;
  let totalNeedsRefresh = 0;
  let totalNotInMM = 0;
  let totalNoEvents = 0;
  let totalNotFound = 0;

  for (const [batchIndex, idBatch] of idBatches.entries()) {
    const batchId = `id_batch_${String(batchIndex + 1).padStart(6, '0')}`;
    console.log(`=== Processing ${batchId} (${idBatch.length} IDs) ===`);

    await ensureMigrationDir(`${MIGRATION_NAME}/id-based/${batchId}`);

    console.log('Resolving care seeker IDs to care recipient IDs...');
    const resolvedInputs = await fetchCareRecipientsByCareSeekerIds(mysqlConn, idBatch);
    console.log(`✓ Resolved ${resolvedInputs.length} input IDs to DIR care recipients`);

    const resolvedByInputId = new Map<number, ResolvedCareSeekerInput>();
    for (const resolvedInput of resolvedInputs) {
      resolvedByInputId.set(resolvedInput.inputId, resolvedInput);
    }

    const careRecipients = buildDistinctCareRecipients(resolvedInputs);

    let mmDataMap: Record<number, MMCareRecipientData> = {};
    let allEventsMap: Record<number, HistoryEvent[]> = {};

    if (careRecipients.length > 0) {
      console.log('Fetching MM data...');
      mmDataMap = await fetchMMDataForBatch(pgClient, careRecipients);
      console.log(`✓ Found ${Object.keys(mmDataMap).length} care recipients in MM`);

      console.log('Fetching history events...');
      allEventsMap = await fetchAllEventsForBatch(
        mysqlConn,
        careRecipients,
        mmDataMap,
        { filterAfterMigratedModmonAt: false }
      );
      console.log(`✓ Fetched events for ${Object.keys(allEventsMap).length} care recipients`);
    }

    const classificationRows: ContactHistoryIdClassificationRow[] = [];
    const actionableUpdatesByCareRecipient = new Map<number, {
      id: string;
      summary: string;
      lastContactedAt: Date | null;
      lastDealSentAt: Date | null;
    }>();

    let batchPrepared = 0;
    let batchSkipped = 0;
    let batchDone = 0;
    let batchNotDone = 0;
    let batchNeedsRefresh = 0;
    let batchNotInMM = 0;
    let batchNoEvents = 0;
    let batchNotFound = 0;

    for (const inputId of idBatch) {
      const resolvedInput = resolvedByInputId.get(inputId);

      if (!resolvedInput) {
        classificationRows.push({
          inputId,
          dirCareRecipientId: null,
          mmCareRecipientId: null,
          state: 'not_found',
          action: getIdMigrationAction('not_found'),
          reason: 'Input ID not found in DIR contacts',
          mldmMigratedModmonAt: null,
          eventsConsidered: 0,
        });
        batchSkipped++;
        batchNotFound++;
        continue;
      }

      const mmInfo = mmDataMap[resolvedInput.careRecipientId];
      const summaryEvents = allEventsMap[resolvedInput.careRecipientId] || [];
      const decision = classifyIdBasedCareRecipient(mmInfo, summaryEvents);

      classificationRows.push({
        inputId,
        dirCareRecipientId: resolvedInput.careRecipientId,
        mmCareRecipientId: mmInfo?.id ?? null,
        state: decision.state,
        action: getIdMigrationAction(decision.state),
        reason: getClassificationReason(decision.state),
        mldmMigratedModmonAt: mmInfo?.mldmMigratedModmonAt
          ? new Date(mmInfo.mldmMigratedModmonAt).toISOString()
          : null,
        eventsConsidered: summaryEvents.length,
      });

      if (decision.state === 'done') {
        batchSkipped++;
        batchDone++;
        continue;
      }

      if (decision.state === 'not_in_mm') {
        batchSkipped++;
        batchNotInMM++;
        continue;
      }

      if (decision.state === 'no_events') {
        batchSkipped++;
        batchNoEvents++;
        continue;
      }

      if (!mmInfo || actionableUpdatesByCareRecipient.has(resolvedInput.careRecipientId)) {
        continue;
      }

      const { summary, lastContactedAt, lastDealSentAt } = buildContactHistorySummary(decision.summaryEvents);
      actionableUpdatesByCareRecipient.set(resolvedInput.careRecipientId, {
        id: mmInfo.id,
        summary,
        lastContactedAt,
        lastDealSentAt,
      });

      batchPrepared++;
      if (decision.state === 'not_done') {
        batchNotDone++;
      } else {
        batchNeedsRefresh++;
      }
    }

    const classificationReport = buildIdBatchClassificationReport(
      batchId,
      classificationRows,
      careRecipients.length
    );
    const classificationFilePath = await writeIdBatchClassificationFile(batchId, classificationReport);
    console.log(`✓ Wrote classification file to ${classificationFilePath}`);

    const bulkUpdates = Array.from(actionableUpdatesByCareRecipient.values());

    console.log(`Batch summary: ${batchPrepared} prepared, ${batchSkipped} skipped`);
    console.log(`  Done: ${batchDone}`);
    console.log(`  Not done: ${batchNotDone}`);
    console.log(`  Needs refresh: ${batchNeedsRefresh}`);
    console.log(`  Not in MM: ${batchNotInMM}`);
    console.log(`  No events: ${batchNoEvents}`);
    console.log(`  Not found: ${batchNotFound}`);

    if (bulkUpdates.length > 0 && !options.dryRun) {
      console.log(`Updating ${bulkUpdates.length} care recipients in MM...`);
      await bulkUpdateMM(pgClient, bulkUpdates);
      console.log('✓ Update complete');
    } else if (options.dryRun) {
      console.log(`[DRY RUN] Would update ${bulkUpdates.length} care recipients`);
    }

    totalPrepared += batchPrepared;
    totalSkipped += batchSkipped;
    totalDone += batchDone;
    totalNotDone += batchNotDone;
    totalNeedsRefresh += batchNeedsRefresh;
    totalNotInMM += batchNotInMM;
    totalNoEvents += batchNoEvents;
    totalNotFound += batchNotFound;
    console.log();
  }

  console.log('=== ID-Based Contact History Migration Summary ===');
  console.log(`Prepared: ${totalPrepared}`);
  console.log(`Skipped: ${totalSkipped}`);
  console.log(`  Done: ${totalDone}`);
  console.log(`  Not done: ${totalNotDone}`);
  console.log(`  Needs refresh: ${totalNeedsRefresh}`);
  console.log(`  Not in MM: ${totalNotInMM}`);
  console.log(`  No events: ${totalNoEvents}`);
  console.log(`  Not found: ${totalNotFound}`);
}

/**
 * Fetch care recipients by care seeker IDs (contact IDs)
 */
async function fetchCareRecipientsByCareSeekerIds(
  mysqlConn: any,
  careSeekerIds: number[]
): Promise<ResolvedCareSeekerInput[]> {
  if (careSeekerIds.length === 0) return [];

  const placeholders = careSeekerIds.map(() => '?').join(', ');
  const query = `
    SELECT
      c.id AS inputId,
      c.care_recipient_id AS careRecipientId,
      cr.created_at
    FROM contacts c
    INNER JOIN care_recipients cr ON cr.id = c.care_recipient_id
    WHERE c.id IN (${placeholders})
      AND c.care_recipient_id IS NOT NULL
      AND c.deleted_at IS NULL
      AND cr.deleted_at IS NULL
    ORDER BY cr.created_at DESC
  `;

  const [rows] = await mysqlConn.query(query, careSeekerIds);
  return rows;
}

function buildDistinctCareRecipients(
  resolvedInputs: ResolvedCareSeekerInput[]
): DirCareRecipient[] {
  const careRecipientsById = new Map<number, DirCareRecipient>();

  for (const resolvedInput of resolvedInputs) {
    if (!careRecipientsById.has(resolvedInput.careRecipientId)) {
      careRecipientsById.set(resolvedInput.careRecipientId, {
        id: resolvedInput.careRecipientId,
        created_at: resolvedInput.created_at,
      });
    }
  }

  return Array.from(careRecipientsById.values());
}

function getClassificationReason(
  state: ContactHistoryIdClassificationRow['state']
): string {
  switch (state) {
    case 'done':
      return 'No new events since last migration';
    case 'not_done':
      return 'Never migrated in MM';
    case 'needs_refresh':
      return 'New events found after last migration';
    case 'not_in_mm':
      return 'DIR care recipient not found in MM';
    case 'no_events':
      return 'No contact history events found';
    case 'not_found':
      return 'Input ID not found in DIR contacts';
    default:
      return 'Unknown classification';
  }
}

async function writeIdBatchClassificationFile(
  batchId: string,
  report: ReturnType<typeof buildIdBatchClassificationReport>
): Promise<string> {
  const fileName = buildIdBatchClassificationFilename(report.generatedAt);
  const filePath = getMigrationStatePath(MIGRATION_NAME, 'id-based', batchId, fileName);
  const tempPath = `${filePath}.tmp`;

  await fs.writeFile(tempPath, JSON.stringify(report, null, 2));
  await fs.rename(tempPath, filePath);

  return filePath;
}

/**
 * Fetch batch of care recipients from DIR
 */
async function fetchCareRecipientBatch(
  mysqlConn: any,
  fromDate: Date,
  toDate: Date | null,
  batchSize: number,
  offset: number
): Promise<DirCareRecipient[]> {
  let query = `
    SELECT
      id,
      created_at
    FROM care_recipients
    WHERE created_at >= ?
  `;
  const params: any[] = [fromDate];

  if (toDate) {
    query += ` AND created_at <= ?`;
    params.push(toDate);
  }

  query += `
      AND deleted_at IS NULL
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
  `;
  params.push(batchSize, offset);

  const [rows] = await mysqlConn.query(query, params);

  return rows;
}

/**
 * Fetch MM data for all care recipients in batch (1 query instead of 1000)
 */
async function fetchMMDataForBatch(
  pgClient: any,
  careRecipients: DirCareRecipient[]
): Promise<Record<number, MMCareRecipientData>> {
  const legacyIds = careRecipients.map(cr => cr.id.toString());

  const result = await pgClient.query(
    `
    SELECT id, "legacyId", "mldmMigratedModmonAt"
    FROM care_recipients
    WHERE "legacyId" = ANY($1)
      AND "deletedAt" IS NULL
    `,
    [legacyIds]
  );

  // Build hashmap: legacyId -> {id, mldmMigratedModmonAt}
  const mmDataMap: Record<number, MMCareRecipientData> = {};
  for (const row of result.rows) {
    mmDataMap[parseInt(row.legacyId, 10)] = {
      id: row.id,
      mldmMigratedModmonAt: row.mldmMigratedModmonAt,
    };
  }

  return mmDataMap;
}

/**
 * Fetch all events for all care recipients in batch (6 queries instead of 6000)
 */
async function fetchAllEventsForBatch(
  mysqlConn: any,
  careRecipients: DirCareRecipient[],
  mmDataMap: Record<number, MMCareRecipientData>,
  options: { filterAfterMigratedModmonAt?: boolean } = {}
): Promise<Record<number, HistoryEvent[]>> {
  const careRecipientIds = careRecipients.map(cr => cr.id);
  const shouldFilterAfterMigratedModmonAt = options.filterAfterMigratedModmonAt ?? true;

  // Fetch from all 6 tables in parallel (6 queries total for entire batch!)
  const [calls, texts, inquiries, inquiryLogs, affirmations, leadSends] = await Promise.all([
    fetchCallsForBatch(mysqlConn, careRecipientIds),
    fetchTextsForBatch(mysqlConn, careRecipientIds),
    fetchInquiriesForBatch(mysqlConn, careRecipientIds),
    fetchInquiryLogsForBatch(mysqlConn, careRecipientIds),
    fetchFormalAffirmationsForBatch(mysqlConn, careRecipientIds),
    fetchLeadSendsForBatch(mysqlConn, careRecipientIds),
  ]);

  // Combine all events
  const allEvents = [...calls, ...texts, ...inquiries, ...inquiryLogs, ...affirmations, ...leadSends];

  // Group events by care_recipient_id
  const eventsMap: Record<number, HistoryEvent[]> = {};
  for (const event of allEvents) {
    if (!eventsMap[event.careRecipientId]) {
      eventsMap[event.careRecipientId] = [];
    }
    eventsMap[event.careRecipientId].push(event);
  }

  // For each care recipient, optionally filter by migration time, then sort and take top 10
  for (const crId of careRecipientIds) {
    let events = eventsMap[crId] || [];
    const mmInfo = mmDataMap[crId];
    const migratedAt = mmInfo?.mldmMigratedModmonAt;

    if (shouldFilterAfterMigratedModmonAt && migratedAt) {
      events = events.filter(e => e.timestamp > migratedAt);
    }

    // Sort by timestamp descending
    events.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());

    // Take top 10
    eventsMap[crId] = events.slice(0, 10);
  }

  return eventsMap;
}

/**
 * Bulk update MM care recipients (1 query instead of 1000)
 */
async function bulkUpdateMM(
  pgClient: any,
  updates: Array<{
    id: string;
    summary: string;
    lastContactedAt: Date | null;
    lastDealSentAt: Date | null;
  }>
): Promise<void> {
  if (updates.length === 0) return;

  // Build VALUES clause
  const values = updates.map((_update, idx) => {
    const baseIdx = idx * 4;
    return `($${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3}, $${baseIdx + 4})`;
  }).join(', ');

  // Flatten parameters
  const params: any[] = [];
  for (const u of updates) {
    params.push(u.id, u.summary, u.lastContactedAt, u.lastDealSentAt);
  }

  const query = `
    UPDATE care_recipients AS cr
    SET
      "legacyContactHistorySummary" = v.summary,
      "legacyLastContactedAt" = v.last_contacted::timestamptz,
      "legacyLastDealSentAt" = v.last_deal_sent::timestamptz,
      "mldmMigratedModmonAt" = NOW(),
      "updatedAt" = NOW()
    FROM (VALUES ${values}) AS v(id, summary, last_contacted, last_deal_sent)
    WHERE cr.id::text = v.id
  `;

  await pgClient.query(query, params);
}

// Old single-care-recipient functions removed - now using batch functions from batch-fetchers.ts

if (require.main === module) {
  migrateContactHistory().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

