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

const MIGRATION_NAME = 'contact_history';

interface HistoryEvent {
  type: 'call' | 'text' | 'inquiry' | 'contact_merge' | 'formal_affirmation' | 'lead_send';
  timestamp: Date;
  description: string;
  sourceId: number;
  sourceTable: string;
  careRecipientId: number;
}

interface DirCareRecipient {
  id: number;
  created_at: Date;
}

interface ContactHistorySummary {
  summary: string;
  lastContactedAt: Date | null;
  lastDealSentAt: Date | null;
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
  console.log();

  // Resolve care seeker IDs → care recipient IDs
  console.log('Resolving care seeker IDs to care recipient IDs...');
  const careRecipients = await fetchCareRecipientsByCareSeekerIds(mysqlConn, careSeekerIds);
  console.log(`✓ Found ${careRecipients.length} care recipients for ${careSeekerIds.length} care seekers`);
  console.log();

  if (careRecipients.length === 0) {
    console.log('No care recipients found for provided care seeker IDs');
    return;
  }

  // Fetch MM data
  console.log('Fetching MM data...');
  const mmDataMap = await fetchMMDataForBatch(pgClient, careRecipients);
  console.log(`✓ Found ${Object.keys(mmDataMap).length} care recipients in MM`);
  console.log();

  // Fetch events
  console.log('Fetching history events...');
  const allEventsMap = await fetchAllEventsForBatch(mysqlConn, careRecipients, mmDataMap);
  console.log(`✓ Fetched events for ${Object.keys(allEventsMap).length} care recipients`);
  console.log();

  // Process and prepare bulk updates
  const bulkUpdates: Array<{
    id: string;
    summary: string;
    lastContactedAt: Date | null;
    lastDealSentAt: Date | null;
  }> = [];

  let success = 0;
  let skipped = 0;

  for (const dirCr of careRecipients) {
    const mmInfo = mmDataMap[dirCr.id];
    const events = allEventsMap[dirCr.id] || [];

    if (!mmInfo) {
      skipped++;
      console.log(`  ⊘ Care recipient ${dirCr.id}: Not in MM`);
      continue;
    }

    if (events.length === 0) {
      skipped++;
      console.log(`  ⊘ Care recipient ${dirCr.id}: No events`);
      continue;
    }

    const { summary, lastContactedAt, lastDealSentAt } = buildContactHistorySummary(events);

    bulkUpdates.push({
      id: mmInfo.id,
      summary,
      lastContactedAt,
      lastDealSentAt,
    });

    success++;
    console.log(`  ✓ Care recipient ${dirCr.id}: Prepared (${events.length} events)`);
  }

  console.log();
  console.log(`Summary: ${success} prepared, ${skipped} skipped`);
  console.log();

  if (bulkUpdates.length > 0 && !options.dryRun) {
    console.log(`Updating ${bulkUpdates.length} care recipients in MM...`);
    await bulkUpdateMM(pgClient, bulkUpdates);
    console.log('✓ Update complete');
  } else if (options.dryRun) {
    console.log(`[DRY RUN] Would update ${bulkUpdates.length} care recipients`);
  }
}

/**
 * Fetch care recipients by care seeker IDs (contact IDs)
 */
async function fetchCareRecipientsByCareSeekerIds(
  mysqlConn: any,
  careSeekerIds: number[]
): Promise<DirCareRecipient[]> {
  if (careSeekerIds.length === 0) return [];

  const placeholders = careSeekerIds.map(() => '?').join(', ');
  const query = `
    SELECT DISTINCT
      c.care_recipient_id AS id,
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
): Promise<Record<number, { id: string; mldmMigratedModmonAt: Date | null }>> {
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
  const mmDataMap: Record<number, { id: string; mldmMigratedModmonAt: Date | null }> = {};
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
  mmDataMap: Record<number, { id: string; mldmMigratedModmonAt: Date | null }>
): Promise<Record<number, HistoryEvent[]>> {
  const careRecipientIds = careRecipients.map(cr => cr.id);

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

  // For each care recipient, filter by mldmMigratedModmonAt, sort, and take top 10
  for (const crId of careRecipientIds) {
    let events = eventsMap[crId] || [];
    const mmInfo = mmDataMap[crId];
    const migratedAt = mmInfo?.mldmMigratedModmonAt;

    // Filter events after mldmMigratedModmonAt if it exists
    if (migratedAt) {
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

/**
 * Build contact history summary from events
 */
function buildContactHistorySummary(events: HistoryEvent[]): ContactHistorySummary {
  // Events are already sorted and limited to top 10

  // 1. lastContactedAt = most recent event timestamp
  const lastContactedAt = events.length > 0 ? events[0].timestamp : null;

  // 2. lastDealSentAt = most recent lead_send timestamp
  const lastDealSentAt = events.find((e) => e.type === 'lead_send')?.timestamp || null;

  // 3. Build summary string (max 1000 chars)
  const MAX_LENGTH = 1000;
  const TRUNCATED_SUFFIX = '... (truncated)';
  let summary = '';

  for (const event of events) {
    const dateStr = formatEventDate(event.timestamp);
    const line = `[${event.type.toUpperCase()}] ${event.description} - ${dateStr}\n`;

    // Check if adding this line would exceed max length
    if (summary.length + line.length > MAX_LENGTH - TRUNCATED_SUFFIX.length) {
      // Add truncation marker if we have room
      if (summary.length + TRUNCATED_SUFFIX.length <= MAX_LENGTH) {
        summary += TRUNCATED_SUFFIX;
      }
      break;
    }
    summary += line;
  }

  // Final safety check - truncate if somehow still over limit
  if (summary.length > MAX_LENGTH) {
    summary = summary.substring(0, MAX_LENGTH - TRUNCATED_SUFFIX.length) + TRUNCATED_SUFFIX;
  }

  return {
    summary: summary.trim().substring(0, MAX_LENGTH), // Extra safety
    lastContactedAt,
    lastDealSentAt,
  };
}

/**
 * Format event date for display
 */
function formatEventDate(date: Date): string {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const month = months[date.getMonth()];
  const day = date.getDate();
  const year = date.getFullYear();
  return `${month} ${day}, ${year}`;
}

// Run migration
migrateContactHistory().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});

