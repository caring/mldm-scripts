import { connectMySQL, disconnectMySQL, getMySQLConnection } from '../../db/mysql';
import { connectPostgres, disconnectPostgres, getPostgresClient } from '../../db/postgres';
import { parseTimeParam, formatDate, getRelativeDescription } from '../../utils/time-utils';
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
import {
  fetchCareRecipientLeadsByDateRange,
  resolveExplicitCareRecipientLeads,
} from '../../utils/care-recipient-lead-selection';

const MIGRATION_NAME = 'lead_status_tour_history';

interface DirLead {
  id: number;
  created_at: Date;
  followup_rank: number | null;
  allowFollowup: boolean | number | string | null;
}

export interface DirLeadStatus {
  lead_id: number;
  created_at: Date;
  status: string;
  sub_status: string | null;
  tour_date: string | null;
  tour_time: string | null;
  source: string | null;
  account_name: string | null;
}

interface MMLeadData {
  id: string;
  mldmMigratedModmonAt: Date | null;
}

async function migrateLeadStatusTourHistory() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:lead-status-tour-history');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Lead Status & Tour History Migration ===\n');
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

/**
 * Detect migration mode based on options
 */
async function detectMigrationMode(
  options: MigrationCLIOptions
): Promise<'care_seeker' | 'explicit_leads' | 'incremental_status' | 'full_leads'> {
  // Mode 1: Care seeker-based (NEW)
  if (options.idsFile || options.idsInline) {
    // Check if IDs look like care seeker IDs (we'll parse to determine)
    await parseIds(options);
    // For now, assume care seeker IDs if --ids is provided
    // (could enhance to detect lead IDs vs care seeker IDs)
    return 'care_seeker';
  }

  // Mode 2: Explicit lead IDs (existing)
  // This is handled by resolveExplicitCareRecipientLeads

  // Mode 3: Incremental status update (NEW)
  const fromDate = parseTimeParam(options.from);
  const daysSinceFrom = (Date.now() - fromDate.getTime()) / (1000 * 60 * 60 * 24);
  if (daysSinceFrom < 60) { // Less than 2 months = incremental
    return 'incremental_status';
  }

  // Mode 4: Full lead migration (existing - by creation date)
  return 'full_leads';
}

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
    // Detect migration mode
    const mode = await detectMigrationMode(options);

    if (mode === 'care_seeker') {
      await runCareSeekerBasedMigration(mysqlConn, pgClient, options);
    } else if (mode === 'incremental_status') {
      await runIncrementalStatusMigration(mysqlConn, pgClient, options);
    } else {
      // Existing logic for explicit_leads and full_leads modes
      await runExistingMigration(mysqlConn, pgClient, options);
    }
  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
    console.log('\n✓ Disconnected from databases');
  }
}

/**
 * Existing migration logic (explicit leads or full leads by creation date)
 */
async function runExistingMigration(
  mysqlConn: any,
  pgClient: any,
  options: MigrationCLIOptions
) {
  const explicitResolution = await resolveExplicitCareRecipientLeads(options, pgClient);
  const explicitLeadIds = explicitResolution?.matchedLeads.map((lead) => lead.legacyId) || null;
  const fromDate = parseTimeParam(options.from);
  const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

    console.log('Migration scope:');
    if (explicitResolution) {
      console.log(`  Explicit lead IDs provided: ${explicitResolution.requestedCount}`);
      console.log(`  Matching MM care_recipient_leads found: ${explicitResolution.matchedLeads.length}`);
      console.log('  Source: --ids-inline / --ids file (supports CSV with legacyId or id columns)');
    } else {
      console.log(`  MM leads created before: ${formatDate(fromDate)}`);
      if (toDate) {
        console.log(`  MM leads created after: ${formatDate(toDate)}`);
        console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
      }
    }
    console.log();

    const batches = await readBatches(MIGRATION_NAME);

    const processedIds: Set<number> = new Set();
    for (const batch of batches) {
      const batchRows = await readRows(`${MIGRATION_NAME}/${batch.batch_id}`);
      batchRows.forEach((r: RowRecord) => processedIds.add(parseInt(r.source_id, 10)));
    }

    console.log('Existing progress:');
    console.log(`  Batches: ${batches.length}`);
    console.log(`  Leads processed: ${processedIds.size}`);
    console.log();

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

    let batchNumber = batches.length + 1;
    if (explicitLeadIds) {
      const unprocessedLeadIds = explicitLeadIds.filter(id => !processedIds.has(id));
      let explicitIndex = 0;
      const totalExplicit = unprocessedLeadIds.length;

      console.log(`Resuming explicit ID migration: ${totalExplicit} unprocessed IDs (${batches.length} batches completed)`);

      while (explicitIndex < totalExplicit) {
        const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
        console.log(`\n=== Processing ${batchId} ===`);

        if (!options.dryRun) {
          await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
        }

        const leadIdsBatch = unprocessedLeadIds.slice(explicitIndex, explicitIndex + options.batchSize);
        const dirLeads = await fetchLeadsByIds(mysqlConn, leadIdsBatch);

        console.log(`Fetched ${dirLeads.length} leads from DIR for ${leadIdsBatch.length} explicit IDs`);

        await processLeadBatch({
          options,
          batchId,
          fromDate,
          toDate,
          batchNumber,
          dirLeads,
          processedIds,
          mysqlConn,
          pgClient,
        });

        explicitIndex += leadIdsBatch.length;
        batchNumber++;
      }
    } else {
      let offset = batches.reduce((sum, batch) => sum + batch.fetched_count, 0);
      let hasMore = true;

      console.log(`Resuming from offset: ${offset} (${batches.length} batches completed)`);

      while (hasMore) {
      const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
      console.log(`\n=== Processing ${batchId} ===`);

      if (!options.dryRun) {
        await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
      }

      const dirLeads = await fetchLeadBatchFromMM(pgClient, mysqlConn, fromDate, toDate, options.batchSize, offset);

      if (dirLeads.length === 0) {
        console.log('No more leads to process');
        hasMore = false;
        break;
      }

      console.log(`Fetched ${dirLeads.length} leads from DIR`);
      await processLeadBatch({
        options,
        batchId,
        fromDate,
        toDate,
        batchNumber,
        dirLeads,
        processedIds,
        mysqlConn,
        pgClient,
      });

      offset += options.batchSize;
      batchNumber++;
    }
    }

    summary = await readSummary();
    summary[MIGRATION_NAME].status = 'completed';
    summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
    await writeSummary(summary);

    console.log('\n=== Migration Complete ===');
    console.log(`Total rows fetched: ${summary[MIGRATION_NAME].total_rows_fetched}`);
    console.log(`✓ Success: ${summary[MIGRATION_NAME].total_success}`);
    console.log(`⊘ Skipped: ${summary[MIGRATION_NAME].total_skipped}`);
    console.log(`✗ Failed: ${summary[MIGRATION_NAME].total_failed}`);
}

/**
 * Incremental status update migration
 * (Only processes leads with new statuses since last run)
 */
async function runIncrementalStatusMigration(
  mysqlConn: any,
  pgClient: any,
  options: MigrationCLIOptions
) {
  console.log('=== Incremental Status Update Migration ===\n');

  const fromDate = parseTimeParam(options.from);
  console.log(`Finding leads with new statuses created after: ${formatDate(fromDate)}`);
  console.log();

  // Fetch affected lead IDs (leads with new statuses)
  const affectedLeadIds = await fetchAffectedLeadsFromNewStatuses(mysqlConn, fromDate);
  console.log(`✓ Found ${affectedLeadIds.length} leads with new statuses`);
  console.log();

  if (affectedLeadIds.length === 0) {
    console.log('No leads with new statuses to process');
    console.log('Migration complete (nothing to migrate)');
    return;
  }

  // Fetch lead data from DIR
  const dirLeads = await fetchLeadsByIds(mysqlConn, affectedLeadIds);
  console.log(`✓ Fetched ${dirLeads.length} lead records from DIR`);
  console.log();

  // Process in batches
  const batchSize = options.batchSize;
  let processedCount = 0;

  for (let i = 0; i < dirLeads.length; i += batchSize) {
    const batch = dirLeads.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;

    console.log(`\n=== Processing ${batchId} ===`);
    console.log(`Leads in batch: ${batch.length}`);

    await processLeadBatch({
      options,
      batchId,
      fromDate,
      toDate: new Date(),
      batchNumber,
      dirLeads: batch,
      processedIds: new Set(), // No skip logic for incremental
      mysqlConn,
      pgClient,
    });

    processedCount += batch.length;
    console.log(`Progress: ${processedCount}/${dirLeads.length} leads processed`);
  }

  console.log('\n=== Incremental Migration Complete ===');
  console.log(`Total leads updated: ${processedCount}`);
}

/**
 * Care seeker-based migration
 * (Processes all leads for specific care seekers)
 */
async function runCareSeekerBasedMigration(
  mysqlConn: any,
  pgClient: any,
  options: MigrationCLIOptions
) {
  console.log('=== Care Seeker-Based Migration ===\n');

  // Parse care seeker IDs from input
  const careSeekerIds = await parseIds(options);

  if (!careSeekerIds || careSeekerIds.length === 0) {
    throw new Error('No care seeker IDs found for care seeker-based migration');
  }

  console.log(`Processing ${careSeekerIds.length} care seekers`);
  console.log();

  // Resolve care_seekers → care_recipients → leads
  const resolvedLeads = await resolveCareSeekerToLeads(pgClient, careSeekerIds);
  console.log(`✓ Found ${resolvedLeads.length} leads for these care seekers`);
  console.log();

  if (resolvedLeads.length === 0) {
    console.log('No leads found for the provided care seekers');
    console.log('Migration complete (nothing to migrate)');
    return;
  }

  // Get legacy lead IDs for DIR queries
  const legacyLeadIds = resolvedLeads.map(l => parseInt(l.leadId, 10));

  // Fetch lead data from DIR
  const dirLeads = await fetchLeadsByIds(mysqlConn, legacyLeadIds);
  console.log(`✓ Fetched ${dirLeads.length} lead records from DIR`);
  console.log();

  // Process in batches
  const batchSize = options.batchSize;
  let processedCount = 0;

  for (let i = 0; i < dirLeads.length; i += batchSize) {
    const batch = dirLeads.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;

    console.log(`\n=== Processing ${batchId} ===`);
    console.log(`Leads in batch: ${batch.length}`);

    await processLeadBatch({
      options,
      batchId,
      fromDate: new Date('2000-01-01'), // No time filter for care seeker mode
      toDate: new Date(),
      batchNumber,
      dirLeads: batch,
      processedIds: new Set(), // No skip logic for care seeker mode
      mysqlConn,
      pgClient,
    });

    processedCount += batch.length;
    console.log(`Progress: ${processedCount}/${dirLeads.length} leads processed`);
  }

  console.log('\n=== Care Seeker Migration Complete ===');
  console.log(`Total leads updated: ${processedCount}`);
}

async function processLeadBatch(args: {
  options: MigrationCLIOptions;
  batchId: string;
  fromDate: Date;
  toDate: Date | null;
  batchNumber: number;
  dirLeads: DirLead[];
  processedIds: Set<number>;
  mysqlConn: any;
  pgClient: any;
}): Promise<void> {
  const {
    options, batchId, fromDate, toDate, batchNumber, dirLeads, processedIds, mysqlConn, pgClient,
  } = args;

  const unprocessedLeads = dirLeads.filter(l => !processedIds.has(l.id));
  console.log(
    `Processing ${unprocessedLeads.length} leads (${dirLeads.length - unprocessedLeads.length} already processed)`
  );

  if (unprocessedLeads.length === 0) {
    console.log('All leads in this batch already processed, moving to next batch');
    return;
  }

  const leadIds = unprocessedLeads.map(l => l.id);

  console.log('Fetching lead statuses from DIR...');
  const statusesByLeadId = await fetchStatusesForBatch(mysqlConn, leadIds);
  console.log(`✓ Fetched statuses for ${Object.keys(statusesByLeadId).length} leads`);

  console.log('Fetching MM data for batch...');
  const mmDataMap = await fetchMMDataForBatch(pgClient, leadIds);
  console.log(`✓ Found ${Object.keys(mmDataMap).length} leads in MM`);

  let batchSuccess = 0;
  let batchSkipped = 0;
  let batchFailed = 0;

  const bulkUpdates: Array<{
    legacyId: string;
    summary: string | null;
    leadPriority: string;
    pipelineStage: string;
  }> = [];

  for (const lead of unprocessedLeads) {
    try {
      const mmData = mmDataMap[lead.id];

      if (!mmData) {
        batchSkipped++;
        console.log(`  ⊘ Lead ${lead.id}: Not found in MM care_recipient_leads`);

        if (!options.dryRun) {
          await appendRow(`${MIGRATION_NAME}/${batchId}`, {
            batch_id: batchId,
            source_id: lead.id.toString(),
            status: 'skipped' as any,
            reason: 'not_in_mm',
            processed_at: formatDate(new Date()),
          });
        }
        continue;
      }

      const statuses = statusesByLeadId[lead.id] || [];

      const leadPriority = deriveLeadPriority(lead.allowFollowup, lead.followup_rank);
      const pipelineStage = 'Working';
      const hasStatuses = statuses.length > 0;
      const summary = hasStatuses ? buildLeadStatusSummary(statuses) : null;

      bulkUpdates.push({
        legacyId: lead.id.toString(),
        summary,
        leadPriority,
        pipelineStage,
      });
      batchSuccess++;

      if (hasStatuses) {
        console.log(
          `  ✓ Lead ${lead.id}: Prepared (${statuses.length} statuses, leadPriority=${leadPriority}, pipelineStage=${pipelineStage})`
        );
        if (options.dryRun) {
          // Show summary preview in dry-run mode
          console.log(`     Summary preview (${summary?.length || 0} chars):`);
          const previewLines = summary?.split('\n').slice(0, 3) || [];
          previewLines.forEach(line => console.log(`     │ ${line}`));
          if ((summary?.split('\n').length || 0) > 3) {
            console.log(`     │ ... (${(summary?.split('\n').length || 0) - 3} more lines)`);
          }
        }
      } else {
        console.log(
          `  ✓ Lead ${lead.id}: Prepared (no statuses, leadPriority=${leadPriority}, pipelineStage=${pipelineStage}, summary unchanged)`
        );
      }

      if (!options.dryRun) {
        await appendRow(`${MIGRATION_NAME}/${batchId}`, {
          batch_id: batchId,
          source_id: lead.id.toString(),
          status: 'success',
          reason: hasStatuses ? undefined : 'no_statuses_priority_stage_only',
          processed_at: formatDate(new Date()),
        });
      }

      processedIds.add(lead.id);
    } catch (error: any) {
      batchFailed++;
      console.error(`  ✗ Lead ${lead.id}: Failed -`, error.message);

      if (!options.dryRun) {
        await appendRow(`${MIGRATION_NAME}/${batchId}`, {
          batch_id: batchId,
          source_id: lead.id.toString(),
          status: 'failed' as any,
          error: error.message,
          processed_at: formatDate(new Date()),
        });
      }
    }
  }

  if (bulkUpdates.length > 0) {
    if (options.dryRun) {
      console.log(`\n[DRY RUN] Would update ${bulkUpdates.length} leads in care_recipient_leads`);
      console.log(`[DRY RUN] Fields to update: legacyLeadStatusAndTourHistory, leadPriority, pipelineStage, mldmMigratedModmonAt`);
    } else {
      console.log(`\nPerforming bulk update for ${bulkUpdates.length} leads...`);
      await bulkUpdateMM(pgClient, bulkUpdates);
      console.log('✓ Bulk update complete');
    }
  }

  if (!options.dryRun) {
    const firstLeadDate = dirLeads.length > 0 ? formatDate(dirLeads[0].created_at) : null;
    const lastLeadDate = dirLeads.length > 0 ? formatDate(dirLeads[dirLeads.length - 1].created_at) : null;

    await appendBatch(MIGRATION_NAME, {
      batch_id: batchId,
      status: 'completed',
      query: {
        from: formatDate(fromDate),
        to: toDate ? formatDate(toDate) : null,
        last_created_at: lastLeadDate,
        last_id: dirLeads.length > 0 ? dirLeads[dirLeads.length - 1].id.toString() : null,
        limit: options.batchSize,
      },
      fetched_count: dirLeads.length,
      started_at: formatDate(new Date()),
      completed_at: formatDate(new Date()),
    });

    console.log(`Date range: ${firstLeadDate} → ${lastLeadDate}`);
  }

  let summary = await readSummary();
  summary[MIGRATION_NAME].completed_batches = batchNumber;
  summary[MIGRATION_NAME].total_rows_fetched += dirLeads.length;
  summary[MIGRATION_NAME].total_success += batchSuccess;
  summary[MIGRATION_NAME].total_skipped += batchSkipped;
  summary[MIGRATION_NAME].total_failed += batchFailed;
  summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
  await writeSummary(summary);

  console.log(`\nBatch ${batchId} Summary:`);
  console.log(`  ✓ Success: ${batchSuccess}`);
  console.log(`  ⊘ Skipped: ${batchSkipped}`);
  console.log(`  ✗ Failed: ${batchFailed}`);
}

export async function fetchLeadBatchFromMM(
  pgClient: any,
  mysqlConn: any,
  fromDate: Date,
  toDate: Date | null,
  batchSize: number,
  offset: number
): Promise<DirLead[]> {
  const mmLeads = await fetchCareRecipientLeadsByDateRange(pgClient, fromDate, toDate, batchSize, offset);
  const legacyIds = mmLeads.map((lead) => lead.legacyId);

  if (legacyIds.length === 0) {
    return [];
  }

  return fetchLeadsByIds(mysqlConn, legacyIds);
}

async function fetchLeadsByIds(mysqlConn: any, leadIds: number[]): Promise<DirLead[]> {
  if (leadIds.length === 0) return [];

  const placeholders = leadIds.map(() => '?').join(', ');
  const [rows] = await mysqlConn.query(
    `
    SELECT
      lrl.id,
      lrl.created_at,
      c.followup_rank,
      c.allow_followup AS allowFollowup
    FROM local_resource_leads lrl
    LEFT JOIN inquiries i ON i.id = lrl.inquiry_id
    LEFT JOIN contacts c ON c.id = i.contact_id
    WHERE lrl.id IN (${placeholders})
      AND lrl.deleted_at IS NULL
    `,
    leadIds
  );

  return rows as DirLead[];
}

/**
 * Fetch leads affected by new statuses created since a specific date
 * (Incremental update mode)
 */
async function fetchAffectedLeadsFromNewStatuses(
  mysqlConn: any,
  afterDate: Date
): Promise<number[]> {
  const [rows] = await mysqlConn.query(
    `
    SELECT DISTINCT local_resource_lead_id as lead_id
    FROM lead_statuses
    WHERE created_at > ?
    ORDER BY local_resource_lead_id
    `,
    [afterDate]
  );

  return (rows as any[]).map(r => r.lead_id);
}

/**
 * Resolve care seeker IDs to lead IDs
 * (Care seeker-based mode: care_seekers → care_recipients → leads)
 */
async function resolveCareSeekerToLeads(
  pgClient: any,
  careSeekerIds: number[]
): Promise<{ leadId: string; careSeekerId: string; careRecipientId: string }[]> {
  if (careSeekerIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT
      crl.id as lead_id,
      crl."legacyId" as lead_legacy_id,
      cr.id as care_recipient_id,
      cs."legacyId" as care_seeker_legacy_id
    FROM care_recipient_leads crl
    INNER JOIN care_recipients cr ON cr.id = crl."careRecipientId"
    INNER JOIN care_seekers cs ON cs.id = cr."careSeekerId"
    WHERE cs."legacyId" = ANY($1)
      AND crl."deletedAt" IS NULL
      AND cr."deletedAt" IS NULL
      AND cs."deletedAt" IS NULL
    ORDER BY crl."createdAt" DESC
    `,
    [careSeekerIds.map(id => id.toString())]
  );

  return result.rows.map((row: any) => ({
    leadId: row.lead_legacy_id,
    careSeekerId: row.care_seeker_legacy_id,
    careRecipientId: row.care_recipient_id,
  }));
}


async function fetchStatusesForBatch(
  mysqlConn: any,
  leadIds: number[]
): Promise<Record<number, DirLeadStatus[]>> {
  if (leadIds.length === 0) return {};

  const placeholders = leadIds.map(() => '?').join(', ');
  const query = `
    SELECT
      ls.local_resource_lead_id AS lead_id,
      ls.created_at,
      ls.status,
      ls.sub_status,
      ls.tour_date,
      ls.tour_time,
      ls.source,
      COALESCE(a.full_name, a.email) AS account_name
    FROM lead_statuses ls
    LEFT JOIN accounts a ON a.id = ls.account_id
    WHERE ls.local_resource_lead_id IN (${placeholders})
    ORDER BY ls.local_resource_lead_id, ls.created_at DESC
  `;

  const [rows] = await mysqlConn.query(query, leadIds);

  const map: Record<number, DirLeadStatus[]> = {};
  for (const row of rows) {
    const id = row.lead_id;
    if (!map[id]) map[id] = [];
    map[id].push(row as DirLeadStatus);
  }

  return map;
}

async function fetchMMDataForBatch(
  pgClient: any,
  leadIds: number[]
): Promise<Record<number, MMLeadData>> {
  if (leadIds.length === 0) return {};

  const legacyIds = leadIds.map(id => id.toString());

  const result = await pgClient.query(
    `
    SELECT id, "legacyId", "mldmMigratedModmonAt"
    FROM care_recipient_leads
    WHERE "legacyId" = ANY($1)
      AND "deletedAt" IS NULL
    `,
    [legacyIds]
  );

  const map: Record<number, MMLeadData> = {};
  for (const row of result.rows) {
    map[parseInt(row.legacyId, 10)] = {
      id: row.id,
      mldmMigratedModmonAt: row.mldmMigratedModmonAt,
    };
  }

  return map;
}

async function bulkUpdateMM(
  pgClient: any,
  updates: Array<{
    legacyId: string;
    summary: string | null;
    leadPriority: string;
    pipelineStage: string;
  }>
): Promise<void> {
  if (updates.length === 0) return;

  const values = updates
    .map((_u, idx) => {
      const baseIdx = idx * 4;
      return `($${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3}, $${baseIdx + 4})`;
    })
    .join(', ');

  const params: any[] = [];
  for (const u of updates) {
    params.push(u.legacyId, u.summary, u.leadPriority, u.pipelineStage);
  }

  const query = `
    UPDATE care_recipient_leads
    SET
      "legacyLeadStatusAndTourHistory" = COALESCE(
        v.summary,
        care_recipient_leads."legacyLeadStatusAndTourHistory"
      ),
      "leadPriority" = v.lead_priority,
      "pipelineStage" = v.pipeline_stage,
      "mldmMigratedModmonAt" = NOW(),
      "updatedAt" = NOW()
    FROM (VALUES ${values}) AS v(legacy_id, summary, lead_priority, pipeline_stage)
    WHERE care_recipient_leads."legacyId" = v.legacy_id
      AND care_recipient_leads."deletedAt" IS NULL
  `;

  await pgClient.query(query, params);
}

export function deriveLeadPriority(
  allowFollowup: boolean | number | string | null,
  followupRank: number | null
): string {
  const isAutomatedCallsOff = allowFollowup === true || allowFollowup === 1 || allowFollowup === '1';
  const isAutomatedCallsOn = allowFollowup === false || allowFollowup === 0 || allowFollowup === '0';

  if (isAutomatedCallsOff) {
    return 'On Hold';
  }

  if (!isAutomatedCallsOn) {
    return 'On Hold';
  }

  switch (followupRank) {
    case 0:
      return 'HOT';
    case 1:
    case 2:
    case 3:
      return 'Warm';
    case 4:
      return 'On Hold';
    default:
      return 'On Hold';
  }
}

export function buildLeadStatusSummary(statuses: DirLeadStatus[]): string {
  const sorted = [...statuses].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );

  const top10 = sorted.slice(0, 10);
  const lines = top10.map(formatStatusLine);

  const MAX_LENGTH = 2000;
  let summary = lines.join('\n');

  if (summary.length > MAX_LENGTH) {
    summary = summary.substring(0, MAX_LENGTH);
    const lastNewline = summary.lastIndexOf('\n');
    if (lastNewline > 0) {
      summary = summary.substring(0, lastNewline);
    }
  }

  return summary;
}

export function formatStatusLine(status: DirLeadStatus): string {
  const date = formatStatusDate(new Date(status.created_at));
  const statusText = formatStatusText(status);
  const attribution = getStatusAttribution(status);
  if (!attribution) {
    return `${date} - ${statusText}`;
  }
  return `${date} - ${statusText} - ${attribution}`;
}

function getStatusAttribution(status: DirLeadStatus): string {
  // Match the logic from LeadStatusLabel.jsx statusLabel function
  if (status.account_name) {
    return status.account_name;
  } else if (status.source === 'provider') {
    return 'Provider';
  } else if (status.source === 'lead' || status.source === 'contact') {
    return 'Contact';
  } else {
    return 'Caring Staff';
  }
}

export function formatStatusText(status: DirLeadStatus): string {
  if (status.status === 'tour_scheduled' && status.tour_date) {
    return `Tour scheduled, ${status.sub_status || 'in person'}`;
  }

  if (status.status === 'tour_completed') {
    return 'Tour completed';
  }

  if (
    status.status === 'tour_cancelled' ||
    (status.status === 'memo' && status.sub_status === 'tour_canceled')
  ) {
    return `Tour cancelled, ${status.sub_status || 'in person'}`;
  }

  if (status.status === 'valid_lead') {
    return 'Valid';
  }

  let text = `Status set as ${status.status}`;
  if (status.sub_status) {
    text += `, ${status.sub_status}`;
  }
  return text;
}

export function formatStatusDate(date: Date): string {
  const M = date.getMonth() + 1;
  const D = date.getDate();
  const YY = String(date.getFullYear()).slice(-2);

  let hours = date.getHours();
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const ampm = hours >= 12 ? 'pm' : 'am';
  hours = hours % 12 || 12;
  const hh = String(hours).padStart(2, '0');

  return `${M}/${D}/${YY} ${hh}:${minutes}${ampm}`;
}

migrateLeadStatusTourHistory().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
