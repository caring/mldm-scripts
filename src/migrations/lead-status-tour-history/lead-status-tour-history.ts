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
  created_by: string | null;
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

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
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
  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
  }
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
    summary: string;
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

      if (statuses.length === 0) {
        batchSkipped++;
        console.log(`  ⊘ Lead ${lead.id}: No statuses`);

        if (!options.dryRun) {
          await appendRow(`${MIGRATION_NAME}/${batchId}`, {
            batch_id: batchId,
            source_id: lead.id.toString(),
            status: 'skipped' as any,
            reason: 'no_statuses',
            processed_at: formatDate(new Date()),
          });
        }
        continue;
      }

      const summary = buildLeadStatusSummary(statuses);
      const leadPriority = deriveLeadPriority(lead.allowFollowup, lead.followup_rank);
      const pipelineStage = 'Working';

      bulkUpdates.push({
        legacyId: lead.id.toString(),
        summary,
        leadPriority,
        pipelineStage,
      });
      batchSuccess++;
      console.log(
        `  ✓ Lead ${lead.id}: Prepared (${statuses.length} statuses, leadPriority=${leadPriority}, pipelineStage=${pipelineStage})`
      );

      if (!options.dryRun) {
        await appendRow(`${MIGRATION_NAME}/${batchId}`, {
          batch_id: batchId,
          source_id: lead.id.toString(),
          status: 'success',
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

  if (bulkUpdates.length > 0 && !options.dryRun) {
    console.log(`\nPerforming bulk update for ${bulkUpdates.length} leads...`);
    await bulkUpdateMM(pgClient, bulkUpdates);
    console.log('✓ Bulk update complete');
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


async function fetchStatusesForBatch(
  mysqlConn: any,
  leadIds: number[]
): Promise<Record<number, DirLeadStatus[]>> {
  if (leadIds.length === 0) return {};

  const placeholders = leadIds.map(() => '?').join(', ');
  const query = `
    SELECT
      local_resource_lead_id AS lead_id,
      created_at,
      status,
      sub_status,
      tour_date,
      tour_time,
      created_by
    FROM lead_statuses
    WHERE local_resource_lead_id IN (${placeholders})
    ORDER BY local_resource_lead_id, created_at DESC
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
    summary: string;
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
    UPDATE care_recipient_leads AS crl
    SET
      "legacyLeadStatusAndTourHistory" = v.summary,
      "leadPriority" = v.lead_priority,
      "pipelineStage" = v.pipeline_stage,
      "mldmMigratedModmonAt" = NOW(),
      "updatedAt" = NOW()
    FROM (VALUES ${values}) AS v(legacy_id, summary, lead_priority, pipeline_stage)
    WHERE crl."legacyId" = v.legacy_id
      AND crl."deletedAt" IS NULL
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
  const createdBy = status.created_by || 'Unknown';
  return `${date} - ${statusText} - ${createdBy}`;
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
