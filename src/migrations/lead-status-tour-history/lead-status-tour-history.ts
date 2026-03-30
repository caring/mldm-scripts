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

const MIGRATION_NAME = 'lead_status_tour_history';

interface DirLead {
  id: number;
  created_at: Date;
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
  mldmMigratedAt: Date | null;
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
    const fromDate = parseTimeParam(options.from);
    const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

    console.log('Migration scope:');
    console.log(`  Leads created before: ${formatDate(fromDate)}`);
    if (toDate) {
      console.log(`  Leads created after: ${formatDate(toDate)}`);
      console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
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

    let offset = batches.reduce((sum, batch) => sum + batch.fetched_count, 0);
    let batchNumber = batches.length + 1;
    let hasMore = true;

    console.log(`Resuming from offset: ${offset} (${batches.length} batches completed)`);

    while (hasMore) {
      const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
      console.log(`\n=== Processing ${batchId} ===`);

      if (!options.dryRun) {
        await ensureMigrationDir(`${MIGRATION_NAME}/${batchId}`);
      }

      const dirLeads = await fetchLeadBatch(mysqlConn, fromDate, toDate, options.batchSize, offset);

      if (dirLeads.length === 0) {
        console.log('No more leads to process');
        hasMore = false;
        break;
      }

      console.log(`Fetched ${dirLeads.length} leads from DIR`);

      const unprocessedLeads = dirLeads.filter(l => !processedIds.has(l.id));
      console.log(
        `Processing ${unprocessedLeads.length} leads (${dirLeads.length - unprocessedLeads.length} already processed)`
      );

      if (unprocessedLeads.length === 0) {
        console.log('All leads in this batch already processed, moving to next batch');
        offset += options.batchSize;
        batchNumber++;
        continue;
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

      const bulkUpdates: Array<{ legacyId: string; summary: string }> = [];

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

          bulkUpdates.push({ legacyId: lead.id.toString(), summary });
          batchSuccess++;
          console.log(`  ✓ Lead ${lead.id}: Prepared (${statuses.length} statuses)`);

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

      summary = await readSummary();
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

      offset += options.batchSize;
      batchNumber++;
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

async function fetchLeadBatch(
  mysqlConn: any,
  fromDate: Date,
  toDate: Date | null,
  batchSize: number,
  offset: number
): Promise<DirLead[]> {
  let query = `
    SELECT id, created_at
    FROM local_resource_leads
    WHERE created_at <= ?
      AND deleted_at IS NULL
  `;
  const params: any[] = [fromDate];

  if (toDate) {
    query += ` AND created_at >= ?`;
    params.push(toDate);
  }

  query += `
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
  `;
  params.push(batchSize, offset);

  const [rows] = await mysqlConn.query(query, params);
  return rows;
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
    SELECT id, "legacyId", "mldmMigratedAt"
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
      mldmMigratedAt: row.mldmMigratedAt,
    };
  }

  return map;
}

async function bulkUpdateMM(
  pgClient: any,
  updates: Array<{ legacyId: string; summary: string }>
): Promise<void> {
  if (updates.length === 0) return;

  const values = updates
    .map((_u, idx) => `($${idx * 2 + 1}, $${idx * 2 + 2})`)
    .join(', ');

  const params: any[] = [];
  for (const u of updates) {
    params.push(u.legacyId, u.summary);
  }

  const query = `
    UPDATE care_recipient_leads AS crl
    SET
      "legacyLeadStatusAndTourHistory" = v.summary,
      "mldmMigratedAt" = NOW(),
      "updatedAt" = NOW()
    FROM (VALUES ${values}) AS v(legacy_id, summary)
    WHERE crl."legacyId" = v.legacy_id
      AND crl."deletedAt" IS NULL
  `;

  await pgClient.query(query, params);
}

export function buildLeadStatusSummary(statuses: DirLeadStatus[]): string {
  const sorted = [...statuses].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );

  const top10 = sorted.slice(0, 10);
  const lines = top10.map(formatStatusLine);

  const MAX_LENGTH = 1000;
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
