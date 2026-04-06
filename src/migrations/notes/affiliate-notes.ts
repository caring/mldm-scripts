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
  BatchRecord,
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
  mapCareRecipients,
  mapAgents,
  checkExistingNotes,
  insertNote,
} from '../../utils/migration-db';

const MIGRATION_NAME = 'affiliate_notes';
const SOURCE_TYPE = 'affiliate_notes';

interface AffiliateNote {
  formatted_text_id: number;
  note_content: string;
  inquiry_id: number;
  contact_id: number;
  dir_care_recipient_id: number;
  dir_account_id: number | null;
  created_at: Date;
  updated_at: Date;
}

interface ExplicitLeadResolution {
  requestedLeadCount: number;
  legacyLeadIds: number[];
  dirCareRecipientIds: number[];
  mmCareRecipientMap: Map<number, string>;  // DIR care_recipient_id → MM UUID
}

/**
 * Main migration function
 */
async function migrateAffiliateNotes() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:affiliate-notes');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Affiliate Notes Migration ===\n');
  printMigrationOptions(options);

  try {
    // Ensure migration directory exists
    await ensureMigrationDir(MIGRATION_NAME);

    // Handle report mode
    if (options.report) {
      await generateReport();
      return;
    }

    // Handle retry-failed mode
    if (options.retryFailed) {
      await retryFailedRows(options);
      return;
    }

    // Normal migration mode
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
  console.log('Time Range:');
  console.log(`  From: ${migrationSummary.time_range.from}`);
  console.log(`  To: ${migrationSummary.time_range.to || 'unlimited'}`);
  if (migrationSummary.time_range.to_relative) {
    console.log(`  Range: ${migrationSummary.time_range.to_relative}`);
  }
  console.log();

  console.log('Progress:');
  console.log(`  Batches: ${migrationSummary.completed_batches} / ${migrationSummary.total_batches}`);
  console.log(`  Batch size: ${migrationSummary.batch_size}`);
  console.log();

  console.log('Results:');
  console.log(`  Total rows fetched: ${migrationSummary.total_rows_fetched}`);
  console.log(`  ✓ Success: ${migrationSummary.total_success} (${((migrationSummary.total_success / migrationSummary.total_rows_fetched) * 100).toFixed(1)}%)`);
  console.log(`  ⊘ Duplicate: ${migrationSummary.total_duplicate} (${((migrationSummary.total_duplicate / migrationSummary.total_rows_fetched) * 100).toFixed(1)}%)`);
  console.log(`  ⊘ Skipped: ${migrationSummary.total_skipped} (${((migrationSummary.total_skipped / migrationSummary.total_rows_fetched) * 100).toFixed(1)}%)`);
  console.log(`  ✗ Failed: ${migrationSummary.total_failed} (${((migrationSummary.total_failed / migrationSummary.total_rows_fetched) * 100).toFixed(1)}%)`);
  console.log();

  console.log('Timestamps:');
  console.log(`  Started: ${migrationSummary.started_at}`);
  console.log(`  Last updated: ${migrationSummary.last_updated_at}`);
  console.log();

  // Show failed rows if any
  if (migrationSummary.total_failed > 0) {
    console.log('Failed rows:');
    const rows = await readRows(MIGRATION_NAME);
    const failedRows = rows.filter((r: RowRecord) => r.status === 'failed');
    failedRows.slice(0, 10).forEach((row: RowRecord) => {
      console.log(`  - Source ID: ${row.source_id}, Reason: ${row.reason}, Error: ${row.error}`);
    });
    if (failedRows.length > 10) {
      console.log(`  ... and ${failedRows.length - 10} more`);
    }
    console.log();
    console.log('To retry failed rows: npm run migrate:affiliate-notes -- --retry-failed');
  }

  console.log('Files:');
  console.log(`  Batches: migration-state/${MIGRATION_NAME}/batches.jsonl`);
  console.log(`  Rows: migration-state/${MIGRATION_NAME}/rows.jsonl`);
  console.log(`  Summary: migration-state/summary.json`);
}

async function retryFailedRows(_options: MigrationCLIOptions) {
  console.log('=== Retry Failed Rows ===\n');

  const rows = await readRows(MIGRATION_NAME);
  const failedRows = rows.filter((r: RowRecord) => r.status === 'failed');

  if (failedRows.length === 0) {
    console.log('No failed rows to retry');
    return;
  }

  console.log(`Found ${failedRows.length} failed rows`);
  console.log('Retry functionality - to be implemented in future iteration');
  console.log('For now, you can:');
  console.log('1. Review failed rows in migration-state/affiliate_notes/rows.jsonl');
  console.log('2. Fix the underlying issues');
  console.log('3. Re-run the migration (it will skip already-processed rows)');
}

/**
 * Resolve explicit lead IDs to care recipient IDs for migration
 */
async function resolveExplicitLeadsForAffiliateNotes(
  options: MigrationCLIOptions,
  mysqlConn: any,
  pgClient: any
): Promise<ExplicitLeadResolution | null> {
  // Check if lead IDs were provided
  if (!options.idsFile && !options.idsInline) {
    return null;
  }

  console.log('=== Resolving Legacy Lead IDs ===\n');

  // Parse lead IDs from input
  const parsedIds = await parseIds(options);
  if (!parsedIds || parsedIds.length === 0) {
    throw new Error('No valid lead IDs found in input');
  }

  const legacyLeadIds = [...new Set(parsedIds.filter(id => Number.isInteger(id) && id > 0))];
  console.log(`Parsed ${legacyLeadIds.length} unique legacy lead IDs`);
  console.log(`Sample IDs: [${legacyLeadIds.slice(0, 5).join(', ')}${legacyLeadIds.length > 5 ? ', ...' : ''}]`);
  console.log();

  // Query DIR to get care_recipient_ids for these leads
  console.log('Querying DIR for care_recipient_ids...');
  const dirCareRecipientIds = await fetchCareRecipientIdsFromLeads(mysqlConn, legacyLeadIds);
  console.log(`Found ${dirCareRecipientIds.length} care recipients for ${legacyLeadIds.length} leads`);
  console.log(`Sample care_recipient IDs: [${dirCareRecipientIds.slice(0, 5).join(', ')}${dirCareRecipientIds.length > 5 ? ', ...' : ''}]`);
  console.log();

  // Verify care_recipients exist in MM and get their UUIDs
  console.log('Verifying care recipients in MM...');
  const mmCareRecipientMap = await mapCareRecipients(pgClient, dirCareRecipientIds);
  console.log(`Found ${mmCareRecipientMap.size} care recipients in MM out of ${dirCareRecipientIds.length}`);
  console.log();

  if (mmCareRecipientMap.size === 0) {
    throw new Error('None of the care recipients from the provided lead IDs exist in MM');
  }

  return {
    requestedLeadCount: legacyLeadIds.length,
    legacyLeadIds,
    dirCareRecipientIds,
    mmCareRecipientMap,
  };
}

/**
 * Fetch care_recipient_ids from DIR for given legacy lead IDs
 */
async function fetchCareRecipientIdsFromLeads(
  mysqlConn: any,
  legacyLeadIds: number[]
): Promise<number[]> {
  if (legacyLeadIds.length === 0) return [];

  const placeholders = legacyLeadIds.map(() => '?').join(', ');
  const query = `
    SELECT DISTINCT care_recipient_id
    FROM local_resource_leads
    WHERE id IN (${placeholders})
      AND deleted_at IS NULL
      AND care_recipient_id IS NOT NULL
    ORDER BY care_recipient_id
  `;

  const [rows] = await mysqlConn.query(query, legacyLeadIds);
  return (rows as { care_recipient_id: number }[]).map(row => row.care_recipient_id);
}

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
    // Check if this is a lead-based migration
    const explicitResolution = await resolveExplicitLeadsForAffiliateNotes(
      options,
      mysqlConn,
      pgClient
    );

    if (explicitResolution) {
      // Lead-based migration with time window
      await runLeadBasedMigration(options, mysqlConn, pgClient, explicitResolution);
    } else {
      // Original time-based migration
      await runTimeBasedMigration(options, mysqlConn, pgClient);
    }

  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
    console.log('\n✓ Disconnected from databases');
  }
}

async function runTimeBasedMigration(
  options: MigrationCLIOptions,
  mysqlConn: any,
  pgClient: any
) {
  // Parse time range
  const fromDate = parseTimeParam(options.from);
  const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

  console.log('Time range:');
  console.log(`  From: ${formatDate(fromDate)}`);
  console.log(`  To: ${toDate ? formatDate(toDate) : 'unlimited'}`);
  if (toDate) {
    console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
  }
  console.log();

  // Load existing progress
  const batches = await readBatches(MIGRATION_NAME);
  const rows = await readRows(MIGRATION_NAME);
  const processedSourceIds: Set<string> = new Set(rows.map((r: RowRecord) => r.source_id));

  console.log('Existing progress:');
  console.log(`  Batches: ${batches.length}`);
  console.log(`  Rows processed: ${rows.length}`);
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

  // Determine cursor for pagination
  let lastCreatedAt: Date | null = fromDate;
  let lastId: string | null = null;

  const lastCompletedBatch = batches.filter((b: BatchRecord) => b.status === 'completed').pop();
  if (lastCompletedBatch && lastCompletedBatch.query.last_created_at) {
    lastCreatedAt = new Date(lastCompletedBatch.query.last_created_at);
    lastId = lastCompletedBatch.query.last_id;
    console.log(`Resuming from last batch: ${lastCompletedBatch.batch_id}`);
    console.log(`  Last created_at: ${lastCreatedAt}`);
    console.log(`  Last id: ${lastId}`);
    console.log();
  }

  // Start batch processing
  let batchNumber = batches.length + 1;
  let hasMore = true;

  while (hasMore) {
    const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;
    console.log(`\n=== Processing ${batchId} ===`);

    // Fetch batch from DIR
    const batch = await fetchBatch(
      mysqlConn,
      fromDate,
      toDate,
      lastCreatedAt,
      lastId,
      options.batchSize
    );

    if (batch.length === 0) {
      console.log('No more rows to process');
      hasMore = false;
      break;
    }

    console.log(`Fetched ${batch.length} rows`);

    // Record batch start
    const batchRecord: BatchRecord = {
      batch_id: batchId,
      query: {
        from: formatDate(fromDate),
        to: toDate ? formatDate(toDate) : null,
        last_created_at: lastCreatedAt ? formatDate(lastCreatedAt) : null,
        last_id: lastId,
        limit: options.batchSize,
      },
      fetched_count: batch.length,
      started_at: formatDate(new Date()),
      completed_at: null,
      status: 'in_progress',
    };
    await appendBatch(MIGRATION_NAME, batchRecord);

    if (options.dryRun) {
      console.log('[DRY RUN] Would process this batch');
      console.log('\nFirst 3 rows:');
      batch.slice(0, 3).forEach((row, idx) => {
        console.log(`\n  Row ${idx + 1}:`);
        console.log(`    formatted_text_id: ${row.formatted_text_id}`);
        console.log(`    note_content: "${row.note_content.substring(0, 100)}${row.note_content.length > 100 ? '...' : ''}"`);
        console.log(`    note_length: ${row.note_content.length} chars`);
        console.log(`    inquiry_id: ${row.inquiry_id}`);
        console.log(`    contact_id: ${row.contact_id}`);
        console.log(`    dir_care_recipient_id: ${row.dir_care_recipient_id}`);
        console.log(`    dir_account_id: ${row.dir_account_id || 'null'}`);
        console.log(`    created_at: ${row.created_at.toISOString()}`);
        console.log(`    updated_at: ${row.updated_at.toISOString()}`);
      });
      console.log(`\n  ... and ${batch.length - 3} more rows`);
    } else {
      // Process batch
      await processBatch(pgClient, batch, batchId, processedSourceIds);
    }

    // Update batch as completed
    batchRecord.completed_at = formatDate(new Date());
    batchRecord.status = 'completed';
    await appendBatch(MIGRATION_NAME, batchRecord);

    // Update cursor for next batch
    const lastRow = batch[batch.length - 1];
    lastCreatedAt = lastRow.created_at;
    lastId = lastRow.formatted_text_id.toString();

    // Update summary
    summary = await readSummary();
    summary[MIGRATION_NAME].total_batches++;
    summary[MIGRATION_NAME].completed_batches++;
    summary[MIGRATION_NAME].total_rows_fetched += batch.length;
    summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
    await writeSummary(summary);

    batchNumber++;

    // Check if we've reached the end
    if (batch.length < options.batchSize) {
      console.log('\nReached end of data (partial batch)');
      hasMore = false;
    }
  }

  // Mark migration as completed
  summary = await readSummary();
  summary[MIGRATION_NAME].status = 'completed';
  await writeSummary(summary);

  console.log('\n=== Migration Summary ===');
  console.log(`Total batches: ${summary[MIGRATION_NAME].total_batches}`);
  console.log(`Total rows fetched: ${summary[MIGRATION_NAME].total_rows_fetched}`);
  console.log(`Success: ${summary[MIGRATION_NAME].total_success}`);
  console.log(`Skipped: ${summary[MIGRATION_NAME].total_skipped}`);
  console.log(`Duplicate: ${summary[MIGRATION_NAME].total_duplicate}`);
  console.log(`Failed: ${summary[MIGRATION_NAME].total_failed}`);
}

/**
 * Lead-based migration with time window
 */
async function runLeadBasedMigration(
  options: MigrationCLIOptions,
  mysqlConn: any,
  pgClient: any,
  resolution: ExplicitLeadResolution
) {
  // Determine time window (auto-detect from last run or use --from)
  const timeWindow = await determineTimeWindow(options);
  const fromDate = timeWindow.from;
  const toDate = timeWindow.to;

  console.log('=== Lead-Based Migration ===\n');
  console.log('Input:');
  console.log(`  Legacy lead IDs: ${resolution.requestedLeadCount}`);
  console.log(`  Care recipients: ${resolution.dirCareRecipientIds.length}`);
  console.log(`  Care recipients in MM: ${resolution.mmCareRecipientMap.size}`);
  console.log();

  console.log('Time window:');
  console.log(`  From: ${formatDate(fromDate)}${options.from ? ' (explicit)' : ' (auto-detected)'}`);
  console.log(`  To: ${formatDate(toDate)}`);
  if (toDate) {
    console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
  }
  console.log();

  // Fetch all affiliate notes for these care recipients within time window
  console.log('Fetching affiliate notes from DIR...');
  const allNotes = await fetchAffiliateNotesByLeadIds(
    mysqlConn,
    resolution.legacyLeadIds,
    fromDate,
    toDate
  );
  console.log(`Found ${allNotes.length} affiliate notes in time window`);
  console.log();

  if (allNotes.length === 0) {
    console.log('No notes found for the given lead IDs in the specified time window');
    console.log('Migration complete (nothing to migrate)');
    return;
  }

  // Check which notes already exist in MM (diff detection)
  console.log('Checking for existing notes in MM (diff detection)...');
  const formattedTextIds = allNotes.map(n => n.formatted_text_id.toString());
  const existingIds = await checkExistingAffiliateNotes(pgClient, formattedTextIds);
  console.log(`Already migrated: ${existingIds.size}`);
  console.log();

  // Filter to only new notes
  const newNotes = allNotes.filter(note => !existingIds.has(note.formatted_text_id.toString()));
  console.log(`New notes to migrate: ${newNotes.length}`);
  console.log();

  if (newNotes.length === 0) {
    console.log('All notes already migrated (no diff)');
    console.log('Migration complete');
    return;
  }

  // Process in batches
  const batchSize = options.batchSize;
  let batchNumber = 1;
  const processedSourceIds: Set<string> = new Set();

  for (let i = 0; i < newNotes.length; i += batchSize) {
    const batch = newNotes.slice(i, i + batchSize);
    const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;

    console.log(`\n=== Processing ${batchId} ===`);
    console.log(`Processing notes ${i + 1} to ${Math.min(i + batchSize, newNotes.length)} of ${newNotes.length}`);

    // Record batch start
    const batchRecord: BatchRecord = {
      batch_id: batchId,
      query: {
        from: formatDate(fromDate),
        to: formatDate(toDate),
        last_created_at: null,
        last_id: null,
        limit: batchSize,
      },
      fetched_count: batch.length,
      started_at: formatDate(new Date()),
      completed_at: null,
      status: 'in_progress',
    };
    await appendBatch(MIGRATION_NAME, batchRecord);

    if (options.dryRun) {
      console.log('[DRY RUN] Would process this batch');
      console.log('\nFirst 3 rows:');
      batch.slice(0, 3).forEach((row, idx) => {
        console.log(`\n  Row ${idx + 1}:`);
        console.log(`    formatted_text_id: ${row.formatted_text_id}`);
        console.log(`    note_content: "${row.note_content.substring(0, 100)}${row.note_content.length > 100 ? '...' : ''}"`);
        console.log(`    dir_care_recipient_id: ${row.dir_care_recipient_id}`);
        console.log(`    created_at: ${row.created_at.toISOString()}`);
      });
      console.log(`\n  ... and ${batch.length - 3} more rows`);
    } else {
      // Process batch
      await processBatch(pgClient, batch, batchId, processedSourceIds);
    }

    // Update batch as completed
    batchRecord.completed_at = formatDate(new Date());
    batchRecord.status = 'completed';
    await appendBatch(MIGRATION_NAME, batchRecord);

    batchNumber++;
  }

  console.log('\n=== Lead-Based Migration Complete ===');
  console.log(`Total notes found: ${allNotes.length}`);
  console.log(`Already migrated: ${existingIds.size}`);
  console.log(`New notes migrated: ${newNotes.length}`);
}

/**
 * Determine time window for migration
 */
async function determineTimeWindow(
  options: MigrationCLIOptions
): Promise<{ from: Date; to: Date }> {
  // If explicit --from is provided, use it
  if (options.from) {
    const from = parseTimeParam(options.from);
    const to = options.to ? parseTimeParam(options.to, from) : new Date();
    return { from, to };
  }

  // Auto-detect from last migration run
  const summary = await readSummary();
  const lastRun = summary[MIGRATION_NAME]?.last_updated_at;

  if (lastRun) {
    console.log(`Auto-detected last migration: ${lastRun}`);
    return {
      from: new Date(lastRun),
      to: options.to ? parseTimeParam(options.to) : new Date()
    };
  }

  // Fallback to business requirement default
  return {
    from: new Date('2024-04-15'),
    to: new Date()
  };
}

/**
 * Fetch affiliate notes for specific legacy lead IDs within time window
 */
async function fetchAffiliateNotesByLeadIds(
  mysqlConn: any,
  legacyLeadIds: number[],
  fromDate: Date,
  toDate: Date
): Promise<AffiliateNote[]> {
  if (legacyLeadIds.length === 0) return [];

  const allNotes: AffiliateNote[] = [];
  const batchSize = 1000; // Process 1000 lead IDs at a time to avoid query size limits

  for (let i = 0; i < legacyLeadIds.length; i += batchSize) {
    const batch = legacyLeadIds.slice(i, i + batchSize);
    const placeholders = batch.map(() => '?').join(', ');

    const query = `
      SELECT
        ft.id as formatted_text_id,
        ft.original_content as note_content,
        i.id as inquiry_id,
        i.contact_id,
        c.care_recipient_id as dir_care_recipient_id,
        i.account_id as dir_account_id,
        i.created_at,
        i.updated_at
      FROM formatted_texts ft
      INNER JOIN inquiries i ON ft.owner_id = i.id AND ft.owner_type = 'Inquiry'
      INNER JOIN contacts c ON i.contact_id = c.id
      WHERE ft.name = 'affiliate_notes'
        AND ft.original_content IS NOT NULL
        AND TRIM(ft.original_content) != ''
        AND c.care_recipient_id IN (
          SELECT DISTINCT care_recipient_id
          FROM local_resource_leads
          WHERE id IN (${placeholders})
            AND deleted_at IS NULL
            AND care_recipient_id IS NOT NULL
        )
        AND i.created_at >= ?
        AND i.created_at <= ?
      ORDER BY i.created_at DESC
    `;

    const params = [...batch, fromDate, toDate];
    const [rows] = await mysqlConn.query(query, params);
    allNotes.push(...(rows as AffiliateNote[]));
  }

  return allNotes;
}

/**
 * Check which affiliate notes already exist in MM
 */
async function checkExistingAffiliateNotes(
  pgClient: any,
  formattedTextIds: string[]
): Promise<Set<string>> {
  if (formattedTextIds.length === 0) return new Set();

  const result = await pgClient.query(`
    SELECT "legacyId"
    FROM care_recipient_notes
    WHERE "legacyId" = ANY($1)
      AND source = $2
      AND "deletedAt" IS NULL
  `, [formattedTextIds, SOURCE_TYPE]);

  return new Set(result.rows.map((r: any) => r.legacyId));
}

async function fetchBatch(
  mysqlConn: any,
  fromDate: Date,
  toDate: Date | null,
  lastCreatedAt: Date | null,
  lastId: string | null,
  limit: number
): Promise<AffiliateNote[]> {
  const query = `
    SELECT
      ft.id as formatted_text_id,
      ft.original_content as note_content,
      i.id as inquiry_id,
      i.contact_id,
      c.care_recipient_id as dir_care_recipient_id,
      i.account_id as dir_account_id,
      i.created_at,
      i.updated_at
    FROM formatted_texts ft
    INNER JOIN inquiries i ON ft.owner_id = i.id AND ft.owner_type = 'Inquiry'
    INNER JOIN contacts c ON i.contact_id = c.id
    WHERE ft.name = 'affiliate_notes'
      AND ft.original_content IS NOT NULL
      AND TRIM(ft.original_content) != ''
      AND c.care_recipient_id IS NOT NULL
      AND i.created_at <= ?
      ${toDate ? 'AND i.created_at >= ?' : ''}
      ${lastCreatedAt && lastId ? 'AND (i.created_at < ? OR (i.created_at = ? AND ft.id < ?))' : ''}
    ORDER BY i.created_at DESC, ft.id DESC
    LIMIT ?
  `;

  const params: any[] = [fromDate];
  if (toDate) params.push(toDate);
  if (lastCreatedAt && lastId) {
    params.push(lastCreatedAt, lastCreatedAt, parseInt(lastId, 10));
  }
  params.push(limit);

  const [rows] = await mysqlConn.query(query, params);
  return rows as AffiliateNote[];
}

async function processBatch(
  pgClient: any,
  batch: AffiliateNote[],
  batchId: string,
  processedSourceIds: Set<string>
): Promise<void> {
  console.log('\n=== BATCH PROCESSING DETAILS ===\n');

  // Show first 3 raw rows from DIR
  console.log('📥 RAW DATA FROM DIR (first 3 rows):');
  batch.slice(0, 3).forEach((row, idx) => {
    console.log(`\n  Row ${idx + 1}:`);
    console.log(`    formatted_text_id: ${row.formatted_text_id}`);
    console.log(`    note_content: "${row.note_content}"`);
    console.log(`    note_length: ${row.note_content.length} chars`);
    console.log(`    inquiry_id: ${row.inquiry_id}`);
    console.log(`    contact_id: ${row.contact_id}`);
    console.log(`    dir_care_recipient_id: ${row.dir_care_recipient_id}`);
    console.log(`    dir_account_id: ${row.dir_account_id || 'null'}`);
    console.log(`    created_at: ${row.created_at.toISOString()}`);
    console.log(`    updated_at: ${row.updated_at.toISOString()}`);
  });

  // Extract unique IDs for mapping
  const dirCareRecipientIds = [...new Set(batch.map(n => n.dir_care_recipient_id.toString()))];
  const dirAccountIds = [...new Set(batch.map(n => n.dir_account_id).filter(id => id !== null).map(id => id!.toString()))];
  const formattedTextIds = batch.map(n => n.formatted_text_id.toString());

  console.log(`\n📊 BATCH STATISTICS:`);
  console.log(`  Total rows in batch: ${batch.length}`);
  console.log(`  Unique care_recipients: ${dirCareRecipientIds.length}`);
  console.log(`  Unique accounts: ${dirAccountIds.length}`);
  console.log(`  DIR care_recipient IDs: [${dirCareRecipientIds.slice(0, 5).join(', ')}${dirCareRecipientIds.length > 5 ? ', ...' : ''}]`);
  console.log(`  DIR account IDs: [${dirAccountIds.slice(0, 5).join(', ')}${dirAccountIds.length > 5 ? ', ...' : ''}]`);

  // Map care_recipients
  console.log(`\n🔄 MAPPING CARE_RECIPIENTS (DIR → MM):`);
  const careRecipientMap = await mapCareRecipients(pgClient, dirCareRecipientIds);
  console.log(`  Found ${careRecipientMap.size} out of ${dirCareRecipientIds.length} in MM`);

  // Show first few mappings
  let count = 0;
  for (const [dirId, mmId] of careRecipientMap.entries()) {
    if (count < 3) {
      console.log(`    DIR ${dirId} → MM ${mmId}`);
      count++;
    }
  }

  // Map agents
  console.log(`\n🔄 MAPPING AGENTS (DIR → MM):`);
  const agentMap = await mapAgents(pgClient, dirAccountIds);
  console.log(`  Found ${agentMap.size} out of ${dirAccountIds.length} in MM`);

  // Show first few mappings
  count = 0;
  for (const [dirId, mmId] of agentMap.entries()) {
    if (count < 3) {
      console.log(`    DIR ${dirId} → MM ${mmId}`);
      count++;
    }
  }

  // Check existing notes
  console.log(`\n🔍 CHECKING EXISTING NOTES IN MM:`);
  const existingLegacyIds = await checkExistingNotes(pgClient, formattedTextIds, SOURCE_TYPE);
  console.log(`  Found ${existingLegacyIds.size} notes already in MM`);
  if (existingLegacyIds.size > 0) {
    const existing = Array.from(existingLegacyIds).slice(0, 3);
    console.log(`  Examples: [${existing.join(', ')}${existingLegacyIds.size > 3 ? ', ...' : ''}]`);
  }

  // Process each row
  console.log(`\n✨ PROCESSING ROWS (first 3 detailed):`);
  let successCount = 0;
  let skippedCount = 0;
  let duplicateCount = 0;
  let failedCount = 0;
  let detailedLogCount = 0;

  for (const note of batch) {
    const sourceId = note.formatted_text_id.toString();
    const legacyId = note.formatted_text_id.toString();
    const dirCrId = note.dir_care_recipient_id.toString();

    const showDetailedLog = detailedLogCount < 3;

    // Skip if already processed
    if (processedSourceIds.has(sourceId)) {
      continue;
    }

    try {
      // Check if already exists
      if (existingLegacyIds.has(legacyId)) {
        if (showDetailedLog) {
          console.log(`\n  ⊘ Row ${detailedLogCount + 1}: DUPLICATE`);
          console.log(`    Source ID: ${sourceId}`);
          console.log(`    Legacy ID: ${legacyId} (already exists in MM)`);
          detailedLogCount++;
        }
        await appendRow(MIGRATION_NAME, {
          batch_id: batchId,
          source_id: sourceId,
          status: 'duplicate_legacy_id',
          legacy_id: legacyId,
          processed_at: formatDate(new Date()),
        });
        duplicateCount++;
        processedSourceIds.add(sourceId);
        continue;
      }

      // Check care_recipient mapping
      const mmCareRecipientId = careRecipientMap.get(dirCrId);
      if (!mmCareRecipientId) {
        if (showDetailedLog) {
          console.log(`\n  ✗ Row ${detailedLogCount + 1}: SKIPPED (no care_recipient)`);
          console.log(`    Source ID: ${sourceId}`);
          console.log(`    DIR care_recipient_id: ${dirCrId} (not found in MM)`);
          detailedLogCount++;
        }
        await appendRow(MIGRATION_NAME, {
          batch_id: batchId,
          source_id: sourceId,
          status: 'skipped_no_care_recipient',
          dir_care_recipient_id: dirCrId,
          reason: 'care_recipient_not_found_in_mm',
          processed_at: formatDate(new Date()),
        });
        skippedCount++;
        processedSourceIds.add(sourceId);
        continue;
      }

      // Map agent (optional)
      const mmAgentId = note.dir_account_id ? agentMap.get(note.dir_account_id.toString()) : null;

      if (showDetailedLog) {
        console.log(`\n  ✓ Row ${detailedLogCount + 1}: SUCCESS`);
        console.log(`    Source ID: ${sourceId}`);
        console.log(`    Legacy ID: ${legacyId}`);
        console.log(`    DIR care_recipient_id: ${dirCrId} → MM: ${mmCareRecipientId}`);
        console.log(`    DIR account_id: ${note.dir_account_id || 'null'} → MM: ${mmAgentId || 'null'}`);
        console.log(`    Note content: "${note.note_content.substring(0, 80)}${note.note_content.length > 80 ? '...' : ''}"`);
        console.log(`    Note length: ${note.note_content.length} chars`);
        console.log(`    Created at: ${note.created_at.toISOString()}`);
        console.log(`    INSERTING INTO MM:`);
        console.log(`      - legacyId: ${legacyId}`);
        console.log(`      - careRecipientId: ${mmCareRecipientId}`);
        console.log(`      - value: "${note.note_content.trim()}"`);
        console.log(`      - agentAccountId: ${mmAgentId || 'null'}`);
        console.log(`      - agentName: ""`);
        console.log(`      - source: "affiliate_notes"`);
        console.log(`      - createdAt: ${note.created_at.toISOString()}`);
        console.log(`      - updatedAt: ${note.updated_at.toISOString()}`);
        detailedLogCount++;
      }

      // Insert note
      await insertNote(pgClient, {
        legacyId,
        careRecipientId: mmCareRecipientId,
        value: note.note_content.trim(),
        agentAccountId: mmAgentId || null,
        agentName: '', // We don't have agent name from DIR
        source: SOURCE_TYPE,
        createdAt: note.created_at,
        updatedAt: note.updated_at,
      });

      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: sourceId,
        status: 'success',
        legacy_id: legacyId,
        care_recipient_id: mmCareRecipientId,
        processed_at: formatDate(new Date()),
      });
      successCount++;
      processedSourceIds.add(sourceId);

    } catch (error: any) {
      if (showDetailedLog) {
        console.log(`\n  ✗ Row ${detailedLogCount + 1}: FAILED`);
        console.log(`    Source ID: ${sourceId}`);
        console.log(`    Error: ${error.message}`);
        detailedLogCount++;
      }
      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: sourceId,
        status: 'failed',
        reason: 'insert_error',
        error: error.message,
        processed_at: formatDate(new Date()),
      });
      failedCount++;
      processedSourceIds.add(sourceId);
    }
  }

  console.log(`\n📈 BATCH RESULTS:`);
  console.log(`  ✓ Success: ${successCount}`);
  console.log(`  ⊘ Duplicate: ${duplicateCount}`);
  console.log(`  ⊘ Skipped: ${skippedCount}`);
  console.log(`  ✗ Failed: ${failedCount}`);

  // Update summary
  const summary = await readSummary();
  summary[MIGRATION_NAME].total_success += successCount;
  summary[MIGRATION_NAME].total_duplicate += duplicateCount;
  summary[MIGRATION_NAME].total_skipped += skippedCount;
  summary[MIGRATION_NAME].total_failed += failedCount;
  await writeSummary(summary);
}

// Run the migration
migrateAffiliateNotes()
  .then(() => {
    console.log('\n✓ Migration completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n✗ Migration failed:', error);
    process.exit(1);
  });

