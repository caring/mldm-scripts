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
} from '../../utils/migration-cli';
import {
  mapCareRecipients,
  mapAgents,
  checkExistingNotes,
  insertNote,
} from '../../utils/migration-db';

const MIGRATION_NAME = 'self_qualified_notes';
const SOURCE_TYPE = 'self_qualified_notes';

interface SelfQualifiedNote {
  contact_id: number;
  care_recipient_id: number;
  self_qualified_notes: string;
  account_id: number | null;
  created_at: Date;
  updated_at: Date;
}

/**
 * Generate report
 */
async function generateReport() {
  console.log('=== Self-Qualified Notes Migration Report ===\n');

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
    console.log('To retry failed rows: npm run migrate:self-qualified-notes -- --retry-failed');
  }

  console.log('Files:');
  console.log(`  Batches: migration-state/${MIGRATION_NAME}/batches.jsonl`);
  console.log(`  Rows: migration-state/${MIGRATION_NAME}/rows.jsonl`);
  console.log(`  Summary: migration-state/summary.json`);
}

/**
 * Retry failed rows
 */
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
  console.log('1. Review failed rows in migration-state/self_qualified_notes/rows.jsonl');
  console.log('2. Fix the underlying issues');
  console.log('3. Re-run the migration (it will skip already-processed rows)');
}

/**
 * Main migration function
 */
async function migrateSelfQualifiedNotes() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:self-qualified-notes');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Self-Qualified Notes Migration ===\n');
  printMigrationOptions(options);

  try {
    await ensureMigrationDir(MIGRATION_NAME);

    if (options.report) {
      await generateReport();
      return;
    }

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

/**
 * Run the migration
 */
async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectMySQL();
  await connectPostgres();
  console.log('✓ Connected to both databases\n');

  const mysqlConn = getMySQLConnection();
  const pgClient = getPostgresClient();

  try {
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
        console.log('Sample row:', {
          contact_id: batch[0].contact_id,
          note_preview: batch[0].self_qualified_notes.substring(0, 50) + '...',
          created_at: batch[0].created_at,
        });
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
      lastId = lastRow.contact_id.toString();

      // Update summary
      summary = await readSummary();
      summary[MIGRATION_NAME].total_batches++;
      summary[MIGRATION_NAME].completed_batches++;
      summary[MIGRATION_NAME].total_rows_fetched += batch.length;
      summary[MIGRATION_NAME].last_updated_at = formatDate(new Date());
      await writeSummary(summary);

      batchNumber++;

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

  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
    console.log('\n✓ Disconnected from databases');
  }
}

/**
 * Fetch a batch of self-qualified notes from DIR
 */
async function fetchBatch(
  mysqlConn: any,
  fromDate: Date,
  toDate: Date | null,
  lastCreatedAt: Date | null,
  lastId: string | null,
  limit: number
): Promise<SelfQualifiedNote[]> {
  const query = `
    SELECT
      c.id as contact_id,
      c.care_recipient_id,
      c.self_qualified_notes,
      c.account_id,
      c.created_at,
      c.updated_at
    FROM contacts c
    WHERE c.self_qualified_notes IS NOT NULL
      AND TRIM(c.self_qualified_notes) != ''
      AND c.care_recipient_id IS NOT NULL
      AND c.created_at <= ?
      ${toDate ? 'AND c.created_at >= ?' : ''}
      ${lastCreatedAt && lastId ? 'AND (c.created_at < ? OR (c.created_at = ? AND c.id < ?))' : ''}
    ORDER BY c.created_at DESC, c.id DESC
    LIMIT ?
  `;

  const params: any[] = [fromDate];
  if (toDate) params.push(toDate);
  if (lastCreatedAt && lastId) {
    params.push(lastCreatedAt, lastCreatedAt, parseInt(lastId, 10));
  }
  params.push(limit);

  const [rows] = await mysqlConn.query(query, params);
  return rows as SelfQualifiedNote[];
}

/**
 * Process a batch of self-qualified notes
 */
async function processBatch(
  pgClient: any,
  batch: SelfQualifiedNote[],
  batchId: string,
  processedSourceIds: Set<string>
): Promise<void> {
  console.log('\n=== BATCH PROCESSING DETAILS ===\n');

  // Show first 3 raw rows from DIR
  console.log('📥 RAW DATA FROM DIR (first 3 rows):');
  batch.slice(0, 3).forEach((row, idx) => {
    console.log(`\n  Row ${idx + 1}:`);
    console.log(`    contact_id: ${row.contact_id}`);
    console.log(`    self_qualified_notes: "${row.self_qualified_notes}"`);
    console.log(`    note_length: ${row.self_qualified_notes.length} chars`);
    console.log(`    care_recipient_id: ${row.care_recipient_id}`);
    console.log(`    account_id: ${row.account_id || 'null'}`);
    console.log(`    created_at: ${row.created_at.toISOString()}`);
    console.log(`    updated_at: ${row.updated_at.toISOString()}`);
  });

  // Extract unique IDs for mapping
  const dirCareRecipientIds = [...new Set(batch.map(n => n.care_recipient_id.toString()))];
  const dirAccountIds = [...new Set(batch.map(n => n.account_id).filter(id => id !== null).map(id => id!.toString()))];
  const contactIds = batch.map(n => n.contact_id.toString());

  console.log(`\n📊 BATCH STATISTICS:`);
  console.log(`  Total rows in batch: ${batch.length}`);
  console.log(`  Unique care_recipients: ${dirCareRecipientIds.length}`);
  console.log(`  Unique accounts: ${dirAccountIds.length}`);

  // Map care_recipients
  console.log(`\n🔄 MAPPING CARE_RECIPIENTS (DIR → MM):`);
  const careRecipientMap = await mapCareRecipients(pgClient, dirCareRecipientIds);
  console.log(`  Found ${careRecipientMap.size} out of ${dirCareRecipientIds.length} in MM`);

  // Map agents
  console.log(`\n🔄 MAPPING AGENTS (DIR → MM):`);
  const agentMap = await mapAgents(pgClient, dirAccountIds);
  console.log(`  Found ${agentMap.size} out of ${dirAccountIds.length} in MM`);

  // Check existing notes
  console.log(`\n🔍 CHECKING EXISTING NOTES IN MM:`);
  const existingLegacyIds = await checkExistingNotes(pgClient, contactIds, SOURCE_TYPE);
  console.log(`  Found ${existingLegacyIds.size} notes already in MM`);

  // Process each row
  console.log(`\n✨ PROCESSING ROWS (first 3 detailed):`);
  let successCount = 0;
  let skippedCount = 0;
  let duplicateCount = 0;
  let failedCount = 0;
  let detailedLogCount = 0;

  for (const note of batch) {
    const sourceId = note.contact_id.toString();
    const legacyId = note.contact_id.toString();
    const dirCrId = note.care_recipient_id.toString();

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
      const mmAgentId = note.account_id ? agentMap.get(note.account_id.toString()) : null;

      if (showDetailedLog) {
        console.log(`\n  ✓ Row ${detailedLogCount + 1}: SUCCESS`);
        console.log(`    Source ID: ${sourceId}`);
        console.log(`    Legacy ID: ${legacyId}`);
        console.log(`    DIR care_recipient_id: ${dirCrId} → MM: ${mmCareRecipientId}`);
        console.log(`    DIR account_id: ${note.account_id || 'null'} → MM: ${mmAgentId || 'null'}`);
        console.log(`    Note content: "${note.self_qualified_notes.substring(0, 80)}${note.self_qualified_notes.length > 80 ? '...' : ''}"`);
        console.log(`    Note length: ${note.self_qualified_notes.length} chars`);
        console.log(`    INSERTING INTO MM:`);
        console.log(`      - legacyId: ${legacyId}`);
        console.log(`      - careRecipientId: ${mmCareRecipientId}`);
        console.log(`      - value: "${note.self_qualified_notes.trim()}"`);
        console.log(`      - agentAccountId: ${mmAgentId || 'null'}`);
        console.log(`      - source: "${SOURCE_TYPE}"`);
        detailedLogCount++;
      }

      // Insert note
      await insertNote(pgClient, {
        legacyId,
        careRecipientId: mmCareRecipientId,
        value: note.self_qualified_notes.trim(),
        agentAccountId: mmAgentId || null,
        agentName: '',
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
migrateSelfQualifiedNotes()
  .then(() => {
    console.log('\n✓ Migration completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n✗ Migration failed:', error);
    process.exit(1);
  });

