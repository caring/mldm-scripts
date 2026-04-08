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

const MIGRATION_NAME = 'care_recipient_lead_notes';

interface MMCareRecipientLead {
  id: string;
  legacyId: string;
  careRecipientId: string;
}

interface MMCareRecipientNote {
  id: string;
  careRecipientId: string;
  noteText: string;
  noteType: string;
  createdAt: Date;
}

interface LeadNoteToInsert {
  id: string;
  leadId: string;
  value: string;
  creator: string;
  createdAt: Date;
  updatedAt: Date;
  mldmMigratedModmonAt: Date;
}

/**
 * Main migration function
 */
async function migrateCareRecipientLeadNotes() {
  if (isHelpRequested()) {
    printMigrationHelp('migrate:care-recipient-lead-notes');
    return;
  }

  const options = parseMigrationArgs();

  console.log('=== Care Recipient Lead Notes Migration ===\n');
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
    console.log('Time range:', migrationSummary.time_range);
  }
  if (migrationSummary.last_processed_date) {
    console.log('Last processed:', migrationSummary.last_processed_date);
  }

  const batches = await readBatches(MIGRATION_NAME);
  console.log('\nBatches completed:', batches.length);

  let totalSuccess = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  for (const batch of batches) {
    const rows = await readRows(MIGRATION_NAME, batch.batchId);
    for (const row of rows) {
      if (row.status === 'SUCCESS') totalSuccess++;
      else if (row.status === 'SKIPPED') totalSkipped++;
      else if (row.status === 'FAILED') totalFailed++;
    }
  }

  console.log('\nRecords processed:');
  console.log(`  Success: ${totalSuccess}`);
  console.log(`  Skipped: ${totalSkipped}`);
  console.log(`  Failed: ${totalFailed}`);
  console.log(`  Total: ${totalSuccess + totalSkipped + totalFailed}`);
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
      // Care seeker ID-based migration
      await runCareSeekerIdBasedMigration(pgClient, careSeekerIds, options);
      return;
    }

    // Time-based migration (default)
    const fromDate = parseTimeParam(options.from);
    const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

    console.log('Migration scope:');
    console.log(`  Leads created after: ${formatDate(fromDate)}`);
    if (toDate) {
      console.log(`  Leads created before: ${formatDate(toDate)}`);
      console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
    }
    console.log();

    await runTimeBasedMigration(pgClient, fromDate, toDate, options);

  } catch (error) {
    console.error('Error in runMigration:', error);
    throw error;
  } finally {
    await disconnectMySQL();
    await disconnectPostgres();
  }
}

/**
 * Run migration for specific care seeker IDs
 */
async function runCareSeekerIdBasedMigration(
  pgClient: any,
  careSeekerIds: number[],
  options: MigrationCLIOptions
) {
  console.log(`Migration scope: ${careSeekerIds.length} care seeker IDs`);
  console.log();

  const batchId = `careseeker_${Date.now()}`;
  await appendBatch(MIGRATION_NAME, { batchId, source: 'care_seeker_ids' });

  // Fetch leads for these care seekers
  console.log('Fetching MM leads for care seekers...');
  const leads = await fetchLeadsForCareSeekers(pgClient, careSeekerIds);
  console.log(`✓ Found ${leads.length} leads to process`);
  console.log();

  await processLeadBatch(pgClient, leads, batchId, options);
}

/**
 * Run incremental time-based migration (only processes care_recipients with new notes)
 */
async function runTimeBasedMigration(
  pgClient: any,
  fromDate: Date,
  toDate: Date | null,
  options: MigrationCLIOptions
) {
  console.log('=== Incremental Note Migration ===\n');

  // Fetch NEW notes created since fromDate
  console.log(`Fetching notes created after: ${formatDate(fromDate)}`);
  const newNotes = await fetchNotesCreatedAfter(pgClient, fromDate);
  console.log(`✓ Found ${newNotes.length} new notes`);
  console.log();

  if (newNotes.length === 0) {
    console.log('No new notes to process');
    console.log('Migration complete (nothing to migrate)');
    return;
  }

  // Get unique affected care_recipient_ids
  const affectedCareRecipientIds = [...new Set(newNotes.map(n => n.careRecipientId))];
  console.log(`Affected care recipients: ${affectedCareRecipientIds.length}`);
  console.log();

  // Process in batches of care_recipients
  const batchSize = 100; // Process 100 care_recipients at a time
  let processedCount = 0;
  let totalLeadsUpdated = 0;
  let totalNotesInserted = 0;

  for (let i = 0; i < affectedCareRecipientIds.length; i += batchSize) {
    const careRecipientBatch = affectedCareRecipientIds.slice(i, i + batchSize);
    const batchId = `batch_${Date.now()}_${Math.floor(i / batchSize) + 1}`;

    console.log(`\n=== Processing Care Recipient Batch ${Math.floor(i / batchSize) + 1} ===`);
    console.log(`Care recipients in batch: ${careRecipientBatch.length}`);

    await appendBatch(MIGRATION_NAME, {
      batchId,
      care_recipient_count: careRecipientBatch.length,
      mode: 'incremental'
    });

    // Fetch ALL leads for these care_recipients
    const leads = await fetchLeadsByCareRecipients(pgClient, careRecipientBatch);
    console.log(`Found ${leads.length} leads to update`);

    // Process each lead (delete old migrated notes + insert all current notes)
    const result = await processLeadsWithIncrementalUpdate(
      pgClient,
      leads,
      batchId,
      options
    );

    processedCount += careRecipientBatch.length;
    totalLeadsUpdated += result.leadsUpdated;
    totalNotesInserted += result.notesInserted;

    console.log(`Progress: ${processedCount}/${affectedCareRecipientIds.length} care recipients processed`);
  }

  console.log('\n=== Migration Complete ===');
  console.log(`Total care recipients processed: ${processedCount}`);
  console.log(`Total leads updated: ${totalLeadsUpdated}`);
  console.log(`Total notes inserted: ${totalNotesInserted}`);
}

/**
 * Process leads with incremental update (delete-then-insert pattern)
 */
async function processLeadsWithIncrementalUpdate(
  pgClient: any,
  leads: MMCareRecipientLead[],
  batchId: string,
  options: MigrationCLIOptions
): Promise<{ leadsUpdated: number; notesInserted: number }> {
  if (leads.length === 0) return { leadsUpdated: 0, notesInserted: 0 };

  // Get unique care recipient IDs
  const careRecipientIds = [...new Set(leads.map(l => l.careRecipientId))];

  console.log(`Fetching ALL notes for ${careRecipientIds.length} care recipients...`);
  const notesMap = await fetchNotesForCareRecipients(pgClient, careRecipientIds);
  console.log(`✓ Fetched notes for ${Object.keys(notesMap).length} care recipients`);
  console.log();

  let leadsUpdated = 0;
  let notesInserted = 0;
  let skipped = 0;

  for (const lead of leads) {
    const notes = notesMap[lead.careRecipientId];

    if (!notes || notes.length === 0) {
      skipped++;
      await appendRow(MIGRATION_NAME, batchId, {
        id: lead.id,
        legacy_id: lead.legacyId,
        status: 'SKIPPED',
        reason: 'No notes found',
      } as RowRecord);
      console.log(`  ⊘ Lead ${lead.legacyId}: No notes`);
      continue;
    }

    if (options.dryRun) {
      console.log(`  [DRY RUN] Lead ${lead.legacyId}: Would delete existing + insert ${notes.length} notes`);
      leadsUpdated++;
      notesInserted += notes.length;
      continue;
    }

    try {
      await pgClient.query('BEGIN');

      // Delete existing migrated notes for this lead
      const deletedCount = await deleteMigratedNotesForLead(pgClient, lead.id);

      // Prepare ALL notes for insertion
      const notesToInsert: LeadNoteToInsert[] = notes.map(note => ({
        id: generateUUID(),
        leadId: lead.id,
        value: formatNoteValue(note),
        creator: 'MLDM Migration',
        createdAt: note.createdAt,
        updatedAt: note.createdAt,
        mldmMigratedModmonAt: new Date(),
      }));

      // Insert ALL notes
      await bulkInsertLeadNotes(pgClient, notesToInsert);

      await pgClient.query('COMMIT');

      leadsUpdated++;
      notesInserted += notes.length;

      await appendRow(MIGRATION_NAME, batchId, {
        id: lead.id,
        legacy_id: lead.legacyId,
        status: 'SUCCESS',
        deleted_count: deletedCount,
        inserted_count: notes.length,
      } as RowRecord);

      console.log(`  ✓ Lead ${lead.legacyId}: Deleted ${deletedCount}, Inserted ${notes.length} notes`);

    } catch (error) {
      await pgClient.query('ROLLBACK');
      console.error(`  ✗ Lead ${lead.legacyId}: Error -`, error);

      await appendRow(MIGRATION_NAME, batchId, {
        id: lead.id,
        legacy_id: lead.legacyId,
        status: 'FAILED',
        error: error instanceof Error ? error.message : String(error),
      } as RowRecord);
    }
  }

  console.log();
  console.log(`Summary: ${leadsUpdated} leads updated, ${notesInserted} notes inserted, ${skipped} leads skipped`);
  console.log();

  return { leadsUpdated, notesInserted };
}

/**
 * Process a batch of leads (legacy function for care-seeker mode)
 */
async function processLeadBatch(
  pgClient: any,
  leads: MMCareRecipientLead[],
  batchId: string,
  options: MigrationCLIOptions
) {
  if (leads.length === 0) return;

  // Get unique care recipient IDs
  const careRecipientIds = [...new Set(leads.map(l => l.careRecipientId))];

  console.log(`Fetching notes for ${careRecipientIds.length} care recipients...`);
  const notesMap = await fetchNotesForCareRecipients(pgClient, careRecipientIds);
  console.log(`✓ Fetched notes for ${Object.keys(notesMap).length} care recipients`);
  console.log();

  const notesToInsert: LeadNoteToInsert[] = [];
  let totalNotesCount = 0;
  let skipped = 0;

  for (const lead of leads) {
    const notes = notesMap[lead.careRecipientId];

    if (!notes || notes.length === 0) {
      skipped++;
      await appendRow(MIGRATION_NAME, batchId, {
        id: lead.id,
        legacy_id: lead.legacyId,
        status: 'SKIPPED',
        reason: 'No notes found',
      } as RowRecord);
      console.log(`  ⊘ Lead ${lead.legacyId}: No notes`);
      continue;
    }

    // Create individual note records for each note
    for (const note of notes) {
      notesToInsert.push({
        id: generateUUID(),
        leadId: lead.id,
        value: formatNoteValue(note),
        creator: 'MLDM Migration',
        createdAt: note.createdAt,
        updatedAt: note.createdAt,
        mldmMigratedModmonAt: new Date(),
      });
    }

    totalNotesCount += notes.length;
    await appendRow(MIGRATION_NAME, batchId, {
      id: lead.id,
      legacy_id: lead.legacyId,
      status: 'SUCCESS',
      notes_count: notes.length,
    } as RowRecord);
    console.log(`  ✓ Lead ${lead.legacyId}: Prepared ${notes.length} notes`);
  }

  console.log();
  console.log(`Summary: ${totalNotesCount} notes prepared for ${leads.length - skipped} leads, ${skipped} leads skipped`);
  console.log();

  if (notesToInsert.length > 0 && !options.dryRun) {
    console.log(`Inserting ${notesToInsert.length} notes into care_recipient_leads_notes...`);
    await bulkInsertLeadNotes(pgClient, notesToInsert);
    console.log('✓ Insert complete');
  } else if (options.dryRun) {
    console.log(`[DRY RUN] Would insert ${notesToInsert.length} notes`);
  }
}

/**
 * Fetch leads for care seekers
 */
async function fetchLeadsForCareSeekers(
  pgClient: any,
  careSeekerIds: number[]
): Promise<MMCareRecipientLead[]> {
  if (careSeekerIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT
      crl.id,
      crl."legacyId",
      crl."careRecipientId"
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
    id: row.id,
    legacyId: row.legacyId,
    careRecipientId: row.careRecipientId,
  }));
}

/**
 * Fetch batch of leads (time-based)
 */
async function fetchLeadBatch(
  pgClient: any,
  fromDate: Date,
  toDate: Date | null,
  batchSize: number,
  offset: number
): Promise<MMCareRecipientLead[]> {
  let query = `
    SELECT
      id,
      "legacyId",
      "careRecipientId"
    FROM care_recipient_leads
    WHERE "createdAt" >= $1
  `;
  const params: any[] = [fromDate];

  if (toDate) {
    query += ` AND "createdAt" <= $2`;
    params.push(toDate);
  }

  query += `
      AND "deletedAt" IS NULL
    ORDER BY "createdAt" DESC
    LIMIT $${params.length + 1} OFFSET $${params.length + 2}
  `;
  params.push(batchSize, offset);

  const result = await pgClient.query(query, params);

  return result.rows.map((row: any) => ({
    id: row.id,
    legacyId: row.legacyId,
    careRecipientId: row.careRecipientId,
  }));
}

/**
 * Fetch notes for care recipients
 */
async function fetchNotesForCareRecipients(
  pgClient: any,
  careRecipientIds: string[]
): Promise<Record<string, MMCareRecipientNote[]>> {
  if (careRecipientIds.length === 0) return {};

  const result = await pgClient.query(
    `
    SELECT
      id,
      "careRecipientId",
      "noteText",
      "noteType",
      "createdAt"
    FROM care_recipient_notes
    WHERE "careRecipientId" = ANY($1)
      AND "deletedAt" IS NULL
      AND "noteType" IN ('AFFILIATE', 'INTERNAL')
    ORDER BY "careRecipientId", "createdAt" DESC
    `,
    [careRecipientIds]
  );

  const notesMap: Record<string, MMCareRecipientNote[]> = {};

  for (const row of result.rows) {
    const crId = row.careRecipientId;
    if (!notesMap[crId]) {
      notesMap[crId] = [];
    }
    notesMap[crId].push({
      id: row.id,
      careRecipientId: row.careRecipientId,
      noteText: row.noteText,
      noteType: row.noteType,
      createdAt: new Date(row.createdAt),
    });
  }

  return notesMap;
}

/**
 * Format note value with type prefix
 */
function formatNoteValue(note: MMCareRecipientNote): string {
  return `[${note.noteType}] ${note.noteText}`;
}

/**
 * Generate UUID v4
 */
function generateUUID(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Bulk insert lead notes
 */
async function bulkInsertLeadNotes(
  pgClient: any,
  notes: LeadNoteToInsert[]
): Promise<void> {
  if (notes.length === 0) return;

  // Insert in batches of 1000 to avoid parameter limit
  const BATCH_SIZE = 1000;
  for (let i = 0; i < notes.length; i += BATCH_SIZE) {
    const batch = notes.slice(i, i + BATCH_SIZE);

    const values: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    for (const note of batch) {
      values.push(
        `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6})`
      );
      params.push(
        note.id,
        note.leadId,
        note.value,
        note.creator,
        note.createdAt,
        note.updatedAt,
        note.mldmMigratedModmonAt
      );
      paramIndex += 7;
    }

    const query = `
      INSERT INTO care_recipient_leads_notes
        (id, "leadId", value, creator, "createdAt", "updatedAt", "mldmMigratedModmonAt")
      VALUES ${values.join(', ')}
    `;

    await pgClient.query(query, params);
  }
}

// Export for testing
export {
  formatNoteValue,
  generateUUID,
};

/**
 * Fetch new notes created since last run (incremental approach)
 */
async function fetchNotesCreatedAfter(
  pgClient: any,
  afterDate: Date
): Promise<MMCareRecipientNote[]> {
  const result = await pgClient.query(
    `
    SELECT
      id,
      "careRecipientId",
      "noteText",
      "noteType",
      "createdAt"
    FROM care_recipient_notes
    WHERE "createdAt" > $1
      AND "deletedAt" IS NULL
      AND "noteType" IN ('AFFILIATE', 'INTERNAL')
    ORDER BY "createdAt" ASC
    `,
    [afterDate]
  );

  return result.rows.map((row: any) => ({
    id: row.id,
    careRecipientId: row.careRecipientId,
    noteText: row.noteText,
    noteType: row.noteType,
    createdAt: row.createdAt,
  }));
}

/**
 * Fetch ALL leads for specific care_recipient_ids
 */
async function fetchLeadsByCareRecipients(
  pgClient: any,
  careRecipientIds: string[]
): Promise<MMCareRecipientLead[]> {
  if (careRecipientIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT
      id,
      "legacyId",
      "careRecipientId"
    FROM care_recipient_leads
    WHERE "careRecipientId" = ANY($1)
      AND "deletedAt" IS NULL
    ORDER BY "createdAt" DESC
    `,
    [careRecipientIds]
  );

  return result.rows.map((row: any) => ({
    id: row.id,
    legacyId: row.legacyId,
    careRecipientId: row.careRecipientId,
  }));
}

/**
 * Delete existing migrated notes for a lead
 */
async function deleteMigratedNotesForLead(
  pgClient: any,
  leadId: string
): Promise<number> {
  const result = await pgClient.query(
    `
    DELETE FROM care_recipient_leads_notes
    WHERE "leadId" = $1
      AND "mldmMigratedModmonAt" IS NOT NULL
    `,
    [leadId]
  );

  return result.rowCount || 0;
}

// Run migration if called directly
if (require.main === module) {
  migrateCareRecipientLeadNotes().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}