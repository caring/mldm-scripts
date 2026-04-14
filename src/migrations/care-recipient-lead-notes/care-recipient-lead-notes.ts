import { connectPostgres, disconnectPostgres, getPostgresClient } from '../../db/postgres';
import { parseTimeParam, formatDate, getRelativeDescription } from '../../utils/time-utils';
import {
  ensureMigrationDir,
  appendBatch,
  appendRow,
  appendRows,
  readBatches,
  readRows,
  readSummary,
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
const MIGRATION_CREATOR = 'MLDM Migration';
const MAX_CONCATENATED_NOTE_LENGTH = 3000;

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

interface ConcatenatedLeadNotePayload {
  value: string;
  includedNotesCount: number;
  createdAt: Date;
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

  const batches = await readBatches(MIGRATION_NAME);
  console.log('\nBatches completed:', batches.length);

  let totalSuccess = 0;
  let totalSkipped = 0;
  let totalFailed = 0;

  const rows = await readRows(MIGRATION_NAME);
  for (const row of rows) {
    if (row.status === 'success') totalSuccess++;
    else if (row.status.startsWith('skipped')) totalSkipped++;
    else if (row.status === 'failed') totalFailed++;
  }

  console.log('\nRecords processed:');
  console.log(`  Success: ${totalSuccess}`);
  console.log(`  Skipped: ${totalSkipped}`);
  console.log(`  Failed: ${totalFailed}`);
  console.log(`  Total: ${totalSuccess + totalSkipped + totalFailed}`);
}

async function runMigration(options: MigrationCLIOptions) {
  console.log('Connecting to databases...');
  await connectPostgres();
  console.log('✓ Connected to PostgreSQL\n');

  const pgClient = getPostgresClient();
  const fromDate = parseTimeParam(options.from);
  const toDate = options.to ? parseTimeParam(options.to, fromDate) : null;

  try {
    // Check if --ids parameter is provided (care seeker IDs)
    const careSeekerIds = await parseIds(options);
    
    if (careSeekerIds && careSeekerIds.length > 0) {
      // Care seeker ID-based migration
      await runCareSeekerIdBasedMigration(pgClient, careSeekerIds, options);
      return;
    }

    console.log('Migration scope:');
    console.log(`  Notes created after: ${formatDate(fromDate)}`);
    if (toDate) {
      console.log(`  Notes created before: ${formatDate(toDate)}`);
      console.log(`  Range: ${getRelativeDescription(fromDate, toDate)}`);
    }
    console.log();

    await runTimeBasedMigration(pgClient, fromDate, toDate, options);

  } catch (error) {
    console.error('Error in runMigration:', error);
    throw error;
  } finally {
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
  await appendBatch(MIGRATION_NAME, { batch_id: batchId, source: 'care_seeker_ids' } as any);

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
  console.log('=== Bulk Care Recipient Migration ===\n');

  // Read last care_recipient_id from rows.jsonl as cursor
  const existingRows = await readRows(MIGRATION_NAME);
  const existingBatches = await readBatches(MIGRATION_NAME);
  const batchSize = options.batchSize;
  let batchNumber = existingBatches.length;
  let hasMore = true;
  let totalLeadsUpdated = 0;
  let totalNotesInserted = 0;

  // Get cursor from last processed row
  let cursor = '00000000-0000-0000-0000-000000000000';
  if (existingRows.length > 0) {
    const lastRow = existingRows[existingRows.length - 1];
    cursor = lastRow.care_recipient_id || cursor;
  }

  if (existingRows.length > 0) {
    console.log(`📌 RESUMING from cursor: ${cursor.substring(0, 8)}...`);
    console.log(`   Previous batches completed: ${existingBatches.length}`);
    console.log(`   Rows processed: ${existingRows.length}\n`);
  }

  console.log(`Batch size: ${batchSize}\n`);

  while (hasMore) {
    batchNumber++;
    const batchId = `batch_${String(batchNumber).padStart(6, '0')}`;

    console.log(`\n=== Batch ${batchNumber} (cursor: ${cursor.substring(0, 8)}...) ===`);

    const careRecipientIds = await fetchCareRecipientIdsWithNotesAndLeads(
      pgClient, fromDate, toDate, batchSize, cursor
    );

    if (careRecipientIds.length === 0) {
      hasMore = false;
      break;
    }

    console.log(`Care recipients: ${careRecipientIds.length}`);

    const leads = await fetchLeadsByCareRecipients(pgClient, careRecipientIds);
    console.log(`Leads: ${leads.length}`);

    // Filter: only process care_recipients who have leads
    const careRecipientIdsWithLeads = [...new Set(leads.map(l => l.careRecipientId))];
    const careRecipientsWithoutLeads = careRecipientIds.filter(id => !careRecipientIdsWithLeads.includes(id));

    if (careRecipientsWithoutLeads.length > 0) {
      console.log(`Skipping ${careRecipientsWithoutLeads.length} care_recipients with no leads`);
    }

    const batchStartTime = new Date();

    const result = await processBatchInOneTransaction(
      pgClient, leads, careRecipientIdsWithLeads, batchId, options
    );

    totalLeadsUpdated += result.leadsUpdated;
    totalNotesInserted += result.notesInserted;

    // Write batch completion with stats
    const batchEndTime = new Date();
    const durationSeconds = Math.floor((batchEndTime.getTime() - batchStartTime.getTime()) / 1000);

    // Update cursor to last care_recipient_id processed
    const lastCareRecipientId = careRecipientIds[careRecipientIds.length - 1];

    await appendBatch(MIGRATION_NAME, {
      batch_id: batchId,
      care_recipient_count: careRecipientIds.length,
      mode: 'bulk',
      last_care_recipient_id: lastCareRecipientId,
      started_at: formatDate(batchStartTime),
      completed_at: formatDate(batchEndTime),
      duration_seconds: durationSeconds,
      leads_updated: result.leadsUpdated,
      notes_inserted: result.notesInserted,
      status: 'completed',
    } as any);

    if (careRecipientIds.length < batchSize) {
      hasMore = false;
    } else {
      cursor = lastCareRecipientId; // Move cursor forward
    }
  }

  console.log('\n=== Complete ===');
  console.log(`Leads updated: ${totalLeadsUpdated}`);
  console.log(`Notes inserted: ${totalNotesInserted}`);
}

/**
 * Process leads with incremental update (delete-then-insert pattern)
 * DEPRECATED: Use processBatchInOneTransaction instead
 */
// @ts-ignore - unused but kept for reference
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
      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: lead.id,
        legacy_id: lead.legacyId,
        status: 'skipped_no_care_recipient',
        processed_at: formatDate(new Date()),
      } as RowRecord);
      console.log(`  ⊘ Lead ${lead.legacyId}: No notes`);
      continue;
    }

    if (options.dryRun) {
      const payload = buildConcatenatedLeadNotePayload(notes);
      console.log(
        `  [DRY RUN] Lead ${lead.legacyId}: Would delete existing + insert ${payload ? 1 : 0} lead note row`
      );
      leadsUpdated++;
      notesInserted += payload ? 1 : 0;
      continue;
    }

    try {
      await pgClient.query('BEGIN');

      // Delete existing migrated notes for this lead
      const deletedCount = await deleteMigratedNotesForLead(pgClient, lead.id);

      const notesToInsert = buildLeadNotesToInsert(
        [lead],
        { [lead.careRecipientId]: notes },
        new Date()
      );

      // Insert aggregated lead note
      await bulkInsertLeadNotes(pgClient, notesToInsert);

      await pgClient.query('COMMIT');

      leadsUpdated++;
      notesInserted += notesToInsert.length;

      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: lead.id,
        legacy_id: lead.legacyId,
        status: 'success',
        processed_at: formatDate(new Date()),
      } as RowRecord);

      console.log(`  ✓ Lead ${lead.legacyId}: Deleted ${deletedCount}, Inserted ${notesToInsert.length} lead note row`);

    } catch (error) {
      await pgClient.query('ROLLBACK');
      console.error(`  ✗ Lead ${lead.legacyId}: Error -`, error);

      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: lead.id,
        legacy_id: lead.legacyId,
        status: 'failed',
        error: error instanceof Error ? error.message : String(error),
        processed_at: formatDate(new Date()),
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
  let totalLeadNoteRows = 0;
  let skipped = 0;
  const migratedAt = new Date();

  for (const lead of leads) {
    const notes = notesMap[lead.careRecipientId];

    if (!notes || notes.length === 0) {
      skipped++;
      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: lead.id,
        legacy_id: lead.legacyId,
        status: 'skipped_no_care_recipient',
        processed_at: formatDate(new Date()),
      } as RowRecord);
      console.log(`  ⊘ Lead ${lead.legacyId}: No notes`);
      continue;
    }

    const payload = buildConcatenatedLeadNotePayload(notes);
    if (!payload) {
      skipped++;
      await appendRow(MIGRATION_NAME, {
        batch_id: batchId,
        source_id: lead.id,
        legacy_id: lead.legacyId,
        status: 'skipped_no_care_recipient',
        processed_at: formatDate(new Date()),
      } as RowRecord);
      console.log(`  ⊘ Lead ${lead.legacyId}: No usable notes after concatenation`);
      continue;
    }

    notesToInsert.push({
      id: generateUUID(),
      leadId: lead.id,
      value: payload.value,
      creator: MIGRATION_CREATOR,
      createdAt: payload.createdAt,
      updatedAt: payload.createdAt,
      mldmMigratedModmonAt: migratedAt,
    });

    totalLeadNoteRows += 1;
    await appendRow(MIGRATION_NAME, {
      batch_id: batchId,
      source_id: lead.id,
      legacy_id: lead.legacyId,
      status: 'success',
      processed_at: formatDate(new Date()),
    } as RowRecord);
    console.log(
      `  ✓ Lead ${lead.legacyId}: Prepared 1 lead note row from ${payload.includedNotesCount} notes`
    );
  }

  console.log();
  console.log(
    `Summary: ${totalLeadNoteRows} lead note rows prepared for ${leads.length - skipped} leads, ${skipped} leads skipped`
  );
  console.log();

  if (notesToInsert.length > 0 && !options.dryRun) {
    console.log(`Inserting ${notesToInsert.length} lead note rows into care_recipient_leads_notes...`);
    await bulkInsertLeadNotes(pgClient, notesToInsert);
    console.log('✓ Insert complete');
  } else if (options.dryRun) {
    console.log(`[DRY RUN] Would insert ${notesToInsert.length} lead note rows`);
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
 * Fetch notes for care recipients (OPTIMIZED: Two separate queries)
 * Query 1: ALL affiliate notes (1 per care_recipient)
 * Query 2: Top 20 internal notes per care_recipient
 */
async function fetchNotesForCareRecipients(
  pgClient: any,
  careRecipientIds: string[]
): Promise<Record<string, MMCareRecipientNote[]>> {
  if (careRecipientIds.length === 0) return {};

  // Query 1: Get ALL affiliate notes (usually 1 per care_recipient)
  const affiliateResult = await pgClient.query(
    `
    SELECT
      id,
      "careRecipientId",
      value as "noteText",
      source as "noteType",
      "createdAt"
    FROM care_recipient_notes
    WHERE "careRecipientId" = ANY($1)
      AND source = 'affiliate_notes'
      AND "deletedAt" IS NULL
    ORDER BY "careRecipientId", "createdAt" DESC
    `,
    [careRecipientIds]
  );

  // Query 2: Get top 20 internal notes per care_recipient using ROW_NUMBER
  const internalResult = await pgClient.query(
    `
    SELECT id, "careRecipientId", "noteText", "noteType", "createdAt"
    FROM (
      SELECT
        id,
        "careRecipientId",
        value as "noteText",
        source as "noteType",
        "createdAt",
        ROW_NUMBER() OVER (
          PARTITION BY "careRecipientId"
          ORDER BY "createdAt" DESC
        ) as rn
      FROM care_recipient_notes
      WHERE "careRecipientId" = ANY($1)
        AND source = 'internal_notes'
        AND "deletedAt" IS NULL
    ) sub
    WHERE rn <= 20
    `,
    [careRecipientIds]
  );

  // Combine results into map
  const notesMap: Record<string, MMCareRecipientNote[]> = {};

  // Add affiliate notes first
  for (const row of affiliateResult.rows) {
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

  // Add internal notes
  for (const row of internalResult.rows) {
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

function buildConcatenatedLeadNotePayload(
  notes: MMCareRecipientNote[],
  maxLength = MAX_CONCATENATED_NOTE_LENGTH
): ConcatenatedLeadNotePayload | null {
  if (notes.length === 0) {
    return null;
  }

  const formattedNotes: string[] = [];
  let includedNotesCount = 0;
  let latestCreatedAt = notes[0].createdAt;
  let currentLength = 0;

  for (const note of notes) {
    const formattedNote = formatNoteValue(note);
    if (!formattedNote) {
      continue;
    }

    if (note.createdAt > latestCreatedAt) {
      latestCreatedAt = note.createdAt;
    }

    const separatorLength = formattedNotes.length > 0 ? 2 : 0;
    const nextLength = currentLength + separatorLength + formattedNote.length;

    if (nextLength > maxLength) {
      if (formattedNotes.length === 0) {
        return {
          value: formattedNote.slice(0, maxLength),
          includedNotesCount: 1,
          createdAt: note.createdAt,
        };
      }

      break;
    }

    formattedNotes.push(formattedNote);
    currentLength = nextLength;
    includedNotesCount += 1;
  }

  if (formattedNotes.length === 0) {
    return null;
  }

  return {
    value: formattedNotes.join('\n\n'),
    includedNotesCount,
    createdAt: latestCreatedAt,
  };
}

function buildLeadNotesToInsert(
  leads: MMCareRecipientLead[],
  notesMap: Record<string, MMCareRecipientNote[]>,
  migratedAt = new Date()
): LeadNoteToInsert[] {
  const notesToInsert: LeadNoteToInsert[] = [];

  for (const lead of leads) {
    const payload = buildConcatenatedLeadNotePayload(notesMap[lead.careRecipientId] || []);
    if (!payload) {
      continue;
    }

    notesToInsert.push({
      id: generateUUID(),
      leadId: lead.id,
      value: payload.value,
      creator: MIGRATION_CREATOR,
      createdAt: payload.createdAt,
      updatedAt: payload.createdAt,
      mldmMigratedModmonAt: migratedAt,
    });
  }

  return notesToInsert;
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
  buildConcatenatedLeadNotePayload,
  buildLeadNotesToInsert,
  formatNoteValue,
  generateUUID,
  fetchCareRecipientIdsWithNotesAndLeads,
};

/**
 * Fetch new notes created since last run (incremental approach)
 * DEPRECATED: Use fetchCareRecipientIdsWithNotesAndLeads instead
 */
// @ts-ignore - unused but kept for reference
async function fetchNotesCreatedAfter(
  pgClient: any,
  afterDate: Date
): Promise<MMCareRecipientNote[]> {
  const result = await pgClient.query(
    `
    SELECT
      id,
      "careRecipientId",
      value as "noteText",
      source as "noteType",
      "createdAt"
    FROM care_recipient_notes
    WHERE "createdAt" > $1
      AND "deletedAt" IS NULL
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

/**
 * Fetch care_recipient_ids who have notes
 * NO JOIN - uses cursor from rows.jsonl for resume
 * Much faster than JOIN approach!
 */
async function fetchCareRecipientIdsWithNotesAndLeads(
  pgClient: any,
  fromDate: Date,
  toDate: Date | null,
  limit: number,
  cursor: string
): Promise<string[]> {
  const params: Array<number | string | Date> = [limit, fromDate, cursor];
  const toDateFilter = toDate ? '\n      AND "createdAt" <= $4' : '';

  if (toDate) {
    params.push(toDate);
  }

  const result = await pgClient.query(
    `
    SELECT DISTINCT "careRecipientId"
    FROM care_recipient_notes crn
    WHERE crn."deletedAt" IS NULL
      AND crn."createdAt" >= $2${toDateFilter}
      AND crn."careRecipientId" > $3
      AND EXISTS (
        SELECT 1
        FROM care_recipient_leads crl
        WHERE crl."careRecipientId" = crn."careRecipientId"
          AND crl."deletedAt" IS NULL
      )
    ORDER BY "careRecipientId"
    LIMIT $1
    `,
    params
  );

  return result.rows.map((row: any) => row.careRecipientId);
}

/**
 * Process batch in ONE TRANSACTION
 */
async function processBatchInOneTransaction(
  pgClient: any,
  leads: MMCareRecipientLead[],
  careRecipientIds: string[],
  batchId: string,
  options: MigrationCLIOptions
): Promise<{ leadsUpdated: number; notesInserted: number }> {
  if (leads.length === 0) return { leadsUpdated: 0, notesInserted: 0 };

  console.log(`\nFetching notes for ${careRecipientIds.length} care recipients...`);
  const notesMap = await fetchNotesForCareRecipients(pgClient, careRecipientIds);

  // Count affiliate vs internal notes
  let affiliateCount = 0;
  let internalCount = 0;
  for (const notes of Object.values(notesMap)) {
    for (const note of notes) {
      if (note.noteType === 'affiliate_notes') affiliateCount++;
      else if (note.noteType === 'internal_notes') internalCount++;
    }
  }
  console.log(`✓ Fetched notes: ${affiliateCount} affiliate, ${internalCount} internal`);

  if (options.dryRun) {
    return {
      leadsUpdated: leads.length,
      notesInserted: buildLeadNotesToInsert(leads, notesMap).length,
    };
  }

  const allLeadIds: string[] = [];
  const allNotesToInsert = buildLeadNotesToInsert(leads, notesMap);
  const rowsToAppend: RowRecord[] = [];

  for (const lead of leads) {
    allLeadIds.push(lead.id);
  }

  try {
    await pgClient.query('BEGIN');
    const deleteResult = await pgClient.query(
      `DELETE FROM care_recipient_leads_notes
       WHERE "leadId" = ANY($1) AND "mldmMigratedModmonAt" IS NOT NULL`,
      [allLeadIds]
    );
    await bulkInsertLeadNotes(pgClient, allNotesToInsert);
    await pgClient.query('COMMIT');

    // Build rows for logging (one per lead)
    const deletedCount = deleteResult.rowCount || 0;
    const deletedPerLead = Math.floor(deletedCount / leads.length);

    for (const lead of leads) {
      const notesCount = buildConcatenatedLeadNotePayload(notesMap[lead.careRecipientId] || []) ? 1 : 0;

      rowsToAppend.push({
        batch_id: batchId,
        source_id: lead.id,
        care_recipient_id: lead.careRecipientId,
        legacy_id: lead.legacyId,
        status: 'success',
        notes_inserted: notesCount,
        notes_deleted: deletedPerLead,
        processed_at: formatDate(new Date()),
      } as RowRecord);
    }

    // Write all rows in one operation
    if (rowsToAppend.length > 0) {
      await appendRows(MIGRATION_NAME, rowsToAppend);
    }

    return { leadsUpdated: allLeadIds.length, notesInserted: allNotesToInsert.length };
  } catch (error) {
    await pgClient.query('ROLLBACK');
    throw error;
  }
}

// Run migration if called directly
if (require.main === module) {
  migrateCareRecipientLeadNotes().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}