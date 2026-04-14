/**
 * Common database utilities for migration scripts
 */

/**
 * Map DIR care_recipient IDs to MM care_recipient UUIDs
 */
export async function mapCareRecipients(
  pgClient: any,
  dirIds: string[]
): Promise<Map<string, string>> {
  if (dirIds.length === 0) return new Map();

  const query = `SELECT id, "legacyId" FROM care_recipients WHERE "legacyId" = ANY($1)`;
  const result = await pgClient.query(query, [dirIds]);

  const map = new Map<string, string>();
  result.rows.forEach((row: any) => {
    map.set(row.legacyId, row.id);
  });
  return map;
}

/**
 * Map DIR account IDs to MM agent UUIDs
 */
export async function mapAgents(
  pgClient: any,
  dirIds: string[]
): Promise<Map<string, string>> {
  if (dirIds.length === 0) return new Map();

  const query = `SELECT id, "accountId" FROM twilio_agents WHERE "accountId" = ANY($1)`;
  const result = await pgClient.query(query, [dirIds]);

  const map = new Map<string, string>();
  result.rows.forEach((row: any) => {
    map.set(row.accountId, row.id);
  });
  return map;
}

/**
 * Check which notes already exist in MM by legacyId and source
 */
export async function checkExistingNotes(
  pgClient: any,
  legacyIds: string[],
  source: string
): Promise<Set<string>> {
  if (legacyIds.length === 0) return new Set();

  const query = `SELECT "legacyId" FROM care_recipient_notes WHERE "legacyId" = ANY($1) AND source = $2`;
  const result = await pgClient.query(query, [legacyIds, source]);

  return new Set(result.rows.map((row: any) => row.legacyId));
}

/**
 * Insert a note into MM care_recipient_notes table
 */
export interface NoteInsert {
  legacyId: string;
  careRecipientId: string;
  value: string;
  agentAccountId: string | null;
  agentName: string;
  source: string;
  createdAt: Date;
  updatedAt: Date;
}

export async function insertNote(pgClient: any, note: NoteInsert): Promise<void> {
  const query = `
    INSERT INTO care_recipient_notes
      (id, "legacyId", "careRecipientId", value, "agentAccountId", "agentName", source, "createdAt", "updatedAt", "mldmMigratedModmonAt")
    VALUES
      (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, NOW())
    ON CONFLICT ("legacyId", source) DO NOTHING
  `;

  await pgClient.query(query, [
    note.legacyId,
    note.careRecipientId,
    note.value,
    note.agentAccountId,
    note.agentName,
    note.source,
    note.createdAt,
    note.updatedAt,
  ]);
}

