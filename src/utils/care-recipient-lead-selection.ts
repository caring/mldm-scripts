import { promises as fs } from 'fs';
import { MigrationCLIOptions, parseIds } from './migration-cli';

export interface CareRecipientLeadRef {
  careRecipientLeadId: string;
  legacyId: number;
}

export interface ExplicitLeadResolution {
  requestedCount: number;
  matchedLeads: CareRecipientLeadRef[];
}

interface CsvLeadIds {
  legacyIds: number[];
  crlIds: string[];
}

/**
 * Resolve explicit lead selection from --ids-inline / --ids file and verify in MM.
 * Returns null when no explicit selection options were provided.
 */
export async function resolveExplicitCareRecipientLeads(
  options: MigrationCLIOptions,
  pgClient: any
): Promise<ExplicitLeadResolution | null> {
  if (!options.idsInline && !options.idsFile) {
    return null;
  }

  if (options.idsInline) {
    const parsedInlineIds = await parseIds(options);
    if (!parsedInlineIds || parsedInlineIds.length === 0) {
      throw new Error('No valid IDs found in --ids-inline parameter');
    }

    const legacyIds = dedupeNumericIds(parsedInlineIds);
    const matchedLeads = await fetchMMLeadsByLegacyIds(pgClient, legacyIds);
    return {
      requestedCount: legacyIds.length,
      matchedLeads,
    };
  }

  const fileContent = await fs.readFile(options.idsFile!, 'utf-8');
  const csvIds = parseLeadIdsFromCsv(fileContent);
  const looksLikeCsvInput = fileContent.includes(',') || looksLikeHeaderRow(fileContent);

  if (looksLikeCsvInput) {
    if (csvIds.legacyIds.length > 0) {
      const legacyIds = dedupeNumericIds(csvIds.legacyIds);
      const matchedLeads = await fetchMMLeadsByLegacyIds(pgClient, legacyIds);
      return {
        requestedCount: legacyIds.length,
        matchedLeads,
      };
    }

    if (csvIds.crlIds.length > 0) {
      const crlIds = dedupeStringIds(csvIds.crlIds);
      const matchedLeads = await fetchMMLeadsByCRLIds(pgClient, crlIds);
      return {
        requestedCount: crlIds.length,
        matchedLeads,
      };
    }
  }

  const parsedFileIds = await parseIds({
    ...options,
    idsInline: null,
  });

  if (parsedFileIds && parsedFileIds.length > 0) {
    const legacyIds = dedupeNumericIds(parsedFileIds);
    const matchedLeads = await fetchMMLeadsByLegacyIds(pgClient, legacyIds);
    return {
      requestedCount: legacyIds.length,
      matchedLeads,
    };
  }

  throw new Error(`Could not parse lead IDs from file: ${options.idsFile}`);
}

/**
 * Fetch MM care_recipient_leads in a date range (only rows with usable legacyId).
 */
export async function fetchCareRecipientLeadsByDateRange(
  pgClient: any,
  fromDate: Date,
  toDate: Date | null,
  batchSize: number,
  offset: number
): Promise<CareRecipientLeadRef[]> {
  const whereParts = ['"deletedAt" IS NULL', '"legacyId" IS NOT NULL', '"createdAt" <= $1'];
  const params: any[] = [fromDate];

  if (toDate) {
    whereParts.push(`"createdAt" >= $2`);
    params.push(toDate);
  }

  params.push(batchSize, offset);
  const limitParam = toDate ? '$3' : '$2';
  const offsetParam = toDate ? '$4' : '$3';

  const result = await pgClient.query(
    `
    SELECT id, "legacyId"
    FROM care_recipient_leads
    WHERE ${whereParts.join(' AND ')}
    ORDER BY "createdAt" DESC
    LIMIT ${limitParam} OFFSET ${offsetParam}
    `,
    params
  );

  return normalizeMMLeadRows(result.rows);
}

/**
 * Parse CSV content that may contain legacyId or care_recipient_leads id columns.
 */
export function parseLeadIdsFromCsv(content: string): CsvLeadIds {
  const rows = parseCsvRows(content);
  if (rows.length === 0) {
    return { legacyIds: [], crlIds: [] };
  }

  const headers = rows[0].map(h => h.trim().toLowerCase());
  const headerIdx = {
    legacyId: headers.findIndex(h => ['legacyid', 'legacy_id', 'local_resource_lead_id'].includes(h)),
    id: headers.findIndex(h => ['id', 'care_recipient_lead_id', 'care_recipient_leads_id'].includes(h)),
  };

  const dataRows = rows.slice(1);
  const legacyIds: number[] = [];
  const crlIds: string[] = [];

  if (headerIdx.legacyId >= 0 || headerIdx.id >= 0) {
    for (const row of dataRows) {
      if (headerIdx.legacyId >= 0) {
        const value = (row[headerIdx.legacyId] || '').trim();
        const id = parseInt(value, 10);
        if (!isNaN(id)) legacyIds.push(id);
      } else if (headerIdx.id >= 0) {
        const id = (row[headerIdx.id] || '').trim();
        if (id) crlIds.push(id);
      }
    }

    return { legacyIds, crlIds };
  }

  for (const row of rows) {
    const first = (row[0] || '').trim();
    if (!first) continue;
    const numeric = parseInt(first, 10);
    if (!isNaN(numeric) && String(numeric) === first) {
      legacyIds.push(numeric);
    } else {
      crlIds.push(first);
    }
  }

  return { legacyIds, crlIds };
}

async function fetchMMLeadsByLegacyIds(pgClient: any, legacyIds: number[]): Promise<CareRecipientLeadRef[]> {
  if (legacyIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT id, "legacyId"
    FROM care_recipient_leads
    WHERE "legacyId" = ANY($1)
      AND "deletedAt" IS NULL
    `,
    [legacyIds.map((id) => id.toString())]
  );

  return normalizeMMLeadRows(result.rows);
}

async function fetchMMLeadsByCRLIds(pgClient: any, crlIds: string[]): Promise<CareRecipientLeadRef[]> {
  if (crlIds.length === 0) return [];

  const result = await pgClient.query(
    `
    SELECT id, "legacyId"
    FROM care_recipient_leads
    WHERE id = ANY($1::uuid[])
      AND "legacyId" IS NOT NULL
      AND "deletedAt" IS NULL
    `,
    [crlIds]
  );

  return normalizeMMLeadRows(result.rows);
}

function normalizeMMLeadRows(rows: any[]): CareRecipientLeadRef[] {
  const normalized = rows
    .map((row) => {
      const legacyId = parseInt(row.legacyId, 10);
      if (isNaN(legacyId) || legacyId <= 0) {
        return null;
      }
      return {
        careRecipientLeadId: row.id,
        legacyId,
      };
    })
    .filter((row): row is CareRecipientLeadRef => row !== null);

  return dedupeLeadRefs(normalized);
}

function dedupeLeadRefs(leads: CareRecipientLeadRef[]): CareRecipientLeadRef[] {
  const seen = new Set<string>();
  const result: CareRecipientLeadRef[] = [];

  for (const lead of leads) {
    const key = `${lead.careRecipientLeadId}:${lead.legacyId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(lead);
  }

  return result;
}

function dedupeNumericIds(ids: number[]): number[] {
  return [...new Set(ids.filter(id => Number.isInteger(id) && id > 0))];
}

function dedupeStringIds(ids: string[]): string[] {
  return [...new Set(ids.map(id => id.trim()).filter(Boolean))];
}

function parseCsvRows(content: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    const next = content[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && ch === ',') {
      row.push(field);
      field = '';
      continue;
    }

    if (!inQuotes && (ch === '\n' || ch === '\r')) {
      if (ch === '\r' && next === '\n') i++;
      row.push(field);
      if (row.some(v => v.trim() !== '')) {
        rows.push(row);
      }
      row = [];
      field = '';
      continue;
    }

    field += ch;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    if (row.some(v => v.trim() !== '')) {
      rows.push(row);
    }
  }

  return rows;
}

function looksLikeHeaderRow(content: string): boolean {
  const firstLine = content
    .split(/\r?\n/)
    .map(line => line.trim())
    .find(line => line.length > 0);

  if (!firstLine) return false;

  const normalized = firstLine.toLowerCase();
  return [
    'id',
    'legacyid',
    'legacy_id',
    'local_resource_lead_id',
    'care_recipient_lead_id',
    'care_recipient_leads_id',
  ].includes(normalized);
}
