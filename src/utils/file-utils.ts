import { promises as fs } from 'fs';
import { join } from 'path';

const MIGRATION_STATE_DIR = 'migration-state';

export interface BatchRecord {
  batch_id: string;
  query: {
    from: string;
    to: string | null;
    last_created_at: string | null;
    last_id: string | null;
    limit: number;
  };
  fetched_count: number;
  started_at: string;
  completed_at: string | null;
  status: 'in_progress' | 'completed' | 'failed';
}

export interface RowRecord {
  batch_id: string;
  source_id: string;
  status: 'success' | 'skipped_no_care_recipient' | 'skipped_no_agent' | 'duplicate_legacy_id' | 'failed';
  legacy_id?: string;
  care_recipient_id?: string;
  dir_care_recipient_id?: string;
  reason?: string;
  error?: string;
  notes_inserted?: number;
  notes_deleted?: number;
  processed_at: string;
}

export interface MigrationSummary {
  status: 'not_started' | 'in_progress' | 'completed' | 'failed';
  time_range: {
    from: string;
    to: string | null;
    to_relative?: string;
  };
  batch_size: number;
  total_batches: number;
  completed_batches: number;
  total_rows_fetched: number;
  total_success: number;
  total_skipped: number;
  total_duplicate: number;
  total_failed: number;
  started_at: string | null;
  last_updated_at: string | null;
}

/**
 * Ensure migration state directory exists
 */
export async function ensureMigrationDir(migrationName: string): Promise<void> {
  const dir = join(MIGRATION_STATE_DIR, migrationName);
  await fs.mkdir(dir, { recursive: true });
}

/**
 * Append a batch record to batches.jsonl
 */
export async function appendBatch(migrationName: string, batch: BatchRecord): Promise<void> {
  const filePath = join(MIGRATION_STATE_DIR, migrationName, 'batches.jsonl');
  await fs.appendFile(filePath, JSON.stringify(batch) + '\n');
}

/**
 * Append a row record to rows.jsonl
 */
export async function appendRow(migrationName: string, row: RowRecord): Promise<void> {
  const filePath = join(MIGRATION_STATE_DIR, migrationName, 'rows.jsonl');
  await fs.appendFile(filePath, JSON.stringify(row) + '\n');
}

/**
 * Append multiple row records to rows.jsonl in one file operation
 */
export async function appendRows(migrationName: string, rows: RowRecord[]): Promise<void> {
  if (rows.length === 0) return;

  const filePath = join(MIGRATION_STATE_DIR, migrationName, 'rows.jsonl');
  const content = rows.map(row => JSON.stringify(row)).join('\n') + '\n';
  await fs.appendFile(filePath, content);
}

/**
 * Read all batch records
 */
export async function readBatches(migrationName: string): Promise<BatchRecord[]> {
  const filePath = join(MIGRATION_STATE_DIR, migrationName, 'batches.jsonl');
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return content
      .trim()
      .split('\n')
      .filter(line => line.trim())
      .map(line => JSON.parse(line));
  } catch (error: any) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

/**
 * Read all row records
 */
export async function readRows(migrationName: string): Promise<RowRecord[]> {
  const filePath = join(MIGRATION_STATE_DIR, migrationName, 'rows.jsonl');
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return content
      .trim()
      .split('\n')
      .filter(line => line.trim())
      .map(line => JSON.parse(line));
  } catch (error: any) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

/**
 * Read summary
 */
export async function readSummary(): Promise<Record<string, MigrationSummary>> {
  const filePath = join(MIGRATION_STATE_DIR, 'summary.json');
  try {
    const content = await fs.readFile(filePath, 'utf-8');
    return JSON.parse(content);
  } catch (error: any) {
    if (error.code === 'ENOENT') {
      return {};
    }
    throw error;
  }
}

/**
 * Write summary (atomic write)
 */
export async function writeSummary(summary: Record<string, MigrationSummary>): Promise<void> {
  const filePath = join(MIGRATION_STATE_DIR, 'summary.json');
  const tempPath = filePath + '.tmp';
  
  await fs.mkdir(MIGRATION_STATE_DIR, { recursive: true });
  await fs.writeFile(tempPath, JSON.stringify(summary, null, 2));
  await fs.rename(tempPath, filePath);
}

