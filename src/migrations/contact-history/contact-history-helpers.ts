import { parseISO } from 'date-fns';

export interface HistoryEvent {
  type: 'call' | 'text' | 'inquiry' | 'contact_merge' | 'formal_affirmation' | 'lead_send';
  timestamp: Date;
  description: string;
  sourceId: number;
  sourceTable: string;
  careRecipientId: number;
}

export interface ContactHistorySummary {
  summary: string;
  lastContactedAt: Date | null;
  lastDealSentAt: Date | null;
}

export interface MMCareRecipientData {
  id: string;
  mldmMigratedModmonAt: Date | null;
}

export type ContactHistoryIdMigrationState =
  | 'done'
  | 'not_done'
  | 'needs_refresh'
  | 'not_in_mm'
  | 'no_events';

export interface ContactHistoryIdMigrationDecision {
  state: ContactHistoryIdMigrationState;
  summaryEvents: HistoryEvent[];
}

export type ContactHistoryIdBatchState = ContactHistoryIdMigrationState | 'not_found';

export interface ContactHistoryIdClassificationRow {
  inputId: number;
  dirCareRecipientId: number | null;
  mmCareRecipientId: string | null;
  state: ContactHistoryIdBatchState;
  action: 'skip' | 'populate' | 'refresh';
  reason: string;
  mldmMigratedModmonAt: string | null;
  eventsConsidered: number;
}

export interface ContactHistoryIdBatchClassificationReport {
  batchId: string;
  generatedAt: string;
  requestedInputCount: number;
  resolvedCareRecipientCount: number;
  actionableCount: number;
  counts: Record<ContactHistoryIdBatchState, number>;
  done: ContactHistoryIdClassificationRow[];
  not_done: ContactHistoryIdClassificationRow[];
  needs_refresh: ContactHistoryIdClassificationRow[];
  not_in_mm: ContactHistoryIdClassificationRow[];
  no_events: ContactHistoryIdClassificationRow[];
  not_found: ContactHistoryIdClassificationRow[];
  rows: ContactHistoryIdClassificationRow[];
}

export function chunkIds(ids: number[], chunkSize: number): number[][] {
  if (chunkSize <= 0) {
    throw new Error(`chunkSize must be > 0. Received: ${chunkSize}`);
  }

  const chunks: number[][] = [];
  for (let index = 0; index < ids.length; index += chunkSize) {
    chunks.push(ids.slice(index, index + chunkSize));
  }
  return chunks;
}

export function getIdMigrationAction(
  state: ContactHistoryIdBatchState
): ContactHistoryIdClassificationRow['action'] {
  if (state === 'not_done') {
    return 'populate';
  }

  if (state === 'needs_refresh') {
    return 'refresh';
  }

  return 'skip';
}

export function buildIdBatchClassificationReport(
  batchId: string,
  rows: ContactHistoryIdClassificationRow[],
  resolvedCareRecipientCount: number,
  generatedAt: Date = new Date()
): ContactHistoryIdBatchClassificationReport {
  const counts: Record<ContactHistoryIdBatchState, number> = {
    done: 0,
    not_done: 0,
    needs_refresh: 0,
    not_in_mm: 0,
    no_events: 0,
    not_found: 0,
  };

  for (const row of rows) {
    counts[row.state] += 1;
  }

  return {
    batchId,
    generatedAt: generatedAt.toISOString(),
    requestedInputCount: rows.length,
    resolvedCareRecipientCount,
    actionableCount: rows.filter((row) => row.action !== 'skip').length,
    counts,
    done: rows.filter((row) => row.state === 'done'),
    not_done: rows.filter((row) => row.state === 'not_done'),
    needs_refresh: rows.filter((row) => row.state === 'needs_refresh'),
    not_in_mm: rows.filter((row) => row.state === 'not_in_mm'),
    no_events: rows.filter((row) => row.state === 'no_events'),
    not_found: rows.filter((row) => row.state === 'not_found'),
    rows,
  };
}

export function buildIdBatchClassificationFilename(
  generatedAt: Date | string
): string {
  const isoTimestamp = typeof generatedAt === 'string'
    ? parseISO(generatedAt).toISOString()
    : generatedAt.toISOString();

  const timestamp = isoTimestamp
    .replace(/[-:]/g, '')
    .replace(/\.(\d{3})Z$/, '$1');

  return `classification-${timestamp}.json`;
}

export function hasNewEventsSinceMigration(
  events: HistoryEvent[],
  migratedAt: Date | null
): boolean {
  if (!migratedAt) {
    return false;
  }

  return events.some((event) => event.timestamp > migratedAt);
}

export function classifyIdBasedCareRecipient(
  mmInfo: MMCareRecipientData | undefined,
  summaryEvents: HistoryEvent[]
): ContactHistoryIdMigrationDecision {
  if (!mmInfo) {
    return { state: 'not_in_mm', summaryEvents: [] };
  }

  if (!mmInfo.mldmMigratedModmonAt) {
    if (summaryEvents.length === 0) {
      return { state: 'no_events', summaryEvents: [] };
    }

    return { state: 'not_done', summaryEvents };
  }

  if (hasNewEventsSinceMigration(summaryEvents, mmInfo.mldmMigratedModmonAt)) {
    return { state: 'needs_refresh', summaryEvents };
  }

  return { state: 'done', summaryEvents: [] };
}

export function buildContactHistorySummary(events: HistoryEvent[]): ContactHistorySummary {
  const lastContactedAt = events.length > 0 ? events[0].timestamp : null;
  const lastDealSentAt = events.find((event) => event.type === 'lead_send')?.timestamp || null;

  const maxLength = 1000;
  const truncatedSuffix = '... (truncated)';
  let summary = '';

  for (const event of events) {
    const dateLabel = formatEventDate(event.timestamp);
    const line = `[${event.type.toUpperCase()}] ${event.description} - ${dateLabel}\n`;

    if (summary.length + line.length > maxLength - truncatedSuffix.length) {
      if (summary.length + truncatedSuffix.length <= maxLength) {
        summary += truncatedSuffix;
      }
      break;
    }

    summary += line;
  }

  if (summary.length > maxLength) {
    summary = summary.substring(0, maxLength - truncatedSuffix.length) + truncatedSuffix;
  }

  return {
    summary: summary.trim().substring(0, maxLength),
    lastContactedAt,
    lastDealSentAt,
  };
}

function formatEventDate(date: Date): string {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const month = months[date.getMonth()];
  const day = date.getDate();
  const year = date.getFullYear();
  return `${month} ${day}, ${year}`;
}