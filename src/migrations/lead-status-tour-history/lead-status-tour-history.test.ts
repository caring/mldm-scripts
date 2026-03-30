import { describe, it, expect, beforeEach } from 'vitest';
import {
  createLeadStatusTourHistoryTestDatabase,
  seedLeadStatusTourHistoryTestData,
  getTestClient,
} from '../../test/setup-lead-status-tour-history-pg-mem';
import {
  formatStatusDate,
  formatStatusText,
  formatStatusLine,
  buildLeadStatusSummary,
  DirLeadStatus,
} from './lead-status-tour-history';

function makeStatus(overrides: Partial<DirLeadStatus> = {}): DirLeadStatus {
  return {
    lead_id: 1001,
    created_at: new Date('2025-09-01T10:18:00Z'),
    status: 'valid_lead',
    sub_status: null,
    tour_date: null,
    tour_time: null,
    created_by: 'Aidan Moloney',
    ...overrides,
  };
}

describe('formatStatusDate', () => {
  it('formats a date without leading zero on month/day', () => {
    const date = new Date(2025, 8, 1, 10, 18, 0);
    const result = formatStatusDate(date);
    expect(result).toBe('9/1/25 10:18am');
  });

  it('formats a date with double-digit month and day', () => {
    const date = new Date(2025, 8, 17, 6, 46, 0);
    const result = formatStatusDate(date);
    expect(result).toBe('9/17/25 06:46am');
  });

  it('formats pm time correctly', () => {
    const date = new Date(2025, 7, 15, 14, 30, 0);
    const result = formatStatusDate(date);
    expect(result).toBe('8/15/25 02:30pm');
  });

  it('formats midnight as 12:00am', () => {
    const date = new Date(2025, 0, 5, 0, 0, 0);
    const result = formatStatusDate(date);
    expect(result).toBe('1/5/25 12:00am');
  });

  it('formats noon as 12:00pm', () => {
    const date = new Date(2025, 0, 5, 12, 0, 0);
    const result = formatStatusDate(date);
    expect(result).toBe('1/5/25 12:00pm');
  });
});

describe('formatStatusText', () => {
  it('formats tour_scheduled with sub_status', () => {
    const status = makeStatus({
      status: 'tour_scheduled',
      sub_status: 'in person',
      tour_date: '2025-09-10',
    });
    expect(formatStatusText(status)).toBe('Tour scheduled, in person');
  });

  it('formats tour_scheduled with no sub_status as "in person"', () => {
    const status = makeStatus({
      status: 'tour_scheduled',
      sub_status: null,
      tour_date: '2025-09-10',
    });
    expect(formatStatusText(status)).toBe('Tour scheduled, in person');
  });

  it('does not treat tour_scheduled as scheduled when tour_date is null', () => {
    const status = makeStatus({ status: 'tour_scheduled', sub_status: null, tour_date: null });
    expect(formatStatusText(status)).toBe('Status set as tour_scheduled');
  });

  it('formats tour_completed', () => {
    expect(formatStatusText(makeStatus({ status: 'tour_completed' }))).toBe('Tour completed');
  });

  it('formats tour_cancelled with sub_status', () => {
    const status = makeStatus({ status: 'tour_cancelled', sub_status: 'virtual' });
    expect(formatStatusText(status)).toBe('Tour cancelled, virtual');
  });

  it('formats tour_cancelled with no sub_status as "in person"', () => {
    const status = makeStatus({ status: 'tour_cancelled', sub_status: null });
    expect(formatStatusText(status)).toBe('Tour cancelled, in person');
  });

  it('formats memo/tour_canceled as tour cancelled', () => {
    const status = makeStatus({ status: 'memo', sub_status: 'tour_canceled' });
    expect(formatStatusText(status)).toBe('Tour cancelled, tour_canceled');
  });

  it('formats valid_lead as "Valid"', () => {
    expect(formatStatusText(makeStatus({ status: 'valid_lead' }))).toBe('Valid');
  });

  it('formats unknown status as "Status set as X"', () => {
    expect(formatStatusText(makeStatus({ status: 'contacted', sub_status: null }))).toBe(
      'Status set as contacted'
    );
  });

  it('formats unknown status with sub_status as "Status set as X, Y"', () => {
    expect(
      formatStatusText(makeStatus({ status: 'contacted', sub_status: 'left_voicemail' }))
    ).toBe('Status set as contacted, left_voicemail');
  });
});

describe('formatStatusLine', () => {
  it('formats a complete status line', () => {
    const status = makeStatus({
      created_at: new Date(2025, 8, 1, 10, 18, 0),
      status: 'tour_scheduled',
      sub_status: 'in person',
      tour_date: '2025-09-10',
      created_by: 'Aidan Moloney',
    });
    const line = formatStatusLine(status);
    expect(line).toBe('9/1/25 10:18am - Tour scheduled, in person - Aidan Moloney');
  });

  it('uses "Unknown" when created_by is null', () => {
    const status = makeStatus({ created_by: null });
    expect(formatStatusLine(status)).toContain('- Unknown');
  });
});

describe('buildLeadStatusSummary', () => {
  it('returns latest statuses first (sorted DESC)', () => {
    const statuses: DirLeadStatus[] = [
      makeStatus({ created_at: new Date(2025, 7, 1, 9, 0, 0), status: 'contacted', created_by: 'Old' }),
      makeStatus({ created_at: new Date(2025, 8, 1, 10, 0, 0), status: 'valid_lead', created_by: 'New' }),
    ];
    const summary = buildLeadStatusSummary(statuses);
    const lines = summary.split('\n');
    expect(lines[0]).toContain('Valid');
    expect(lines[0]).toContain('New');
    expect(lines[1]).toContain('Status set as contacted');
  });

  it('caps at 10 statuses regardless of input length', () => {
    const statuses: DirLeadStatus[] = Array.from({ length: 15 }, (_, i) =>
      makeStatus({
        created_at: new Date(2025, 0, i + 1, 10, 0, 0),
        status: 'contacted',
        created_by: `Agent ${i}`,
      })
    );
    const summary = buildLeadStatusSummary(statuses);
    expect(summary.split('\n')).toHaveLength(10);
  });

  it('truncates to 1000 characters on a clean line boundary', () => {
    const statuses: DirLeadStatus[] = Array.from({ length: 10 }, (_, i) =>
      makeStatus({
        created_at: new Date(2025, 0, i + 1, 10, 0, 0),
        status: 'contacted',
        sub_status: 'left_voicemail_with_callback_request_followup',
        created_by: 'A'.repeat(80),
      })
    );
    const summary = buildLeadStatusSummary(statuses);
    expect(summary.length).toBeLessThanOrEqual(1000);
    const lines = summary.split('\n');
    lines.forEach(line => expect(line.length).toBeGreaterThan(0));
  });

  it('handles a single status', () => {
    const statuses = [
      makeStatus({ status: 'valid_lead', created_by: 'Aidan Moloney' }),
    ];
    const summary = buildLeadStatusSummary(statuses);
    expect(summary).toContain('Valid');
    expect(summary).toContain('Aidan Moloney');
    expect(summary.split('\n')).toHaveLength(1);
  });

  it('returns empty string for empty statuses array', () => {
    expect(buildLeadStatusSummary([])).toBe('');
  });
});

describe('care_recipient_leads lookup', () => {
  let db: any;
  let client: any;

  beforeEach(async () => {
    db = createLeadStatusTourHistoryTestDatabase();
    await seedLeadStatusTourHistoryTestData(db);
    client = await getTestClient(db);
  });

  it('finds a lead by legacyId', async () => {
    const result = await client.query(
      `SELECT id, "legacyId", "mldmMigratedAt"
       FROM care_recipient_leads
       WHERE "legacyId" = $1 AND "deletedAt" IS NULL`,
      ['1001']
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].id).toBe('00000000-0000-0000-0000-000000000001');
    expect(result.rows[0].mldmMigratedAt).toBeNull();
  });

  it('returns empty for a legacyId not in MM', async () => {
    const result = await client.query(
      `SELECT id FROM care_recipient_leads WHERE "legacyId" = $1 AND "deletedAt" IS NULL`,
      ['9999']
    );
    expect(result.rows).toHaveLength(0);
  });

  it('fetches multiple leads by legacyId', async () => {
    const result = await client.query(
      `SELECT "legacyId" FROM care_recipient_leads
       WHERE "legacyId" IN ($1, $2, $3) AND "deletedAt" IS NULL`,
      ['1001', '1002', '1003']
    );
    expect(result.rows).toHaveLength(3);
  });

  it('detects an already-migrated lead via mldmMigratedAt', async () => {
    const result = await client.query(
      `SELECT "mldmMigratedAt" FROM care_recipient_leads WHERE "legacyId" = $1`,
      ['1002']
    );
    expect(result.rows[0].mldmMigratedAt).not.toBeNull();
  });
});

describe('bulk update care_recipient_leads', () => {
  let db: any;
  let client: any;

  beforeEach(async () => {
    db = createLeadStatusTourHistoryTestDatabase();
    await seedLeadStatusTourHistoryTestData(db);
    client = await getTestClient(db);
  });

  it('updates legacyLeadStatusAndTourHistory and mldmMigratedAt', async () => {
    const summary = '9/1/25 10:18am - Tour scheduled, in person - Aidan Moloney';

    await client.query(
      `UPDATE care_recipient_leads
       SET
         "legacyLeadStatusAndTourHistory" = $1,
         "mldmMigratedAt" = NOW(),
         "updatedAt" = NOW()
       WHERE "legacyId" = $2 AND "deletedAt" IS NULL`,
      [summary, '1001']
    );

    const result = await client.query(
      `SELECT "legacyLeadStatusAndTourHistory", "mldmMigratedAt"
       FROM care_recipient_leads WHERE "legacyId" = $1`,
      ['1001']
    );

    expect(result.rows[0].legacyLeadStatusAndTourHistory).toBe(summary);
    expect(result.rows[0].mldmMigratedAt).not.toBeNull();
  });

  it('bulk updates multiple leads independently', async () => {
    const updates = [
      { legacyId: '1001', summary: '9/1/25 10:00am - Valid - Agent A' },
      { legacyId: '1003', summary: '9/2/25 11:00am - Tour completed - Agent B' },
    ];

    for (const u of updates) {
      await client.query(
        `UPDATE care_recipient_leads
         SET
           "legacyLeadStatusAndTourHistory" = $1,
           "mldmMigratedAt" = NOW(),
           "updatedAt" = NOW()
         WHERE "legacyId" = $2 AND "deletedAt" IS NULL`,
        [u.summary, u.legacyId]
      );
    }

    const result = await client.query(
      `SELECT "legacyId", "legacyLeadStatusAndTourHistory"
       FROM care_recipient_leads
       WHERE "legacyId" IN ($1, $2)
       ORDER BY "legacyId"`,
      ['1001', '1003']
    );

    expect(result.rows[0].legacyLeadStatusAndTourHistory).toBe(updates[0].summary);
    expect(result.rows[1].legacyLeadStatusAndTourHistory).toBe(updates[1].summary);
  });

  it('does not update a deleted lead', async () => {
    await client.query(
      `UPDATE care_recipient_leads SET "deletedAt" = NOW() WHERE "legacyId" = '1001'`
    );

    await client.query(
      `UPDATE care_recipient_leads
       SET "legacyLeadStatusAndTourHistory" = $1, "mldmMigratedAt" = NOW(), "updatedAt" = NOW()
       WHERE "legacyId" = $2 AND "deletedAt" IS NULL`,
      ['should not appear', '1001']
    );

    const result = await client.query(
      `SELECT "legacyLeadStatusAndTourHistory" FROM care_recipient_leads WHERE "legacyId" = '1001'`
    );

    expect(result.rows[0].legacyLeadStatusAndTourHistory).toBeNull();
  });
});
