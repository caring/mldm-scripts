import { describe, it, expect, beforeEach } from 'vitest';
import { parseISO } from 'date-fns';
import { createContactHistoryTestDatabase, seedContactHistoryTestData, getTestClient } from '../../test/setup-contact-history-pg-mem';
import {
  buildContactHistorySummary,
  buildIdBatchClassificationFilename,
  buildIdBatchClassificationReport,
  chunkIds,
  classifyIdBasedCareRecipient,
  getIdMigrationAction,
} from './contact-history-helpers';

function makeHistoryEvent(overrides: Partial<{
  type: 'call' | 'text' | 'inquiry' | 'contact_merge' | 'formal_affirmation' | 'lead_send';
  timestamp: Date;
  description: string;
  sourceId: number;
  sourceTable: string;
  careRecipientId: number;
}> = {}) {
  return {
    type: overrides.type ?? 'call',
    timestamp: overrides.timestamp ?? parseISO('2024-03-20T10:30:00Z'),
    description: overrides.description ?? 'Outbound call - Qualified - 5m 23s - John Smith',
    sourceId: overrides.sourceId ?? 1,
    sourceTable: overrides.sourceTable ?? 'call_center_calls',
    careRecipientId: overrides.careRecipientId ?? 9987168,
  };
}

describe('Contact History Migration', () => {
  let db: any;
  let client: any;

  beforeEach(async () => {
    // Create fresh database for each test
    db = createContactHistoryTestDatabase();
    await seedContactHistoryTestData(db);
    client = await getTestClient(db);
  });

  describe('ID-based classification', () => {
    it('chunks input ids into batches of 1000', () => {
      const ids = Array.from({ length: 2005 }, (_, index) => index + 1);

      const chunks = chunkIds(ids, 1000);

      expect(chunks).toHaveLength(3);
      expect(chunks[0]).toHaveLength(1000);
      expect(chunks[1]).toHaveLength(1000);
      expect(chunks[2]).toHaveLength(5);
      expect(chunks[0][0]).toBe(1);
      expect(chunks[2][4]).toBe(2005);
    });

    it('classifies never-migrated care recipients with events as not_done', () => {
      const decision = classifyIdBasedCareRecipient(
        { id: '00000000-0000-0000-0000-000000000001', mldmMigratedModmonAt: null },
        [makeHistoryEvent()]
      );

      expect(decision.state).toBe('not_done');
      expect(decision.summaryEvents).toHaveLength(1);
    });

    it('classifies migrated care recipients with no newer events as done', () => {
      const decision = classifyIdBasedCareRecipient(
        {
          id: '00000000-0000-0000-0000-000000000002',
          mldmMigratedModmonAt: parseISO('2024-03-10T00:00:00Z'),
        },
        [
          makeHistoryEvent({
            timestamp: parseISO('2024-03-01T10:30:00Z'),
          }),
        ]
      );

      expect(decision.state).toBe('done');
      expect(decision.summaryEvents).toHaveLength(0);
    });

    it('classifies migrated care recipients with newer events as needs_refresh and keeps full summary events', () => {
      const decision = classifyIdBasedCareRecipient(
        {
          id: '00000000-0000-0000-0000-000000000002',
          mldmMigratedModmonAt: parseISO('2024-03-10T00:00:00Z'),
        },
        [
          makeHistoryEvent({
            type: 'call',
            timestamp: parseISO('2024-03-20T10:30:00Z'),
            description: 'Outbound call - Qualified - 5m 23s - John Smith',
          }),
          makeHistoryEvent({
            type: 'lead_send',
            timestamp: parseISO('2024-02-28T09:15:00Z'),
            description: 'BidderLead sent to Sunrise - Assisted Living - sent_to_provider',
            sourceId: 2,
            sourceTable: 'local_resource_leads',
          }),
        ]
      );

      expect(decision.state).toBe('needs_refresh');
      expect(decision.summaryEvents).toHaveLength(2);

      const summary = buildContactHistorySummary(decision.summaryEvents);
      expect(summary.summary).toContain('[CALL] Outbound call - Qualified - 5m 23s - John Smith - Mar 20, 2024');
      expect(summary.summary).toContain('[LEAD_SEND] BidderLead sent to Sunrise - Assisted Living - sent_to_provider - Feb 28, 2024');
      expect(summary.lastContactedAt).toEqual(parseISO('2024-03-20T10:30:00Z'));
      expect(summary.lastDealSentAt).toEqual(parseISO('2024-02-28T09:15:00Z'));
    });

    it('builds batch classification reports with grouped counts and actions', () => {
      const report = buildIdBatchClassificationReport(
        'batch_000001',
        [
          {
            inputId: 1001,
            dirCareRecipientId: 9987168,
            mmCareRecipientId: '00000000-0000-0000-0000-000000000001',
            state: 'not_done',
            action: getIdMigrationAction('not_done'),
            reason: 'Never migrated in MM',
            mldmMigratedModmonAt: null,
            eventsConsidered: 4,
          },
          {
            inputId: 1002,
            dirCareRecipientId: 9987169,
            mmCareRecipientId: '00000000-0000-0000-0000-000000000002',
            state: 'done',
            action: getIdMigrationAction('done'),
            reason: 'No new events since last migration',
            mldmMigratedModmonAt: '2024-03-10T00:00:00.000Z',
            eventsConsidered: 2,
          },
          {
            inputId: 1003,
            dirCareRecipientId: null,
            mmCareRecipientId: null,
            state: 'not_found',
            action: getIdMigrationAction('not_found'),
            reason: 'Input ID not found in DIR contacts',
            mldmMigratedModmonAt: null,
            eventsConsidered: 0,
          },
          {
            inputId: 1004,
            dirCareRecipientId: 9987170,
            mmCareRecipientId: '00000000-0000-0000-0000-000000000003',
            state: 'needs_refresh',
            action: getIdMigrationAction('needs_refresh'),
            reason: 'New events found after last migration',
            mldmMigratedModmonAt: '2024-03-10T00:00:00.000Z',
            eventsConsidered: 5,
          },
        ],
        3,
        parseISO('2024-04-01T12:00:00Z')
      );

      expect(report.batchId).toBe('batch_000001');
      expect(report.generatedAt).toBe('2024-04-01T12:00:00.000Z');
      expect(report.requestedInputCount).toBe(4);
      expect(report.resolvedCareRecipientCount).toBe(3);
      expect(report.actionableCount).toBe(2);
      expect(report.counts.done).toBe(1);
      expect(report.counts.not_done).toBe(1);
      expect(report.counts.needs_refresh).toBe(1);
      expect(report.counts.not_found).toBe(1);
      expect(report.done).toHaveLength(1);
      expect(report.not_done).toHaveLength(1);
      expect(report.needs_refresh).toHaveLength(1);
      expect(report.not_found).toHaveLength(1);
    });

    it('builds timestamped classification filenames', () => {
      const fileName = buildIdBatchClassificationFilename('2024-04-01T12:00:00.123Z');

      expect(fileName).toBe('classification-20240401T120000123.json');
    });
  });

  describe('Care Recipient Lookup', () => {
    it('should find MM care_recipient by legacyId', async () => {
      const dirCareRecipientId = '9987168';

      const query = `
        SELECT id, "mldmMigratedModmonAt"
        FROM care_recipients
        WHERE "legacyId" = $1
          AND "deletedAt" IS NULL
      `;
      const result = await client.query(query, [dirCareRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].id).toBe('00000000-0000-0000-0000-000000000001');
      expect(result.rows[0].mldmMigratedModmonAt).toBeNull();
    });

    it('should return empty for non-existent care_recipient', async () => {
      const dirCareRecipientId = '999999';

      const query = `
        SELECT id, "mldmMigratedModmonAt"
        FROM care_recipients
        WHERE "legacyId" = $1
          AND "deletedAt" IS NULL
      `;
      const result = await client.query(query, [dirCareRecipientId]);

      expect(result.rows).toHaveLength(0);
    });

    it('should skip care_recipient already migrated', async () => {
      const dirCareRecipientId = '9987169'; // This one is already migrated

      const query = `
        SELECT id, "mldmMigratedModmonAt"
        FROM care_recipients
        WHERE "legacyId" = $1
          AND "deletedAt" IS NULL
      `;
      const result = await client.query(query, [dirCareRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].mldmMigratedModmonAt).not.toBeNull();
    });
  });

  describe('Contact History Update', () => {
    it('should update care_recipient with contact history summary', async () => {
      const careRecipientId = '00000000-0000-0000-0000-000000000001';
      const summary = '[CALL] Outbound call - Qualified - 5m 23s - John Smith - Mar 20, 2024\n[TEXT] Sent text - delivered - "Hi!" - Mar 18, 2024';
      const lastContactedAt = new Date('2024-03-20T10:30:00Z');
      const lastDealSentAt = new Date('2024-03-15T09:15:00Z');

      const updateQuery = `
        UPDATE care_recipients
        SET
          "legacyContactHistorySummary" = $1,
          "legacyLastContactedAt" = $2,
          "legacyLastDealSentAt" = $3,
          "mldmMigratedModmonAt" = NOW(),
          "updatedAt" = NOW()
        WHERE id = $4
      `;

      await client.query(updateQuery, [summary, lastContactedAt, lastDealSentAt, careRecipientId]);

      // Verify update
      const verifyQuery = `
        SELECT
          "legacyContactHistorySummary",
          "legacyLastContactedAt",
          "legacyLastDealSentAt",
          "mldmMigratedModmonAt"
        FROM care_recipients
        WHERE id = $1
      `;
      const result = await client.query(verifyQuery, [careRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyContactHistorySummary).toBe(summary);
      expect(result.rows[0].legacyLastContactedAt).toEqual(lastContactedAt);
      expect(result.rows[0].legacyLastDealSentAt).toEqual(lastDealSentAt);
      expect(result.rows[0].mldmMigratedModmonAt).not.toBeNull();
    });

    it('should handle null timestamps', async () => {
      const careRecipientId = '00000000-0000-0000-0000-000000000001';
      const summary = '[INQUIRY] ProviderInquiry - Assisted Living - San Francisco, CA - Mar 15, 2024';
      const lastContactedAt = new Date('2024-03-15T09:15:00Z');
      const lastDealSentAt = null; // No lead sends

      const updateQuery = `
        UPDATE care_recipients
        SET
          "legacyContactHistorySummary" = $1,
          "legacyLastContactedAt" = $2,
          "legacyLastDealSentAt" = $3,
          "mldmMigratedModmonAt" = NOW(),
          "updatedAt" = NOW()
        WHERE id = $4
      `;

      await client.query(updateQuery, [summary, lastContactedAt, lastDealSentAt, careRecipientId]);

      // Verify update
      const verifyQuery = `
        SELECT
          "legacyContactHistorySummary",
          "legacyLastContactedAt",
          "legacyLastDealSentAt"
        FROM care_recipients
        WHERE id = $1
      `;
      const result = await client.query(verifyQuery, [careRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyContactHistorySummary).toBe(summary);
      expect(result.rows[0].legacyLastContactedAt).toEqual(lastContactedAt);
      expect(result.rows[0].legacyLastDealSentAt).toBeNull();
    });

    it('should handle empty summary', async () => {
      const careRecipientId = '00000000-0000-0000-0000-000000000001';
      const summary = '';
      const lastContactedAt = null;
      const lastDealSentAt = null;

      const updateQuery = `
        UPDATE care_recipients
        SET
          "legacyContactHistorySummary" = $1,
          "legacyLastContactedAt" = $2,
          "legacyLastDealSentAt" = $3,
          "mldmMigratedModmonAt" = NOW()
        WHERE id = $4
      `;

      await client.query(updateQuery, [summary, lastContactedAt, lastDealSentAt, careRecipientId]);

      // Verify update
      const verifyQuery = `SELECT "legacyContactHistorySummary" FROM care_recipients WHERE id = $1`;
      const result = await client.query(verifyQuery, [careRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyContactHistorySummary).toBe('');
    });
  });
});

