import { describe, it, expect, beforeEach } from 'vitest';
import { createContactHistoryTestDatabase, seedContactHistoryTestData, getTestClient } from '../../test/setup-contact-history-pg-mem';

describe('Contact History Migration', () => {
  let db: any;
  let client: any;

  beforeEach(async () => {
    // Create fresh database for each test
    db = createContactHistoryTestDatabase();
    await seedContactHistoryTestData(db);
    client = await getTestClient(db);
  });

  describe('Care Recipient Lookup', () => {
    it('should find MM care_recipient by legacyId', async () => {
      const dirCareRecipientId = '9987168';

      const query = `
        SELECT id, "mldmMigratedAt"
        FROM care_recipients
        WHERE "legacyId" = $1
          AND "deletedAt" IS NULL
      `;
      const result = await client.query(query, [dirCareRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].id).toBe('00000000-0000-0000-0000-000000000001');
      expect(result.rows[0].mldmMigratedAt).toBeNull();
    });

    it('should return empty for non-existent care_recipient', async () => {
      const dirCareRecipientId = '999999';

      const query = `
        SELECT id, "mldmMigratedAt"
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
        SELECT id, "mldmMigratedAt"
        FROM care_recipients
        WHERE "legacyId" = $1
          AND "deletedAt" IS NULL
      `;
      const result = await client.query(query, [dirCareRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].mldmMigratedAt).not.toBeNull();
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
          "mldmMigratedAt" = NOW(),
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
          "mldmMigratedAt"
        FROM care_recipients
        WHERE id = $1
      `;
      const result = await client.query(verifyQuery, [careRecipientId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyContactHistorySummary).toBe(summary);
      expect(result.rows[0].legacyLastContactedAt).toEqual(lastContactedAt);
      expect(result.rows[0].legacyLastDealSentAt).toEqual(lastDealSentAt);
      expect(result.rows[0].mldmMigratedAt).not.toBeNull();
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
          "mldmMigratedAt" = NOW(),
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
          "mldmMigratedAt" = NOW()
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

