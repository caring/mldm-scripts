import { describe, it, expect, beforeEach } from 'vitest';
import { createTestDatabase, seedTestData, getTestClient, sampleAffiliateNotes } from '../../test/setup-pg-mem';

describe('Affiliate Notes Migration', () => {
  let db: any;
  let client: any;

  beforeEach(async () => {
    // Create fresh database for each test
    db = createTestDatabase();
    await seedTestData(db);
    client = await getTestClient(db);
  });

  describe('Care Recipient Mapping', () => {
    it('should map DIR care_recipient_ids to MM care_recipient ids', async () => {
      const dirIds = ['9987168', '9987169', '9987170'];

      // pg-mem doesn't support ANY($1) with arrays, use IN instead
      const query = `SELECT id, "legacyId" FROM care_recipients WHERE "legacyId" IN ($1, $2, $3)`;
      const result = await client.query(query, dirIds);

      expect(result.rows).toHaveLength(3);
      expect(result.rows[0].legacyId).toBe('9987168');
      expect(result.rows[0].id).toBe('00000000-0000-0000-0000-000000000001');
    });

    it('should return empty map for non-existent care_recipients', async () => {
      const dirIds = ['999999'];

      const query = `SELECT id, "legacyId" FROM care_recipients WHERE "legacyId" IN ($1)`;
      const result = await client.query(query, dirIds);

      expect(result.rows).toHaveLength(0);
    });
  });

  describe('Agent Mapping', () => {
    it('should map DIR account_ids to MM agent ids', async () => {
      const dirIds = ['100', '200'];

      const query = `SELECT id, "legacyId" FROM agents WHERE "legacyId" IN ($1, $2)`;
      const result = await client.query(query, dirIds);

      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].legacyId).toBe('100');
      expect(result.rows[0].id).toBe('00000000-0000-0000-0000-000000000101');
    });

    it('should handle empty agent list', async () => {
      // When there are no agents to map, just verify empty result
      const query = `SELECT id, "legacyId" FROM agents WHERE "legacyId" IN ('nonexistent')`;
      const result = await client.query(query);

      expect(result.rows).toHaveLength(0);
    });
  });

  describe('Note Insertion', () => {
    it('should insert affiliate note successfully', async () => {
      const note = sampleAffiliateNotes[0];
      const legacyId = note.formatted_text_id.toString();
      
      const insertQuery = `
        INSERT INTO care_recipient_notes
          (id, "legacyId", "careRecipientId", value, "agentAccountId", "agentName", source, "createdAt", "updatedAt")
        VALUES
          (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, "legacyId"
      `;

      const result = await client.query(insertQuery, [
        legacyId,
        '00000000-0000-0000-0000-000000000001',
        note.note_content.trim(),
        '00000000-0000-0000-0000-000000000101',
        '',
        'affiliate_notes',
        note.created_at,
        note.updated_at,
      ]);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyId).toBe(legacyId);
    });

    it('should handle null agentAccountId', async () => {
      const note = sampleAffiliateNotes[1];
      const legacyId = note.formatted_text_id.toString();
      
      const insertQuery = `
        INSERT INTO care_recipient_notes
          (id, "legacyId", "careRecipientId", value, "agentAccountId", "agentName", source, "createdAt", "updatedAt")
        VALUES
          (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, "legacyId", "agentAccountId"
      `;

      const result = await client.query(insertQuery, [
        legacyId,
        '00000000-0000-0000-0000-000000000002',
        note.note_content.trim(),
        null, // null agent
        '',
        'affiliate_notes',
        note.created_at,
        note.updated_at,
      ]);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].agentAccountId).toBeNull();
    });


  });

  describe('Duplicate Detection', () => {
    it('should detect existing notes by legacyId', async () => {
      // Insert a note first
      const legacyId = '8775048';
      await client.query(`
        INSERT INTO care_recipient_notes ("legacyId", "careRecipientId", value, source)
        VALUES ($1, $2, $3, $4)
      `, [legacyId, '00000000-0000-0000-0000-000000000001', 'Test note', 'affiliate_notes']);

      // Check if it exists using IN instead of ANY
      const checkQuery = `SELECT "legacyId" FROM care_recipient_notes WHERE "legacyId" IN ($1)`;
      const result = await client.query(checkQuery, [legacyId]);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyId).toBe(legacyId);
    });
  });
});

