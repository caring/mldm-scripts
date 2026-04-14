import { describe, it, expect, beforeEach } from 'vitest';
import { createTestDatabase, seedTestData, getTestClient } from '../../test/setup-pg-mem';

describe('Self-Qualified Notes Migration', () => {
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
    it('should insert self-qualified note successfully', async () => {
      const contactId = '9821523';
      const legacyId = contactId;
      const noteContent = 'Looking for memory care options';
      
      const insertQuery = `
        INSERT INTO care_recipient_notes 
          (id, "legacyId", "careRecipientId", value, "agentAccountId", "agentName", source, "createdAt", "updatedAt")
        VALUES 
          (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, "legacyId", source
      `;
      
      const result = await client.query(insertQuery, [
        legacyId,
        '00000000-0000-0000-0000-000000000001',
        noteContent,
        '00000000-0000-0000-0000-000000000101',
        '',
        'self_qualified_notes',
        new Date('2026-03-11T16:00:00.000Z'),
        new Date('2026-03-11T16:00:00.000Z'),
      ]);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyId).toBe(legacyId);
      expect(result.rows[0].source).toBe('self_qualified_notes');
    });

    it('should handle null agentAccountId', async () => {
      const contactId = '9821524';
      const legacyId = contactId;
      const noteContent = 'Interested in assisted living';
      
      const insertQuery = `
        INSERT INTO care_recipient_notes 
          (id, "legacyId", "careRecipientId", value, "agentAccountId", "agentName", source, "createdAt", "updatedAt")
        VALUES 
          (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, "legacyId", "agentAccountId", source
      `;
      
      const result = await client.query(insertQuery, [
        legacyId,
        '00000000-0000-0000-0000-000000000002',
        noteContent,
        null, // null agent
        '',
        'self_qualified_notes',
        new Date('2026-03-11T16:00:00.000Z'),
        new Date('2026-03-11T16:00:00.000Z'),
      ]);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].agentAccountId).toBeNull();
      expect(result.rows[0].source).toBe('self_qualified_notes');
    });
  });

  describe('Duplicate Detection', () => {
    it('should detect existing notes by legacyId and source', async () => {
      // Insert a note first
      const legacyId = '9821523';
      await client.query(`
        INSERT INTO care_recipient_notes ("legacyId", "careRecipientId", value, source)
        VALUES ($1, $2, $3, $4)
      `, [legacyId, '00000000-0000-0000-0000-000000000001', 'Test note', 'self_qualified_notes']);
      
      // Check if it exists using IN instead of ANY
      const checkQuery = `SELECT "legacyId", source FROM care_recipient_notes WHERE "legacyId" IN ($1) AND source = $2`;
      const result = await client.query(checkQuery, [legacyId, 'self_qualified_notes']);
      
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].legacyId).toBe(legacyId);
      expect(result.rows[0].source).toBe('self_qualified_notes');
    });

    it('should allow same legacyId with different source', async () => {
      const legacyId = '9821523';

      // Insert affiliate note (with explicit ID to avoid pg-mem UUID collision)
      await client.query(`
        INSERT INTO care_recipient_notes (id, "legacyId", "careRecipientId", value, source)
        VALUES ($1, $2, $3, $4, $5)
      `, ['00000000-0000-0000-0000-000000000201', legacyId, '00000000-0000-0000-0000-000000000001', 'Affiliate note', 'affiliate_notes']);

      // Insert self-qualified note with same legacyId (should succeed)
      await client.query(`
        INSERT INTO care_recipient_notes (id, "legacyId", "careRecipientId", value, source)
        VALUES ($1, $2, $3, $4, $5)
      `, ['00000000-0000-0000-0000-000000000202', legacyId, '00000000-0000-0000-0000-000000000001', 'Self-qualified note', 'self_qualified_notes']);

      // Check both exist
      const checkQuery = `SELECT "legacyId", source FROM care_recipient_notes WHERE "legacyId" = $1 ORDER BY source`;
      const result = await client.query(checkQuery, [legacyId]);

      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].source).toBe('affiliate_notes');
      expect(result.rows[1].source).toBe('self_qualified_notes');
    });
  });
});

