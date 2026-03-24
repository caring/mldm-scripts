import { newDb, DataType } from 'pg-mem';

/**
 * Create an in-memory PostgreSQL database with MM schema
 */
export function createTestDatabase() {
  const db = newDb();

  // Register gen_random_uuid function (pg-mem doesn't have it by default)
  db.public.registerFunction({
    name: 'gen_random_uuid',
    returns: DataType.uuid,
    implementation: () => {
      // Simple UUID v4 generator
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = (Math.random() * 16) | 0;
        const v = c === 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
      });
    },
  });

  // Create care_recipients table
  db.public.none(`
    CREATE TABLE care_recipients (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      "legacyId" VARCHAR UNIQUE,
      "firstName" VARCHAR,
      "lastName" VARCHAR,
      email VARCHAR,
      "phoneNumber" VARCHAR,
      timezone VARCHAR,
      status VARCHAR,
      "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP DEFAULT NOW(),
      "deletedAt" TIMESTAMP
    )
  `);

  // Create agents table
  db.public.none(`
    CREATE TABLE agents (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      "legacyId" VARCHAR UNIQUE,
      email VARCHAR,
      "firstName" VARCHAR,
      "lastName" VARCHAR,
      "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP DEFAULT NOW(),
      "deletedAt" TIMESTAMP
    )
  `);

  // Create care_recipient_notes table
  db.public.none(`
    CREATE TABLE care_recipient_notes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      "legacyId" VARCHAR,
      "careRecipientId" UUID,
      value TEXT NOT NULL,
      "agentAccountId" UUID,
      "agentName" VARCHAR DEFAULT '',
      source VARCHAR(100) NOT NULL DEFAULT 'contact_history',
      "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP DEFAULT NOW(),
      "deletedAt" TIMESTAMP,
      UNIQUE ("legacyId", source)
    )
  `);

  return db;
}

/**
 * Seed test data based on real data structure from test-affiliate-notes-query.ts
 */
export async function seedTestData(db: any) {
  // Seed care_recipients (based on real DIR data)
  await db.public.none(`
    INSERT INTO care_recipients (id, "legacyId", "firstName", "lastName", "createdAt")
    VALUES
      ('00000000-0000-0000-0000-000000000001'::uuid, '9987168', 'John', 'Doe', '2024-01-01T00:00:00Z'),
      ('00000000-0000-0000-0000-000000000002'::uuid, '9987169', 'Jane', 'Smith', '2024-01-02T00:00:00Z'),
      ('00000000-0000-0000-0000-000000000003'::uuid, '9987170', 'Bob', 'Johnson', '2024-01-03T00:00:00Z')
  `);

  // Seed agents (some notes have null account_id)
  await db.public.none(`
    INSERT INTO agents (id, "legacyId", email, "firstName", "lastName", "createdAt")
    VALUES
      ('00000000-0000-0000-0000-000000000101'::uuid, '100', 'agent1@caring.com', 'Agent', 'One', '2024-01-01T00:00:00Z'),
      ('00000000-0000-0000-0000-000000000102'::uuid, '200', 'agent2@caring.com', 'Agent', 'Two', '2024-01-02T00:00:00Z')
  `);

  return {
    careRecipients: [
      { id: '00000000-0000-0000-0000-000000000001', legacyId: '9987168' },
      { id: '00000000-0000-0000-0000-000000000002', legacyId: '9987169' },
      { id: '00000000-0000-0000-0000-000000000003', legacyId: '9987170' },
    ],
    agents: [
      { id: '00000000-0000-0000-0000-000000000101', legacyId: '100' },
      { id: '00000000-0000-0000-0000-000000000102', legacyId: '200' },
    ],
  };
}

/**
 * Get a pg Client from pg-mem database
 */
export async function getTestClient(db: any): Promise<any> {
  const { Client } = db.adapters.createPg();
  const client = new Client();
  await client.connect();
  return client;
}

/**
 * Sample affiliate notes data (mimicking DIR structure)
 */
export const sampleAffiliateNotes = [
  {
    formatted_text_id: 8775048,
    note_content: 'Interested in the Redwoods',
    inquiry_id: 16880823,
    contact_id: 9821523,
    dir_care_recipient_id: 9987168,
    dir_account_id: 100,
    created_at: new Date('2026-03-11T17:03:29.000Z'),
    updated_at: new Date('2026-03-11T17:03:29.000Z'),
  },
  {
    formatted_text_id: 8775049,
    note_content: 'Looking for memory care options',
    inquiry_id: 16880824,
    contact_id: 9821524,
    dir_care_recipient_id: 9987169,
    dir_account_id: null,
    created_at: new Date('2026-03-11T16:00:00.000Z'),
    updated_at: new Date('2026-03-11T16:00:00.000Z'),
  },
  {
    formatted_text_id: 8775050,
    note_content: 'Prefers assisted living near downtown',
    inquiry_id: 16880825,
    contact_id: 9821525,
    dir_care_recipient_id: 9987170,
    dir_account_id: 200,
    created_at: new Date('2026-03-11T15:00:00.000Z'),
    updated_at: new Date('2026-03-11T15:00:00.000Z'),
  },
];

