import { newDb, DataType } from 'pg-mem';

/**
 * Create an in-memory PostgreSQL database for contact history migration tests
 */
export function createContactHistoryTestDatabase() {
  const db = newDb();

  // Register gen_random_uuid function
  db.public.registerFunction({
    name: 'gen_random_uuid',
    returns: DataType.uuid,
    implementation: () => {
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
        const r = (Math.random() * 16) | 0;
        const v = c === 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
      });
    },
  });

  // Register NOW() function
  db.public.registerFunction({
    name: 'now',
    returns: DataType.timestamptz,
    implementation: () => new Date(),
  });

  // Create care_recipients table with contact history fields
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
      "legacyContactHistorySummary" VARCHAR(1000),
      "legacyLastContactedAt" TIMESTAMP,
      "legacyLastDealSentAt" TIMESTAMP,
      "mldmMigratedAt" TIMESTAMP,
      "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP DEFAULT NOW(),
      "deletedAt" TIMESTAMP
    )
  `);

  return db;
}

/**
 * Seed test data for contact history migration
 */
export async function seedContactHistoryTestData(db: any) {
  // Seed care_recipients
  // - One ready to migrate (mldmMigratedAt = NULL)
  // - One already migrated (mldmMigratedAt set)
  // - One with existing summary (to test overwrite)
  await db.public.none(`
    INSERT INTO care_recipients (
      id, 
      "legacyId", 
      "firstName", 
      "lastName", 
      "createdAt",
      "mldmMigratedAt",
      "legacyContactHistorySummary",
      "legacyLastContactedAt",
      "legacyLastDealSentAt"
    )
    VALUES
      (
        '00000000-0000-0000-0000-000000000001'::uuid, 
        '9987168', 
        'John', 
        'Doe', 
        '2024-01-01T00:00:00Z',
        NULL,
        NULL,
        NULL,
        NULL
      ),
      (
        '00000000-0000-0000-0000-000000000002'::uuid, 
        '9987169', 
        'Jane', 
        'Smith', 
        '2024-01-02T00:00:00Z',
        '2024-03-01T00:00:00Z',
        '[CALL] Previous call - Mar 1, 2024',
        '2024-03-01T10:00:00Z',
        '2024-02-28T09:00:00Z'
      ),
      (
        '00000000-0000-0000-0000-000000000003'::uuid, 
        '9987170', 
        'Bob', 
        'Johnson', 
        '2024-01-03T00:00:00Z',
        NULL,
        NULL,
        NULL,
        NULL
      )
  `);

  return {
    careRecipients: [
      { 
        id: '00000000-0000-0000-0000-000000000001', 
        legacyId: '9987168',
        mldmMigratedAt: null 
      },
      { 
        id: '00000000-0000-0000-0000-000000000002', 
        legacyId: '9987169',
        mldmMigratedAt: new Date('2024-03-01T00:00:00Z')
      },
      { 
        id: '00000000-0000-0000-0000-000000000003', 
        legacyId: '9987170',
        mldmMigratedAt: null
      },
    ],
  };
}

/**
 * Get a test client for querying the database
 */
export async function getTestClient(db: any): Promise<any> {
  const { Client } = db.adapters.createPg();
  const client = new Client();
  await client.connect();
  return client;
}

