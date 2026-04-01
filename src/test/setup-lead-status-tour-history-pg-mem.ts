import { newDb, DataType } from 'pg-mem';

export function createLeadStatusTourHistoryTestDatabase() {
  const db = newDb();

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

  db.public.registerFunction({
    name: 'now',
    returns: DataType.timestamptz,
    implementation: () => new Date(),
  });

  db.public.none(`
    CREATE TABLE care_recipient_leads (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      "legacyId" VARCHAR UNIQUE,
      "legacyLeadStatusAndTourHistory" VARCHAR(2000),
      "leadPriority" VARCHAR(50),
      "pipelineStage" VARCHAR(50),
      "mldmMigratedModmonAt" TIMESTAMP WITH TIME ZONE,
      "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      "updatedAt" TIMESTAMP DEFAULT NOW(),
      "deletedAt" TIMESTAMP
    )
  `);

  return db;
}

export async function seedLeadStatusTourHistoryTestData(db: any) {
  await db.public.none(`
    INSERT INTO care_recipient_leads (
      id,
      "legacyId",
      "legacyLeadStatusAndTourHistory",
      "mldmMigratedModmonAt"
    )
    VALUES
      (
        '00000000-0000-0000-0000-000000000001'::uuid,
        '1001',
        NULL,
        NULL
      ),
      (
        '00000000-0000-0000-0000-000000000002'::uuid,
        '1002',
        '9/1/25 10:00am - Valid - Jane Smith',
        '2025-09-01T10:00:00Z'
      ),
      (
        '00000000-0000-0000-0000-000000000003'::uuid,
        '1003',
        NULL,
        NULL
      )
  `);
}

export async function getTestClient(db: any): Promise<any> {
  const { Client } = db.adapters.createPg();
  const client = new Client();
  await client.connect();
  return client;
}
