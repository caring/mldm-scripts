import { describe, it, expect } from 'vitest';
import { mkdtemp, writeFile, rm } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  parseLeadIdsFromCsv,
  resolveExplicitCareRecipientLeads,
  fetchCareRecipientLeadsByDateRange,
} from './care-recipient-lead-selection';
import { MigrationCLIOptions } from './migration-cli';

describe('parseLeadIdsFromCsv', () => {
  it('parses legacyId csv column', () => {
    const csv = `legacyId,name\n57601684,A\n57601685,B\n`;
    expect(parseLeadIdsFromCsv(csv)).toEqual({
      legacyIds: [57601684, 57601685],
      crlIds: [],
    });
  });

  it('parses CRL id csv column', () => {
    const csv = `id,name\n11111111-1111-1111-1111-111111111111,A\n22222222-2222-2222-2222-222222222222,B\n`;
    expect(parseLeadIdsFromCsv(csv)).toEqual({
      legacyIds: [],
      crlIds: [
        '11111111-1111-1111-1111-111111111111',
        '22222222-2222-2222-2222-222222222222',
      ],
    });
  });
});

describe('resolveExplicitCareRecipientLeads', () => {
  it('resolves ids-inline as legacy IDs and verifies MM leads', async () => {
    const options: MigrationCLIOptions = {
      from: 'now',
      to: null,
      batchSize: 1000,
      dryRun: true,
      retryFailed: false,
      report: false,
      idsFile: null,
      idsInline: '57601684,57601685',
      lookbackYears: null,
    };

    const pgCalls: any[] = [];
    const pgClient = {
      query: async (query: string, params: any[]) => {
        pgCalls.push({ query, params });
        return {
          rows: [
            { id: '11111111-1111-1111-1111-111111111111', legacyId: '57601684' },
            { id: '22222222-2222-2222-2222-222222222222', legacyId: '57601685' },
          ],
        };
      },
    };

    const result = await resolveExplicitCareRecipientLeads(options, pgClient);
    expect(result?.requestedCount).toBe(2);
    expect(result?.matchedLeads.map((l) => l.legacyId)).toEqual([57601684, 57601685]);
    expect(pgCalls[0].query).toContain('"legacyId" = ANY($1)');
  });

  it('resolves csv legacyId column', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'lead-selection-legacy-'));
    const csvPath = join(dir, 'legacy.csv');
    try {
      await writeFile(csvPath, 'legacyId\n57601684\n57601685\n');
      const options: MigrationCLIOptions = {
        from: 'now',
        to: null,
        batchSize: 1000,
        dryRun: true,
        retryFailed: false,
        report: false,
        idsFile: csvPath,
        idsInline: null,
        lookbackYears: null,
      };

      const result = await resolveExplicitCareRecipientLeads(options, {
        query: async () => ({
          rows: [
            { id: '11111111-1111-1111-1111-111111111111', legacyId: '57601684' },
            { id: '22222222-2222-2222-2222-222222222222', legacyId: '57601685' },
          ],
        }),
      });

      expect(result?.requestedCount).toBe(2);
      expect(result?.matchedLeads.map((l) => l.legacyId)).toEqual([57601684, 57601685]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('resolves csv CRL id column by mapping to legacyId in MM', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'lead-selection-crl-'));
    const csvPath = join(dir, 'crl.csv');
    try {
      await writeFile(csvPath, 'id\n11111111-1111-1111-1111-111111111111\n');
      const options: MigrationCLIOptions = {
        from: 'now',
        to: null,
        batchSize: 1000,
        dryRun: true,
        retryFailed: false,
        report: false,
        idsFile: csvPath,
        idsInline: null,
        lookbackYears: null,
      };

      const result = await resolveExplicitCareRecipientLeads(options, {
        query: async () => ({
          rows: [{ id: '11111111-1111-1111-1111-111111111111', legacyId: '57601684' }],
        }),
      });

      expect(result?.requestedCount).toBe(1);
      expect(result?.matchedLeads.map((l) => l.legacyId)).toEqual([57601684]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe('fetchCareRecipientLeadsByDateRange', () => {
  it('queries MM leads by date range and returns normalized rows', async () => {
    const pgCalls: any[] = [];
    const pgClient = {
      query: async (query: string, params: any[]) => {
        pgCalls.push({ query, params });
        return {
          rows: [
            { id: '11111111-1111-1111-1111-111111111111', legacyId: '57601684' },
            { id: '22222222-2222-2222-2222-222222222222', legacyId: null },
            { id: '33333333-3333-3333-3333-333333333333', legacyId: '57601685' },
          ],
        };
      },
    };

    const rows = await fetchCareRecipientLeadsByDateRange(
      pgClient,
      new Date('2026-03-31T00:00:00Z'),
      new Date('2025-03-31T00:00:00Z'),
      1000,
      0
    );

    expect(rows).toEqual([
      { careRecipientLeadId: '11111111-1111-1111-1111-111111111111', legacyId: 57601684 },
      { careRecipientLeadId: '33333333-3333-3333-3333-333333333333', legacyId: 57601685 },
    ]);
    expect(pgCalls[0].query).toContain('"legacyId" IS NOT NULL');
    expect(pgCalls[0].query).toContain('"createdAt" <=');
    expect(pgCalls[0].query).toContain('"createdAt" >=');
  });
});
