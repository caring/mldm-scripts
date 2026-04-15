import { join } from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { getMigrationStatePath, getMigrationStateRoot } from './file-utils';

const originalEnvironment = process.env.ENVIRONMENT;

afterEach(() => {
  if (originalEnvironment === undefined) {
    delete process.env.ENVIRONMENT;
    return;
  }

  process.env.ENVIRONMENT = originalEnvironment;
});

describe('file-utils migration state paths', () => {
  it('throws when ENVIRONMENT is not set', () => {
    delete process.env.ENVIRONMENT;

    expect(() => getMigrationStateRoot()).toThrow(
      'ENVIRONMENT is required. Use "stage" or "prod".',
    );
  });

  it('uses the prod namespace for prod environment', () => {
    process.env.ENVIRONMENT = 'prod';

    expect(getMigrationStateRoot()).toBe(join('migration-state', 'prod'));
  });

  it('uses the stage namespace for stage environment', () => {
    process.env.ENVIRONMENT = 'stage';

    expect(getMigrationStatePath('affiliate_notes', 'rows.jsonl')).toBe(
      join('migration-state', 'stage', 'affiliate_notes', 'rows.jsonl'),
    );
  });
});