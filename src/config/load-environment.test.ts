import { mkdtempSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { getScriptEnvironment, loadEnvironmentConfig, resolveEnvFilePath } from './load-environment';

const originalEnvironment = process.env.ENVIRONMENT;
const originalMysqlHost = process.env.MYSQL_HOST;

let tempDirectory: string | undefined;

function restoreEnvVariable(name: 'ENVIRONMENT' | 'MYSQL_HOST', value: string | undefined) {
  if (value === undefined) {
    delete process.env[name];
    return;
  }

  process.env[name] = value;
}

afterEach(() => {
  if (tempDirectory) {
    rmSync(tempDirectory, { recursive: true, force: true });
    tempDirectory = undefined;
  }

  restoreEnvVariable('ENVIRONMENT', originalEnvironment);
  restoreEnvVariable('MYSQL_HOST', originalMysqlHost);
});

describe('load-environment', () => {
  it('throws when ENVIRONMENT is not set', () => {
    expect(() => getScriptEnvironment(undefined)).toThrow(
      'ENVIRONMENT is required. Use "stage" or "prod".',
    );
  });

  it('resolves the stage env file when ENVIRONMENT is stage', () => {
    expect(resolveEnvFilePath('stage', '/tmp/mldm')).toBe('/tmp/mldm/.env.stage');
  });

  it('throws for unsupported environments', () => {
    expect(() => resolveEnvFilePath('qa', '/tmp/mldm')).toThrow(
      'Unsupported ENVIRONMENT="qa". Use "stage" or "prod".',
    );
  });

  it('loads values from the selected environment file', () => {
    tempDirectory = mkdtempSync(join(tmpdir(), 'mldm-env-'));
    writeFileSync(join(tempDirectory, '.env.stage'), 'MYSQL_HOST=stage-host\n');
    delete process.env.MYSQL_HOST;

    loadEnvironmentConfig('stage', tempDirectory);

    expect(process.env.MYSQL_HOST).toBe('stage-host');
  });

  it('throws when the selected environment file is missing', () => {
    tempDirectory = mkdtempSync(join(tmpdir(), 'mldm-env-'));

    expect(() => loadEnvironmentConfig('prod', tempDirectory)).toThrow(
      `Failed to load environment file "${join(tempDirectory, '.env.prod')}":`,
    );
  });
});