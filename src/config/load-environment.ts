import * as dotenv from 'dotenv';
import { join } from 'path';

const supportedEnvironments = ['stage', 'prod'] as const;

export type ScriptEnvironment = (typeof supportedEnvironments)[number];

function normalizeEnvironment(environment?: string): string | undefined {
  const normalizedEnvironment = environment?.trim().toLowerCase();

  return normalizedEnvironment ? normalizedEnvironment : undefined;
}

function isSupportedEnvironment(environment: string): environment is ScriptEnvironment {
  return supportedEnvironments.includes(environment as ScriptEnvironment);
}

export function getScriptEnvironment(
  environment = process.env.ENVIRONMENT,
): ScriptEnvironment {
  const normalizedEnvironment = normalizeEnvironment(environment);

  if (!normalizedEnvironment) {
    throw new Error('ENVIRONMENT is required. Use "stage" or "prod".');
  }

  if (!isSupportedEnvironment(normalizedEnvironment)) {
    throw new Error(
      `Unsupported ENVIRONMENT="${normalizedEnvironment}". Use "stage" or "prod".`,
    );
  }

  return normalizedEnvironment;
}

export function resolveEnvFilePath(
  environment = process.env.ENVIRONMENT,
  rootDirectory = process.cwd(),
): string {
  const scriptEnvironment = getScriptEnvironment(environment);

  return join(rootDirectory, `.env.${scriptEnvironment}`);
}

export function loadEnvironmentConfig(
  environment = process.env.ENVIRONMENT,
  rootDirectory = process.cwd(),
): string {
  const envFilePath = resolveEnvFilePath(environment, rootDirectory);
  const result = dotenv.config({ path: envFilePath });

  if (result.error) {
    throw new Error(`Failed to load environment file "${envFilePath}": ${result.error.message}`);
  }

  return envFilePath;
}