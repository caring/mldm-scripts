/**
 * Common CLI utilities for migration scripts
 */

import { promises as fs } from 'fs';

export interface MigrationCLIOptions {
  from: string;
  to: string | null;
  batchSize: number;
  dryRun: boolean;
  retryFailed: boolean;
  report: boolean;
  idsFile: string | null;
  idsInline: string | null;
  lookbackYears: number | null;
}

/**
 * Parse CLI arguments for migration scripts
 */
export function parseMigrationArgs(): MigrationCLIOptions {
  const args = process.argv.slice(2);
  const options: MigrationCLIOptions = {
    from: 'now',
    to: null,
    batchSize: 5000,
    dryRun: false,
    retryFailed: false,
    report: false,
    idsFile: null,
    idsInline: null,
    lookbackYears: null,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    switch (arg) {
      case '--from':
        options.from = args[++i];
        break;
      case '--to':
        options.to = args[++i];
        break;
      case '--batch-size':
        options.batchSize = parseInt(args[++i], 10);
        break;
      case '--lookback-years':
        options.lookbackYears = parseInt(args[++i], 10);
        break;
      case '--dry-run':
        options.dryRun = true;
        break;
      case '--retry-failed':
        options.retryFailed = true;
        break;
      case '--report':
        options.report = true;
        break;
      case '--ids':
        options.idsFile = args[++i];
        break;
      case '--ids-inline':
        options.idsInline = args[++i];
        break;
      default:
        if (arg.startsWith('--')) {
          console.error(`Unknown option: ${arg}`);
          process.exit(1);
        }
    }
  }

  return options;
}

/**
 * Print help for migration scripts
 */
export function printMigrationHelp(scriptName: string) {
  console.log(`
Usage: npm run ${scriptName} -- [options]

Options:
  --from <date>         Start timestamp (default: "now")
                        Examples: "now", "2024-01-01"

  --to <date>           End timestamp (optional)
                        Examples: "5 years", "30 days", "2020-01-01"

  --batch-size <num>    Number of rows per batch (default: 5000)

  --lookback-years <num> Number of years to look back (default: varies by script)
                        Example: --lookback-years 3

  --ids <file>          Path to file with IDs to migrate (one per line)
                        Overrides time-based migration

  --ids-inline <ids>    Comma-separated list of IDs to migrate
                        Example: "123,456,789"

  --dry-run             Show what would be processed without migrating

  --retry-failed        Retry only previously failed rows

  --report              Generate and display migration report

Examples:
  # Migrate last 5 years
  npm run ${scriptName} -- --to "5 years"

  # Test with small batch
  npm run ${scriptName} -- --batch-size 100 --to "30 days"

  # Migrate specific IDs from file
  npm run ${scriptName} -- --ids ids.txt

  # Migrate specific IDs inline
  npm run ${scriptName} -- --ids-inline "123,456,789"

  # Dry run to see what would be processed
  npm run ${scriptName} -- --to "1 year" --dry-run

  # Generate report
  npm run ${scriptName} -- --report
`);
}

/**
 * Check if help was requested
 */
export function isHelpRequested(): boolean {
  return process.argv.includes('--help') || process.argv.includes('-h');
}

/**
 * Print migration options
 */
export function printMigrationOptions(options: MigrationCLIOptions): void {
  console.log('Options:', options);
  console.log();
}

/**
 * Parse IDs from file or inline parameter
 */
export async function parseIds(options: MigrationCLIOptions): Promise<number[] | null> {
  // If --ids-inline provided
  if (options.idsInline) {
    const ids = options.idsInline
      .split(',')
      .map(id => id.trim())
      .filter(id => id)
      .map(id => parseInt(id, 10))
      .filter(id => !isNaN(id));

    if (ids.length === 0) {
      throw new Error('No valid IDs found in --ids-inline parameter');
    }

    return ids;
  }

  // If --ids file provided
  if (options.idsFile) {
    const content = await fs.readFile(options.idsFile, 'utf-8');
    const ids = content
      .split('\n')
      .map(line => line.trim())
      .filter(line => line && !line.startsWith('#'))  // Skip empty lines and comments
      .map(id => parseInt(id, 10))
      .filter(id => !isNaN(id));

    if (ids.length === 0) {
      throw new Error(`No valid IDs found in file: ${options.idsFile}`);
    }

    return ids;
  }

  return null;  // No IDs provided, use time-based migration
}

