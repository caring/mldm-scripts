import { subYears, subMonths, subDays } from 'date-fns';

/**
 * Parse time parameter to Date
 * Supports:
 * - "now" -> current date
 * - "5 years" -> 5 years ago from reference
 * - "30 days" -> 30 days ago from reference
 * - "2024-01-01" -> specific date
 * - "2024-01-01T10:30:00Z" -> specific timestamp
 */
export function parseTimeParam(param: string, referenceDate: Date = new Date()): Date {
  if (param === 'now') {
    return new Date();
  }

  // Check for relative time formats
  const relativeMatch = param.match(/^(\d+)\s+(year|years|month|months|day|days)$/i);
  if (relativeMatch) {
    const amount = parseInt(relativeMatch[1], 10);
    const unit = relativeMatch[2].toLowerCase();

    if (unit.startsWith('year')) {
      return subYears(referenceDate, amount);
    } else if (unit.startsWith('month')) {
      return subMonths(referenceDate, amount);
    } else if (unit.startsWith('day')) {
      return subDays(referenceDate, amount);
    }
  }

  // Try parsing as absolute date/timestamp
  const date = new Date(param);
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid time parameter: ${param}`);
  }

  return date;
}

/**
 * Format date to ISO string
 */
export function formatDate(date: Date): string {
  return date.toISOString();
}

/**
 * Get relative description for time range
 */
export function getRelativeDescription(from: Date, to: Date): string {
  const diffMs = from.getTime() - to.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  const diffYears = Math.floor(diffDays / 365);
  const diffMonths = Math.floor(diffDays / 30);

  if (diffYears >= 1) {
    return `${diffYears} year${diffYears > 1 ? 's' : ''}`;
  } else if (diffMonths >= 1) {
    return `${diffMonths} month${diffMonths > 1 ? 's' : ''}`;
  } else {
    return `${diffDays} day${diffDays > 1 ? 's' : ''}`;
  }
}

