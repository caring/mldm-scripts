import { describe, it, expect } from 'vitest';
import { parseTimeParam, formatDate, getRelativeDescription } from './time-utils';

describe('time-utils', () => {
  describe('parseTimeParam', () => {
    it('should parse "now" as current date', () => {
      const result = parseTimeParam('now');
      const now = new Date();
      
      // Should be within 1 second of now
      expect(Math.abs(result.getTime() - now.getTime())).toBeLessThan(1000);
    });

    it('should parse relative years', () => {
      const referenceDate = new Date('2026-03-12T00:00:00Z');
      const result = parseTimeParam('5 years', referenceDate);
      
      expect(result.getFullYear()).toBe(2021);
      expect(result.getMonth()).toBe(2); // March (0-indexed)
    });

    it('should parse relative months', () => {
      const referenceDate = new Date('2026-03-12T00:00:00Z');
      const result = parseTimeParam('6 months', referenceDate);
      
      expect(result.getFullYear()).toBe(2025);
      expect(result.getMonth()).toBe(8); // September (0-indexed)
    });

    it('should parse relative days', () => {
      const referenceDate = new Date('2026-03-12T00:00:00Z');
      const result = parseTimeParam('30 days', referenceDate);
      
      expect(result.getFullYear()).toBe(2026);
      expect(result.getMonth()).toBe(1); // February (0-indexed)
      expect(result.getDate()).toBe(10);
    });

    it('should parse absolute date strings', () => {
      const result = parseTimeParam('2024-01-01');
      
      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(0); // January
      expect(result.getDate()).toBe(1);
    });

    it('should parse absolute timestamp strings', () => {
      const result = parseTimeParam('2024-01-01T10:30:00Z');

      expect(result.getFullYear()).toBe(2024);
      expect(result.getMonth()).toBe(0);
      expect(result.getDate()).toBe(1);
      expect(result.getUTCHours()).toBe(10); // Use UTC hours
      expect(result.getUTCMinutes()).toBe(30); // Use UTC minutes
    });

    it('should throw error for invalid time parameter', () => {
      expect(() => parseTimeParam('invalid')).toThrow('Invalid time parameter');
    });
  });

  describe('formatDate', () => {
    it('should format date as ISO string', () => {
      const date = new Date('2024-01-01T10:30:00Z');
      const result = formatDate(date);
      
      expect(result).toBe('2024-01-01T10:30:00.000Z');
    });
  });

  describe('getRelativeDescription', () => {
    it('should describe years difference', () => {
      const from = new Date('2026-03-12T00:00:00Z');
      const to = new Date('2021-03-12T00:00:00Z');
      
      const result = getRelativeDescription(from, to);
      expect(result).toBe('5 years');
    });

    it('should describe single year difference', () => {
      const from = new Date('2026-03-12T00:00:00Z');
      const to = new Date('2025-03-12T00:00:00Z');
      
      const result = getRelativeDescription(from, to);
      expect(result).toBe('1 year');
    });

    it('should describe months difference', () => {
      const from = new Date('2026-03-12T00:00:00Z');
      const to = new Date('2025-09-12T00:00:00Z');
      
      const result = getRelativeDescription(from, to);
      expect(result).toBe('6 months');
    });

    it('should describe days difference', () => {
      const from = new Date('2026-03-12T00:00:00Z');
      const to = new Date('2026-02-25T00:00:00Z'); // 15 days difference

      const result = getRelativeDescription(from, to);
      expect(result).toBe('15 days');
    });
  });
});

