import {
  buildConcatenatedLeadNotePayload,
  buildLeadNotesToInsert,
  fetchCareRecipientIdsWithNotesAndLeads,
  formatNoteValue,
  generateUUID,
} from './care-recipient-lead-notes';

interface MMCareRecipientNote {
  id: string;
  careRecipientId: string;
  noteText: string;
  noteType: string;
  createdAt: Date;
}

describe('generateUUID', () => {
  it('generates a valid UUID v4', () => {
    const uuid = generateUUID();
    expect(uuid).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  });

  it('generates unique UUIDs', () => {
    const uuid1 = generateUUID();
    const uuid2 = generateUUID();
    expect(uuid1).not.toBe(uuid2);
  });
});

describe('formatNoteValue', () => {
  function makeNote(overrides: Partial<MMCareRecipientNote> = {}): MMCareRecipientNote {
    return {
      id: 'note-1',
      careRecipientId: 'cr-1',
      noteText: 'Test note text',
      noteType: 'AFFILIATE',
      createdAt: new Date(2025, 2, 15, 14, 30, 0),
      ...overrides,
    };
  }

  it('formats affiliate note with type prefix', () => {
    const note = makeNote({ noteType: 'AFFILIATE', noteText: 'This is an affiliate note' });
    const result = formatNoteValue(note);
    expect(result).toBe('[AFFILIATE] This is an affiliate note');
  });

  it('formats internal note with type prefix', () => {
    const note = makeNote({ noteType: 'INTERNAL', noteText: 'This is an internal note' });
    const result = formatNoteValue(note);
    expect(result).toBe('[INTERNAL] This is an internal note');
  });

  it('preserves special characters in note text', () => {
    const note = makeNote({ noteText: 'Note with special chars: @#$%^&*()' });
    const result = formatNoteValue(note);
    expect(result).toBe('[AFFILIATE] Note with special chars: @#$%^&*()');
  });

  it('preserves multiline note text', () => {
    const note = makeNote({ noteText: 'Line 1\nLine 2\nLine 3' });
    const result = formatNoteValue(note);
    expect(result).toBe('[AFFILIATE] Line 1\nLine 2\nLine 3');
  });

  it('handles empty note text', () => {
    const note = makeNote({ noteText: '' });
    const result = formatNoteValue(note);
    expect(result).toBe('[AFFILIATE] ');
  });

  it('handles very long note text without truncation', () => {
    const longText = 'A'.repeat(150000);
    const note = makeNote({ noteText: longText });
    const result = formatNoteValue(note);
    expect(result).toBe(`[AFFILIATE] ${longText}`);
    expect(result.length).toBe(longText.length + 12); // +12 for "[AFFILIATE] "
  });
});

describe('fetchCareRecipientIdsWithNotesAndLeads', () => {
  it('filters affected care recipients by from date', async () => {
    const pgCalls: Array<{ query: string; params: any[] }> = [];
    const pgClient = {
      query: async (query: string, params: any[]) => {
        pgCalls.push({ query, params });
        return {
          rows: [{ careRecipientId: 'cr-1' }],
        };
      },
    };

    const fromDate = new Date('2024-01-01T00:00:00.000Z');

    const rows = await fetchCareRecipientIdsWithNotesAndLeads(
      pgClient,
      fromDate,
      null,
      500,
      '00000000-0000-0000-0000-000000000000'
    );

    expect(rows).toEqual(['cr-1']);
    expect(pgCalls[0].query).toContain('AND crn."createdAt" >= $2');
    expect(pgCalls[0].query).toContain('AND crn."careRecipientId" > $3');
    expect(pgCalls[0].query).toContain('AND EXISTS (');
    expect(pgCalls[0].query).toContain('FROM care_recipient_leads crl');
    expect(pgCalls[0].query).not.toContain('source IN');
    expect(pgCalls[0].params).toEqual([
      500,
      fromDate,
      '00000000-0000-0000-0000-000000000000',
    ]);
  });

  it('adds the to date filter when provided', async () => {
    const pgCalls: Array<{ query: string; params: any[] }> = [];
    const pgClient = {
      query: async (query: string, params: any[]) => {
        pgCalls.push({ query, params });
        return {
          rows: [],
        };
      },
    };

    const fromDate = new Date('2024-01-01T00:00:00.000Z');
    const toDate = new Date('2024-12-31T23:59:59.999Z');

    await fetchCareRecipientIdsWithNotesAndLeads(
      pgClient,
      fromDate,
      toDate,
      250,
      'cursor-id'
    );

    expect(pgCalls[0].query).toContain('AND "createdAt" <= $4');
    expect(pgCalls[0].params).toEqual([250, fromDate, 'cursor-id', toDate]);
  });
});

describe('buildConcatenatedLeadNotePayload', () => {
  function makeNote(overrides: Partial<MMCareRecipientNote> = {}): MMCareRecipientNote {
    return {
      id: 'note-1',
      careRecipientId: 'cr-1',
      noteText: 'Test note text',
      noteType: 'affiliate_notes',
      createdAt: new Date('2025-01-01T00:00:00.000Z'),
      ...overrides,
    };
  }

  it('joins notes with blank lines and preserves order', () => {
    const payload = buildConcatenatedLeadNotePayload([
      makeNote({ id: '1', noteText: 'Affiliate note', noteType: 'affiliate_notes' }),
      makeNote({ id: '2', noteText: 'Internal note', noteType: 'internal_notes', createdAt: new Date('2025-02-01T00:00:00.000Z') }),
    ]);

    expect(payload).toEqual({
      value: '[affiliate_notes] Affiliate note\n\n[internal_notes] Internal note',
      includedNotesCount: 2,
      createdAt: new Date('2025-02-01T00:00:00.000Z'),
    });
  });

  it('caps the final value at 3000 characters', () => {
    const longNote = makeNote({ noteText: 'A'.repeat(4000) });
    const payload = buildConcatenatedLeadNotePayload([longNote]);

    expect(payload?.value).toHaveLength(3000);
    expect(payload?.includedNotesCount).toBe(1);
  });

  it('stops before adding a note that would exceed the limit', () => {
    const payload = buildConcatenatedLeadNotePayload([
      makeNote({ id: '1', noteText: 'A'.repeat(2900) }),
      makeNote({ id: '2', noteText: 'B'.repeat(200), noteType: 'internal_notes' }),
    ]);

    expect(payload?.includedNotesCount).toBe(1);
    expect(payload?.value).toContain('[affiliate_notes]');
    expect(payload?.value).not.toContain('[internal_notes]');
  });

  it('trims the final concatenated value to 3000 characters when the first note alone is too long', () => {
    const payload = buildConcatenatedLeadNotePayload([
      makeNote({ id: '1', noteText: 'A'.repeat(4000), noteType: 'affiliate_notes' }),
      makeNote({ id: '2', noteText: 'Should never appear', noteType: 'internal_notes' }),
    ]);

    const expectedValue = `[affiliate_notes] ${'A'.repeat(2982)}`;

    expect(payload).toEqual({
      value: expectedValue,
      includedNotesCount: 1,
      createdAt: new Date('2025-01-01T00:00:00.000Z'),
    });
    expect(payload?.value).toHaveLength(3000);
    expect(payload?.value).not.toContain('Should never appear');
  });
});

describe('buildLeadNotesToInsert', () => {
  it('creates one concatenated row per lead for the same care recipient', () => {
    const migratedAt = new Date('2025-03-01T00:00:00.000Z');
    const rows = buildLeadNotesToInsert(
      [
        { id: 'lead-1', legacyId: 'legacy-1', careRecipientId: 'cr-1' },
        { id: 'lead-2', legacyId: 'legacy-2', careRecipientId: 'cr-1' },
      ],
      {
        'cr-1': [
          {
            id: 'note-1',
            careRecipientId: 'cr-1',
            noteText: 'Affiliate note',
            noteType: 'affiliate_notes',
            createdAt: new Date('2025-01-01T00:00:00.000Z'),
          },
          {
            id: 'note-2',
            careRecipientId: 'cr-1',
            noteText: 'Internal note',
            noteType: 'internal_notes',
            createdAt: new Date('2025-02-01T00:00:00.000Z'),
          },
        ],
      },
      migratedAt
    );

    expect(rows).toHaveLength(2);
    expect(rows[0].leadId).toBe('lead-1');
    expect(rows[1].leadId).toBe('lead-2');
    expect(rows[0].value).toBe('[affiliate_notes] Affiliate note\n\n[internal_notes] Internal note');
    expect(rows[1].value).toBe(rows[0].value);
    expect(rows[0].mldmMigratedModmonAt).toBe(migratedAt);
    expect(rows[0].createdAt).toEqual(new Date('2025-02-01T00:00:00.000Z'));
  });
});
