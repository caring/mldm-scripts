import { formatNoteValue, generateUUID } from './care-recipient-lead-notes';

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
