# Care Recipient Lead Notes Migration

## Overview

Migrates all care recipient notes (both AFFILIATE and INTERNAL types) from the `care_recipient_notes` table to the `care_recipient_leads_notes` table at the lead level.

## What It Does

1. Fetches all notes for each care recipient (both AFFILIATE and INTERNAL types)
2. Creates individual note records in `care_recipient_leads_notes` for each note
3. Marks migrated notes with `mldmMigratedModmonAt` timestamp to prevent Hubspot sync

## Database Changes Required

The MM repo PR #5063 adds the required column to `care_recipient_leads_notes`:

```sql
ALTER TABLE care_recipient_leads_notes 
ADD COLUMN "mldmMigratedModmonAt" TIMESTAMP WITH TIME ZONE;

CREATE INDEX "IDX_crln_mldm_migrated_modmon_at_not_null" 
  ON care_recipient_leads_notes ("mldmMigratedModmonAt") 
  WHERE "mldmMigratedModmonAt" IS NOT NULL;
```

**Important:** The Hubspot sync adapter has been updated to exclude notes where `mldmMigratedModmonAt IS NOT NULL`, preventing migrated notes from syncing to Hubspot.

**You must wait for MM PR #5063 to be merged and deployed before running this migration.**

## Output Format

Each note is stored as a separate row with:
- `id`: New UUID
- `leadId`: Care recipient lead ID
- `value`: Formatted as `[TYPE] note text` (e.g., `[AFFILIATE] This is a note`)
- `creator`: `'MLDM Migration'`
- `createdAt`: Original note's creation date
- `updatedAt`: Original note's creation date  
- `mldmMigratedModmonAt`: Migration timestamp (NOW())

Example records:
```
id: abc-123, leadId: xyz-789, value: "[AFFILIATE] Great community, very responsive"
id: def-456, leadId: xyz-789, value: "[INTERNAL] Follow up needed next week"
```

### Benefits:
- **No size limit** - Each note stored separately (TEXT field, no truncation)
- **Queryable** - Can filter/search by note type, date, etc.
- **No Hubspot sync** - Migrated notes marked to exclude from sync
- **User notes still sync** - Only migrated notes are excluded
- **Scalable** - Can handle 1M+ notes without Hubspot rate limit issues

## Usage

### Time-Based Migration (Default)

Migrate all leads created in the last 2 years:
```bash
npm run migrate:care-recipient-lead-notes -- --from "2 years"
```

Migrate leads within a specific date range:
```bash
npm run migrate:care-recipient-lead-notes -- --from "2024-01-01" --to "2024-12-31"
```

### Care Seeker ID-Based Migration

Migrate notes for specific care seekers (using CSV file):
```bash
npm run migrate:care-recipient-lead-notes -- --ids ./care-seeker-ids.csv
```

Or inline:
```bash
npm run migrate:care-recipient-lead-notes -- --ids-inline "123456,789012,345678"
```

**CSV Format:**
```csv
legacyContactId
123456
789012
345678
```

Or simple list:
```
123456
789012
345678
```

### Dry Run

Test without making changes:
```bash
npm run migrate:care-recipient-lead-notes -- --ids ./care-seeker-ids.csv --dry-run
```

### Custom Batch Size

Process in smaller batches:
```bash
npm run migrate:care-recipient-lead-notes -- --from "2 years" --batch-size 500
```

## Migration State

Migration state is tracked in:
```
migration-state/care_recipient_lead_notes/
├── batches.jsonl          # Batch metadata
└── batch_XXXXX/
    └── rows.jsonl         # Individual record status
```

## Report

View migration progress:
```bash
npm run migrate:care-recipient-lead-notes -- --report
```

## Behavior

### Time-Based Mode
- Processes leads created within the specified date range
- Processes in batches (default: 1000 leads per batch)
- Skips leads that have no notes

### Care Seeker ID Mode
- Fetches all leads for the specified care seekers
- Only processes leads that exist in MM
- Creates individual note records for ALL notes from the care recipient

### Idempotency
- **NOT idempotent** - Running multiple times will create duplicate notes
- Only run once per set of leads
- Use migration state to track what's been processed

## Troubleshooting

### No notes found
- Lead's care recipient has no AFFILIATE or INTERNAL notes
- Lead will be skipped

### Duplicate notes
- If you run the migration twice on the same leads, you'll get duplicate notes
- Check migration state before re-running

### Performance
- Inserts 1000 notes per SQL batch
- For large datasets (1M+ notes), expect several hours of runtime
- No Hubspot impact since notes are marked to exclude from sync

## Testing

Run unit tests:
```bash
npm test src/migrations/care-recipient-lead-notes/care-recipient-lead-notes.test.ts
```

## Examples

### Example 1: Migrate All Recent Leads
```bash
npm run migrate:care-recipient-lead-notes -- --from "1 year"
```

### Example 2: Migrate Specific Care Seekers (DRY RUN)
```bash
npm run migrate:care-recipient-lead-notes -- --ids ./care-seekers.csv --dry-run
```

### Example 3: Migrate Specific Care Seekers (ACTUAL)
```bash
npm run migrate:care-recipient-lead-notes -- --ids ./care-seekers.csv
```

### Example 4: Check Migration Status
```bash
npm run migrate:care-recipient-lead-notes -- --report
```
