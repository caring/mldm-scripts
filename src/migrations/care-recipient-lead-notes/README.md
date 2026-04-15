# Care Recipient Lead Notes Migration

## Overview

**Incremental migration** that moves care recipient notes from the `care_recipient_notes` table to the `care_recipient_leads_notes` table at the lead level.

## What It Does (Incremental Approach)

1. **Finds NEW notes** created since last run (`createdAt > last_run_time`)
2. **Groups by care_recipient** to identify affected care recipients
3. **For each affected care_recipient:**
   - Fetches ALL leads for that care_recipient
   - Fetches ALL notes (old + new) for that care_recipient
   - For EACH lead:
     - **DELETES** existing migrated notes (where `mldmMigratedModmonAt IS NOT NULL`)
     - **INSERTS** ALL current notes (fresh aggregation)
4. **Marks notes** with `mldmMigratedModmonAt` timestamp to prevent Hubspot sync

### Benefits of Incremental Approach:
- ✅ **Efficient**: Only processes care_recipients with new notes
- ✅ **Idempotent**: Delete-then-insert ensures clean state
- ✅ **Incremental**: Can run daily/weekly to stay up-to-date
- ✅ **Safe**: Protects user-created notes (doesn't touch notes without `mldmMigratedModmonAt`)

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
- **Smart prioritization** - AFFILIATE notes first (typically 1), then INTERNAL notes (latest first)
- **3,000 char limit** - Per care recipient, covers ~85-90% of cases completely
- **No truncation per note** - Each note stored separately as full text (VARCHAR field)
- **Queryable** - Can filter/search by note type, date, etc.
- **No Hubspot sync** - Migrated notes marked to exclude from sync
- **User notes still sync** - Only migrated notes are excluded
- **Scalable** - Can handle 1M+ notes without Hubspot rate limit issues

### Character Limit Logic:
1. **AFFILIATE notes** fetched first (usually 1 note, ~300 chars)
2. **INTERNAL notes** fill remaining space up to 3,000 chars (latest first)
3. Result: ~10 notes average per care recipient (based on data analysis)

## Usage

### Incremental Migration (Recommended)

Migrate notes created since last run (incremental updates):

```bash
# First run: Process notes created in last 2 years
npm run migrate:care-recipient-lead-notes -- --from "2 years"

# Subsequent runs: Process notes created since last run
npm run migrate:care-recipient-lead-notes -- --from "2026-04-08T05:00:00.000Z"
```

The script will:
1. Find notes created after the `--from` date
2. Group by care_recipient_id to find affected care recipients
3. For each affected care_recipient:
   - Fetch ALL leads
   - DELETE existing migrated notes for those leads
   - INSERT ALL current notes (old + new)

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
migration-state/<environment>/care_recipient_lead_notes/
├── batches.jsonl          # Batch metadata
└── rows.jsonl             # Individual record status
```

Where `<environment>` is:
- `prod` when `ENVIRONMENT=prod`
- `stage` when `ENVIRONMENT=stage`

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
- ✅ **NOW IDEMPOTENT** - Delete-then-insert pattern ensures clean state
- ✅ Safe to run multiple times - produces same result
- ✅ Incremental approach - only processes care_recipients with new notes
- ✅ User notes protected - only deletes notes with `mldmMigratedModmonAt IS NOT NULL`

## Troubleshooting

### No notes found
- Lead's care recipient has no AFFILIATE or INTERNAL notes
- Lead will be skipped

### Duplicate notes
- ✅ **No longer an issue** - Delete-then-insert pattern prevents duplicates
- Running twice produces the same result (idempotent)

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
