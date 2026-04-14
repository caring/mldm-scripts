# Re-running Lead Status & Tour History Migration

## Overview

The migration was previously run WITHOUT capturing account names. We now need to **re-run** it to include:
- ✅ Account names (e.g., "Aidan Moloney", "Jordan Bennett") 
- ✅ Proper source attribution (Provider, Contact, Caring Staff)

## What Changed

### Before (Old Query):
```sql
SELECT 
  local_resource_lead_id AS lead_id,
  created_at,
  status,
  sub_status,
  tour_date,
  tour_time,
  created_by  -- ❌ This was mostly empty/null
FROM lead_statuses
```

### After (New Query):
```sql
SELECT
  ls.local_resource_lead_id AS lead_id,
  ls.created_at,
  ls.status,
  ls.sub_status,
  ls.tour_date,
  ls.tour_time,
  ls.source,
  COALESCE(a.full_name, a.email) AS account_name  -- ✅ Gets actual names
FROM lead_statuses ls
LEFT JOIN accounts a ON a.id = ls.account_id
```

## Before Re-running: Preview Sample Data

### Option 1: Preview Specific Leads

```bash
ts-node src/migrations/lead-status-tour-history/preview-data.ts 57601684 57601685
```

This will show you:
- Current data in MM
- New data that will be written
- Full summary text preview
- Lead priority and pipeline stage

**Example output:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Lead ID: 57601684
Created: 2022-07-23T...

MM Status:
  ✓ Found in care_recipient_leads (id: abc-123...)
  Previously migrated: 2026-04-02T...
  Current summary length: 250 chars

New Values (to be written):
  leadPriority: HOT
  pipelineStage: Working
  Summary length: 280 chars

  Full Summary Text:
  ┌──────────────────────────────────────────────────┐
  │ 7/23/22 01:03pm - Tour scheduled, in person - Jordan Bennett │
  │ 7/23/22 01:02pm - Valid - Jordan Bennett        │
  │ 7/23/22 04:23pm - Valid - Provider              │
  └──────────────────────────────────────────────────┘
```

### Option 2: Dry-Run with Sample Batch

```bash
yarn migrate:lead-status-tour-history -- --from "3y" --batch-size 10 --dry-run
```

This will:
- Process 10 leads
- Show summary previews (first 3 lines of each)
- NOT write to database
- Show what would be updated

**Example output:**
```
  ✓ Lead 57601684: Prepared (3 statuses, leadPriority=HOT, pipelineStage=Working)
     Summary preview (180 chars):
     │ 7/23/22 01:03pm - Tour scheduled, in person - Jordan Bennett
     │ 7/23/22 01:02pm - Valid - Jordan Bennett
     │ 7/23/22 04:23pm - Valid - Provider

[DRY RUN] Would update 10 leads in care_recipient_leads
[DRY RUN] Fields to update: legacyLeadStatusAndTourHistory, leadPriority, pipelineStage, mldmMigratedModmonAt
```

## Re-running the Migration

### Strategy 1: Re-run ALL Leads (Recommended)

This will update ALL previously migrated leads with new account names:

```bash
# Dry run first to verify
yarn migrate:lead-status-tour-history -- --from "3y" --dry-run

# Actual run
yarn migrate:lead-status-tour-history -- --from "3y"
```

**Notes:**
- Uses `COALESCE` in the UPDATE query, so NULL summaries won't overwrite existing data
- Sets `mldmMigratedModmonAt = NOW()` to track when re-migration happened
- Processes in batches of 1000 (configurable with `--batch-size`)
- Resumes from where it left off if interrupted

### Strategy 2: Re-run Specific Leads

If you only want to update specific leads (e.g., those in a CSV from the business team):

```bash
# Using inline IDs
yarn migrate:lead-status-tour-history -- --ids-inline "57601684,57601685,57601686" --dry-run

# Using CSV file
yarn migrate:lead-status-tour-history -- --ids ./leads-to-update.csv --dry-run

# Actual run
yarn migrate:lead-status-tour-history -- --ids ./leads-to-update.csv
```

**CSV format:**
```csv
legacyId
57601684
57601685
57601686
```

## Verification After Re-run

### 1. Check Migration Summary

```bash
yarn migrate:lead-status-tour-history -- --report
```

Shows:
- Total leads processed
- Success/skipped/failed counts
- Batch completion status

### 2. Spot-Check in Database

```sql
-- Check a few leads to verify account names are present
SELECT 
  "legacyId",
  "legacyLeadStatusAndTourHistory",
  "leadPriority",
  "mldmMigratedModmonAt"
FROM care_recipient_leads
WHERE "legacyId" IN ('57601684', '57601685')
  AND "deletedAt" IS NULL;
```

Look for names like "Jordan Bennett", "Aidan Moloney" instead of just "Provider" or "Caring Staff".

### 3. Compare Before/After

Use the preview script on the same leads before and after:

```bash
# Before re-run
ts-node src/migrations/lead-status-tour-history/preview-data.ts 57601684 > before.txt

# After re-run
ts-node src/migrations/lead-status-tour-history/preview-data.ts 57601684 > after.txt

# Compare
diff before.txt after.txt
```

## Rollback (If Needed)

If something goes wrong, you can restore from backup:

```bash
# Backups are in migration-state/lead_status_tour_history_backup_YYYYMMDD_HHMMSS/
cp -r migration-state/lead_status_tour_history_backup_20260402_145135 migration-state/lead_status_tour_history
```

Or reset specific fields:

```sql
UPDATE care_recipient_leads
SET 
  "legacyLeadStatusAndTourHistory" = backup."legacyLeadStatusAndTourHistory",
  "mldmMigratedModmonAt" = backup."mldmMigratedModmonAt"
FROM backup_table backup
WHERE care_recipient_leads.id = backup.id;
```
