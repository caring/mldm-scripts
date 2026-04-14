# Lead Status & Tour History Migration - All Modes

## Overview

This migration moves lead status and tour history from DIR (MySQL) `lead_statuses` table to MM (PostgreSQL) `care_recipient_leads` table as aggregated text summaries.

The migration now supports **THREE modes** to handle different use cases efficiently.

---

## Mode 1: Incremental Status Update (Recommended for Regular Updates)

**Use Case:** Daily/weekly updates to capture new statuses added since last run

### How It Works:
```
1. Finds leads with NEW statuses created since last run (created_at > last_run_time)
2. Fetches ALL statuses for those leads (old + new)
3. Re-aggregates status summary
4. Updates ONLY affected leads
```

### Command:
```bash
# Daily/weekly run - process statuses created in last week
npm run migrate:lead-status-tour-history -- --from "2026-04-08T05:00:00.000Z"

# Or use relative time
npm run migrate:lead-status-tour-history -- --from "7 days"
```

### Benefits:
- ✅ **Efficient**: Only processes leads with new statuses (e.g., 500 leads instead of 1.7M)
- ✅ **Always fresh**: Captures new statuses added after initial migration
- ✅ **Fast**: Typical run processes thousands of leads, not millions
- ✅ **Auto-detected**: If `--from` is < 60 days ago, automatically uses this mode

### Example Output:
```
=== Incremental Status Update Migration ===
Finding leads with new statuses created after: 2026-04-08T05:00:00.000Z
✓ Found 3,245 leads with new statuses
✓ Fetched 3,245 lead records from DIR
Processing batch_000001...
Total leads updated: 3,245
```

---

## Mode 2: Care Seeker-Based Migration (Targeted Updates)

**Use Case:** Process leads for specific care seekers (e.g., fix data for specific families)

### How It Works:
```
1. Takes care seeker IDs as input (from CSV)
2. Resolves: care_seekers → care_recipients → leads
3. Fetches ALL statuses for those leads
4. Updates ALL leads for those care seekers
```

### Command:
```bash
npm run migrate:lead-status-tour-history -- --ids ./care-seeker-ids.csv
```

### CSV Format:
```csv
12345
67890
11111
```
(One care seeker ID per line, no header)

### Benefits:
- ✅ **Targeted**: Only processes specific care seekers
- ✅ **Flexible**: Can fix data for specific families
- ✅ **Complete**: Processes ALL leads for those care seekers
- ✅ **Auto-detected**: If `--ids` is provided, automatically uses this mode

### Example Output:
```
=== Care Seeker-Based Migration ===
Processing 150 care seekers
✓ Found 1,234 leads for these care seekers
✓ Fetched 1,234 lead records from DIR
Total leads updated: 1,234
```

---

## Mode 3: Full Lead Migration (Initial Bulk Load)

**Use Case:** Initial migration or backfilling historical data

### How It Works:
```
1. Processes ALL leads created in a time range
2. Fetches statuses for those leads
3. Aggregates and updates
```

### Command:
```bash
# Process all leads created in last 2 years
npm run migrate:lead-status-tour-history -- --from "2 years"

# Process leads in specific date range
npm run migrate:lead-status-tour-history -- --from "2024-01-01" --to "2024-12-31"
```

### Benefits:
- ✅ **Complete**: Processes all leads in time range
- ✅ **Resumable**: Uses OFFSET pagination, can resume after crash
- ✅ **Backward compatible**: Same as original migration
- ✅ **Auto-detected**: If `--from` is > 60 days ago, automatically uses this mode

---

## Key Changes from Previous Version

### 1. Always Updates Summary (No COALESCE)

**Before:**
```sql
"legacyLeadStatusAndTourHistory" = COALESCE(v.summary, crl."legacyLeadStatusAndTourHistory")
-- Never updates if summary already exists
```

**After:**
```sql
"legacyLeadStatusAndTourHistory" = v.summary
-- Always updates with fresh data
```

### 2. Smart Mode Detection

The script automatically detects which mode to use:
- `--ids` provided → Care Seeker mode
- `--from` < 60 days → Incremental Status mode
- `--from` > 60 days → Full Lead mode

---

## Recommended Workflow

### Initial Setup (One Time):
```bash
# Migrate all leads from last 2 years
npm run migrate:lead-status-tour-history -- --from "2 years"
```

### Daily/Weekly Updates (Incremental):
```bash
# Process new statuses from last week
npm run migrate:lead-status-tour-history -- --from "2026-04-08T05:00:00.000Z"
```

### Ad-Hoc Fixes (Care Seeker-Based):
```bash
# Fix specific care seekers
npm run migrate:lead-status-tour-history -- --ids ./care-seeker-ids.csv
```

---

## Summary Format

Each lead's status history is aggregated into a text summary (max 2000 chars):

```
4/7/26 12:57pm - Tour scheduled, in person - Jordan Bennett
3/26/26 05:47pm - Status set as valid_lead - Provider
3/25/26 03:39pm - Tour completed - Contact
```

Additionally updates:
- `leadPriority`: HOT, Warm, or On Hold
- `pipelineStage`: Working
- `mldmMigratedModmonAt`: Timestamp of migration
