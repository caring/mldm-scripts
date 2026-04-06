# Affiliate Notes Migration Guide

## Overview

The affiliate notes migration script now supports **two modes**:
1. **Time-based migration** - Migrate all notes created within a date range
2. **Lead-based migration** - Migrate notes for specific legacy lead IDs with automatic time window filtering

---

## Lead-Based Migration (NEW)

### Usage

```bash
# Using a CSV file with lead IDs (standard from business team)
npm run migrate:notes:affiliate -- --ids ./legacy-lead-ids.csv

# Using inline lead IDs
npm run migrate:notes:affiliate -- --ids-inline 58001234,58001235,58001236

# Dry run first (recommended)
npm run migrate:notes:affiliate -- --ids ./legacy-lead-ids.csv --dry-run

# Override time window (default: auto-detect from last run)
npm run migrate:notes:affiliate -- --ids ./legacy-lead-ids.csv --from 2024-04-15

# Specify exact time window
npm run migrate:notes:affiliate -- --ids ./legacy-lead-ids.csv --from 2024-04-15 --to 2024-05-01
```

### Input File Format

**CSV format - 1 lead ID per line (business team standard):**
```csv
58001234
58001235
58001236
58001237
```

**CSV with header (also supported, header ignored):**
```csv
legacyId
58001234
58001235
58001236
```

**Comments supported:**
```csv
# Production leads batch 1
58001234
58001235

# Production leads batch 2
58001236
58001237
```

**Note:** The file extension can be `.csv`, `.txt`, or any text file - the script reads it the same way (1 ID per line).

### How It Works

1. **Parse lead IDs** from `--ids` file or `--ids-inline` parameter
2. **Query DIR** to get `care_recipient_id` for each lead
3. **Verify care recipients exist in MM** (skip if not found)
4. **Determine time window:**
   - Auto-detect from last migration run, OR
   - Use `--from` flag if provided
5. **Fetch affiliate notes** for those care recipients within time window
6. **Diff detection:** Check which notes already exist in MM
7. **Migrate only new notes** (skip already-migrated)
8. **Process in batches** (default: 1000 notes per batch)

### Time Window Auto-Detection

- **First run:** Defaults to `2024-04-15` (business requirement)
- **Subsequent runs:** Uses timestamp from last successful migration
- **Override:** Use `--from` flag to specify explicit start date

```bash
# First run - will use default 2024-04-15
npm run migrate:notes:affiliate -- --ids ./leads.csv

# Second run (1 week later) - auto-detects from first run
npm run migrate:notes:affiliate -- --ids ./new-leads.csv

# Override auto-detection
npm run migrate:notes:affiliate -- --ids ./leads.csv --from 2024-05-01
```

### Diff Detection

The script automatically skips notes that have already been migrated:

- Queries MM for existing notes by `legacyId` (formatted_text_id)
- Filters out duplicates before inserting
- Reports: "X total notes, Y already migrated, Z new notes"
- **Safe to re-run** - won't create duplicates

---

## Time-Based Migration (Original Mode)

### Usage

```bash
# Migrate all notes from a specific date
npm run migrate:notes:affiliate -- --from 2024-01-01

# Migrate notes in a date range
npm run migrate:notes:affiliate -- --from 2024-01-01 --to 2024-12-31

# Dry run
npm run migrate:notes:affiliate -- --from 2024-01-01 --dry-run

# Custom batch size
npm run migrate:notes:affiliate -- --from 2024-01-01 --batch-size 500
```

### How It Works

1. **Parse time range** from `--from` and `--to` flags
2. **Fetch batches** from DIR using cursor-based pagination
3. **Process each batch** and insert into MM
4. **Resume capability:** Can resume from last completed batch if interrupted

---

## Examples

### Example 1: First-time migration for specific leads (Standard workflow)

```bash
# Business team provides 500 lead IDs in legacy-leads.csv (1 ID per line)
# Step 1: Do a dry run to see what will happen
npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv --dry-run

# Expected dry-run output:
# - Parsed 500 unique legacy lead IDs
# - Found 485 care recipients
# - Found 475 care recipients in MM
# - Fetched 1,250 affiliate notes in time window (from 2024-04-15 to now)
# - Already migrated: 850
# - New notes to migrate: 400

# Step 2: Review the dry run output
# - Check if the numbers make sense
# - Verify time window is correct
# - Ensure care recipient mapping is reasonable

# Step 3: If dry run looks good, execute the actual migration
npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv
```

### Example 2: Incremental migration (add more leads later)

```bash
# Week 1: Business provides 500 lead IDs - migrate them
npm run migrate:notes:affiliate -- --ids ./april-leads.csv

# Week 2: Business provides 200 more lead IDs
# Auto-detects time window from last run (only migrates notes created since Week 1)
npm run migrate:notes:affiliate -- --ids ./may-leads.csv

# Week 3: Re-run first batch to catch any newly created notes
# Diff detection will skip already-migrated notes (no duplicates)
npm run migrate:notes:affiliate -- --ids ./april-leads.csv
```

### Example 3: Override time window for specific date range

```bash
# Only migrate notes from June 2024 for these leads
npm run migrate:notes:affiliate -- \
  --ids ./june-leads.csv \
  --from 2024-06-01 \
  --to 2024-06-30
```

---

## Output & Reporting

### Lead-Based Migration Output

```
=== Resolving Legacy Lead IDs ===
Parsed 500 unique legacy lead IDs
Sample IDs: [58001234, 58001235, 58001236, 58001237, 58001238, ...]

Querying DIR for care_recipient_ids...
Found 485 care recipients for 500 leads

Verifying care recipients in MM...
Found 475 care recipients in MM out of 485

=== Lead-Based Migration ===
Input:
  Legacy lead IDs: 500
  Care recipients: 485
  Care recipients in MM: 475

Time window:
  From: 2024-04-15T00:00:00.000Z (auto-detected)
  To: 2026-05-01T23:59:59.999Z
  Range: 1 year, 16 days

Fetching affiliate notes from DIR...
Found 1,250 affiliate notes in time window

Checking for existing notes in MM (diff detection)...
Already migrated: 850

New notes to migrate: 400

=== Processing batch_000001 ===
Processing notes 1 to 400 of 400
✓ Batch completed

=== Lead-Based Migration Complete ===
Total notes found: 1,250
Already migrated: 850
New notes migrated: 400
```

---

## Migration State

- **State directory:** `migration-state/affiliate_notes/`
- **Files:**
  - `batches.jsonl` - Batch processing records
  - `rows.jsonl` - Individual row processing records
  - `summary.json` - Overall migration summary

---

## Troubleshooting

### No notes found

```
Found 0 affiliate notes in time window
Migration complete (nothing to migrate)
```

**Possible causes:**
- Time window is too narrow
- Leads don't have any affiliate notes
- Notes were created before the time window

**Solution:** Check `--from` date or remove it to use default (2024-04-15)

### Care recipients not found in MM

```
Found 0 care recipients in MM out of 485
```

**Cause:** Care recipients haven't been migrated to MM yet

**Solution:** Run care recipient migration first

### All notes already migrated

```
Already migrated: 1,250
New notes to migrate: 0
Migration complete
```

**Cause:** All notes for these leads have been migrated previously

**Solution:** This is normal - diff detection is working correctly
