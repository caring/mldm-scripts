# Lead Status and Tour History Migration Plan

## Overview

Migrate lead status and tour history from DIR (MySQL) `lead_statuses` table to MM (PostgreSQL) `care_recipient_leads` table as aggregated text summary.

**Pattern**: Single-field projection (same as Contact History migration)

---

## Source Data (DIR - MySQL)

### Table: `lead_statuses`
- **Parent**: `local_resource_leads` (via `local_resource_lead_id`)
- **Key Fields**:
  - `id`, `local_resource_lead_id`
  - `status`, `sub_status`
  - `tour_date`, `tour_time`
  - `created_by`, `source`
  - `notes` (provider notes)
  - `created_at`, `updated_at`

### Volume Statistics (Last 2 Years)
- **Total leads**: 1,705,419
- **Total statuses**: 2,671,697
- **Average statuses per lead**: 2.97
- **Max statuses per lead**: 230
- **Distribution**:
  - 55% of leads have 1 status
  - 9.8% have 0 statuses
  - Only 0.07% have >10 statuses

---

## Target Schema (MM - PostgreSQL)

### Table: `care_recipient_leads`

**New Columns to Add**:
```typescript
legacyLeadStatusAndTourHistory: varchar(2000)  // Aggregated summary
mldmMigratedAt: TIMESTAMP WITH TIME ZONE       // Migration timestamp
```

**Mapping**:
- DIR `local_resource_leads.id` → MM `care_recipient_leads.legacyId`

---

## Summary Format

### Example Output
```
9/17/25 06:46am - Tour cancelled, in person - Charmaine Guillen
9/1/25 10:18am - Tour scheduled, in person - Aidan Moloney
9/1/25 10:09am - Valid - Aidan Moloney
8/15/25 02:30pm - Status set as contacted - John Doe
9/17/25 06:46am - Tour cancelled, in person - Charmaine Guillen
9/1/25 10:18am - Tour scheduled, in person - Aidan Moloney
9/1/25 10:09am - Valid - Aidan Moloney
8/15/25 02:30pm - Status set as contacted - John Doe
```

### Format Rules
- **Date/Time**: `M/D/YY hh:mma` (e.g., `9/17/25 06:46am`)
- **Status Text**: Varies by status type (see formatting logic below)
- **Created By**: Agent name from `lead_statuses.created_by`
- **Separator**: ` - ` (space-dash-space)
- **Line Separator**: `\n` (newline)
- **Max Length**: 1000 characters
- **Truncation**: Top 10 most recent statuses only

---

## Status Formatting Logic

Based on `app/javascript/components/LeadStatusLabel.jsx`:

```typescript
function formatStatusLine(status: LeadStatus): string {
  const date = moment(status.created_at).format('M/D/YY hh:mma');
  const createdBy = status.created_by || 'Unknown';
  
  let statusText = '';
  
  if (status.status === 'tour_scheduled' && status.tour_date) {
    statusText = `Tour scheduled, ${status.sub_status || 'in person'}`;
  } else if (status.status === 'tour_completed') {
    statusText = 'Tour completed';
  } else if (status.status === 'tour_cancelled' || 
             (status.status === 'memo' && status.sub_status === 'tour_canceled')) {
    statusText = `Tour cancelled, ${status.sub_status || 'in person'}`;
  } else if (status.status === 'valid_lead') {
    statusText = 'Valid';
  } else {
    statusText = `Status set as ${status.status}`;
    if (status.sub_status) {
      statusText += `, ${status.sub_status}`;
    }
  }
  
  return `${date} - ${statusText} - ${createdBy}`;
}
```

---

## Migration Strategy: Two-Phase Approach

### Phase 1: Pre-fetch All Lead IDs (One-time)
```sql
-- Get ALL lead IDs from last 2 years (runs once at start)
SELECT id
FROM local_resource_leads
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
  AND deleted_at IS NULL
ORDER BY id ASC;
```

**Storage**:
- In-memory array: ~1.7M lead IDs (~14MB)
- Or JSONL file for resumability

### Phase 2: Process in Batches of 1000

**Per Batch**:
1. Take 1000 lead IDs from pre-fetched list
2. Fetch ALL statuses for these 1000 leads (1 query)
3. Group by lead_id using Map (in-memory)
4. Sort by created_at DESC (in-memory)
5. Take top 10 per lead (in-memory)
6. Format and aggregate to summary string
7. Fetch MM data to check which leads exist
8. Bulk update MM (1 query)

---

## SQL Queries

### Query 1: Fetch Statuses for Batch
```sql
-- Fetch ALL statuses for batch of 1000 leads
SELECT 
  local_resource_lead_id AS lead_id,
  created_at,
  status,
  sub_status,
  tour_date,
  tour_time,
  created_by,
  source
FROM lead_statuses
WHERE local_resource_lead_id IN (?, ?, ?, ...)  -- 1000 lead IDs
ORDER BY local_resource_lead_id, created_at DESC;
```

### Query 2: Fetch MM Data for Batch
```sql
-- Check which leads exist in MM
SELECT id, "legacyId", "mldmMigratedAt"
FROM care_recipient_leads
WHERE "legacyId" = ANY($1)
  AND "deletedAt" IS NULL;
```

### Query 3: Bulk Update MM
```sql
-- Bulk update using VALUES
UPDATE care_recipient_leads AS crl
SET
  "legacyLeadStatusAndTourHistory" = v.summary,
  "mldmMigratedAt" = NOW(),
  "updatedAt" = NOW()
FROM (VALUES 
  ($1, $2),
  ($3, $4),
  ...
) AS v(legacy_id, summary)
WHERE crl."legacyId" = v.legacy_id;
```

---

## Performance Estimates

- **Total leads**: 1.7M
- **Batch size**: 1000 leads
- **Total batches**: ~1,700
- **Queries per batch**: 3 (fetch statuses + fetch MM + bulk update)
- **Total queries**: ~5,100
- **Estimated time**: 30-60 minutes

---

## Files to Create

1. **Schema Migration**: `database/migrations/XXXXXX-add-legacy-lead-status-tour-history.ts`
2. **Entity Update**: `src/data/entities/crm/care-recipient-leads.entity.ts`
3. **Migration Script**: `src/migrations/lead-status-tour-history/lead-status-tour-history.ts`
4. **Tests**: `src/migrations/lead-status-tour-history/lead-status-tour-history.test.ts`
5. **README**: `src/migrations/lead-status-tour-history/README.md`
6. **Validation Queries**: `src/migrations/lead-status-tour-history/quick-count.sql`

---



SELECT 
  status_count,
  COUNT(*) AS num_leads,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM (
  SELECT 
    lrl.id,
    COUNT(ls.id) AS status_count
  FROM local_resource_leads lrl
  LEFT JOIN lead_statuses ls ON ls.local_resource_lead_id = lrl.id
  WHERE lrl.created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
    AND lrl.deleted_at IS NULL
  GROUP BY lrl.id
) lead_counts
GROUP BY status_count
ORDER BY status_count;

status_count|num_leads|percentage|
------------+---------+----------+
           0|   167884|      9.84|
           1|   943460|     55.32|
           2|   315876|     18.52|
           3|   150866|      8.85|
           4|    66128|      3.88|
           5|    29879|      1.75|
           6|    14584|      0.86|
           7|     7708|      0.45|
           8|     3948|      0.23|
           9|     2155|      0.13|
          10|     1141|      0.07|
          11|      616|      0.04|
          12|      369|      0.02|
          13|      193|      0.01|
          14|      145|      0.01|
          15|       84|      0.00|
          16|       80|      0.00|
          17|       92|      0.01|

## Next Steps

1. ✅ Confirm target table (`care_recipient_leads`)
2. ✅ Confirm DataBridge doesn't sync `lead_statuses`
3. ✅ Design aggregation strategy
4. ⏭️ Create schema migration
5. ⏭️ Update entity
6. ⏭️ Implement migration script
7. ⏭️ Test on sample data
8. ⏭️ Run full migration

