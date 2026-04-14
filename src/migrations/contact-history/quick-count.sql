-- ============================================================================
-- Quick Count Query - Contact History Migration
-- ============================================================================
-- Run this to get a quick estimate of records to migrate
-- ============================================================================

-- ============================================================================
-- RUN IN DIR (MySQL)
-- ============================================================================

-- Total care recipients created in last 2 years
SELECT COUNT(*) as total_care_recipients_last_2_years
FROM care_recipients
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
  AND deleted_at IS NULL;

-- Breakdown by year
SELECT 
  YEAR(created_at) as year,
  MONTH(created_at) as month,
  COUNT(*) as count
FROM care_recipients
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 2 YEAR)
  AND deleted_at IS NULL
GROUP BY YEAR(created_at), MONTH(created_at)
ORDER BY year DESC, month DESC;


-- ============================================================================
-- RUN IN MM (PostgreSQL)
-- ============================================================================

-- Total care recipients in MM (not yet migrated)
SELECT COUNT(*) as total_ready_to_migrate
FROM care_recipients
WHERE "legacyId" IS NOT NULL
  AND "deletedAt" IS NULL
  AND "mldmMigratedAt" IS NULL;

-- Total care recipients in MM (already migrated)
SELECT COUNT(*) as total_already_migrated
FROM care_recipients
WHERE "legacyId" IS NOT NULL
  AND "deletedAt" IS NULL
  AND "mldmMigratedAt" IS NOT NULL;

-- Breakdown by creation date
SELECT 
  DATE_TRUNC('month', "createdAt") as month,
  COUNT(*) as count,
  SUM(CASE WHEN "mldmMigratedAt" IS NULL THEN 1 ELSE 0 END) as not_migrated,
  SUM(CASE WHEN "mldmMigratedAt" IS NOT NULL THEN 1 ELSE 0 END) as already_migrated
FROM care_recipients
WHERE "legacyId" IS NOT NULL
  AND "deletedAt" IS NULL
  AND "createdAt" >= NOW() - INTERVAL '2 years'
GROUP BY DATE_TRUNC('month', "createdAt")
ORDER BY month DESC;


-- ============================================================================
-- SAMPLE DATA (MM PostgreSQL)
-- ============================================================================

-- Sample 10 care recipients ready to migrate
SELECT 
  id,
  "legacyId",
  "createdAt",
  "updatedAt",
  "mldmMigratedAt",
  "legacyContactHistorySummary" IS NOT NULL as has_summary,
  "legacyLastContactedAt",
  "legacyLastDealSentAt"
FROM care_recipients
WHERE "legacyId" IS NOT NULL
  AND "deletedAt" IS NULL
  AND "mldmMigratedAt" IS NULL
ORDER BY "createdAt" DESC
LIMIT 10;

