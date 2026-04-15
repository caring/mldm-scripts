# MLDM Migration Status

**Last Updated:** April 6, 2026  
**Epic:** [CARE-1721](https://caring.atlassian.net/browse/CARE-1721)  
**Related Ticket:** [CARE-1726](https://caring.atlassian.net/browse/CARE-1726)

---

## Overview

This document tracks the progress of Mass Legacy Data Migration (MLDM) from legacy DIR (MySQL) to Modular Monolith (PostgreSQL).

---

## ✅ Completed Migrations

### 1. Notes Migration

**Destination Table:** `care_recipient_notes` (MM PostgreSQL)

#### Affiliate Notes (`affiliate-notes`)
- **Status:** ✅ Completed (Partially - only affiliate notes)
- **Script Location:** `src/migrations/notes/affiliate-notes.ts`
- **Source:** DIR `inquiries` table (affiliate note rows from legacy inquiry formatted text storage)
- **Destination:** MM `care_recipient_notes` table
- **Mapping:** `inquiries.contact_id` → `contacts.care_recipient_id`
- **Features:**
  - Deduplication by `(care_recipient_id, normalized note text)`
  - Skips 4 rows with no care recipient mapping
  - Uses batch processing with state tracking
  - Supports retry-failed mode

#### Self-Qualified Notes
- **Status:** 📦 Archived (intentionally not run)
- **Script Location:** `src/migrations/notes/self-qualified-notes.ts` (kept for reference only)
- **Source:** DIR `contacts.self_qualified_notes` table
- **Reason Not Migrated:** Old, legacy data deemed not valuable for migration

#### Internal Notes (Care Recipient Notes)
- **Status:** ✅ Already Migrated via Data Bridge
- **Migration Method:** Continuous real-time sync via Data Bridge (not one-time MLDM migration)
- **Source:** DIR Snowflake `CARE_RECIPIENT_NOTES` table
- **Destination:** MM `care_recipient_notes` table with `source = 'internal_notes'`
- **Transformer:** `CareRecipientNotesTransformer`
  - Location: `src/modules/data-bridge/transformers/care-recipients-notes-transformer.ts`
  - Entity Type: `EntityTransformerEnum.CARE_RECIPIENT_NOTES`
- **Fields Migrated:**
  - `legacyId` ← `care_recipient_note_id`
  - `value` ← note content
  - `careRecipientId` ← mapped from `legacy_care_recipient_id`
  - `source` ← hardcoded as `'internal_notes'`
  - `createdAt`, `updatedAt` ← from DIR
  - `deletedAt` ← set if `is_dir_deleted = true`
- **How It Works:**
  - Data Bridge continuously polls DIR Snowflake for updated/new care recipient notes
  - Transformer maps DIR notes to MM using `legacy_care_recipient_id` → `care_recipients.legacyId`
  - Creates new records or updates existing ones based on `care_recipient_note_id`
  - Unique constraint on `(legacyId, source)` ensures no duplicates
- **Important:** This is NOT part of MLDM - it runs continuously via Data Bridge cron jobs

---

### 2. Contact History Migration

**Destination Table:** `care_recipients` (MM PostgreSQL)

- **Status:** ✅ Completed (Time-based migration)
- **Script Location:** `src/migrations/contact-history/contact-history.ts`
- **Source:** DIR MySQL - 6 different source tables aggregated
- **Destination:** MM PostgreSQL `care_recipients` table
- **Fields Migrated:**
  - `legacyContactHistorySummary` - Compact text summary (max 1000 chars) of top 10 most recent events
  - `legacyLastContactedAt` - Timestamp of most recent contact interaction
  - `legacyLastDealSentAt` - Timestamp of most recent lead send (deal send)
- **Scope:** Care recipients created in the last 2 years
- **Migration Method:** Time-based batch processing
- **State Tracking:** `migration-state/contact_history/`

#### ⚠️ Missing Feature
- **Gap:** No script to migrate based on list of legacy lead IDs
- **Current:** Only supports time-based migration (date range)
- **Needed:** Add support for `--lead-ids` parameter to migrate specific leads by legacy ID list

---

### 3. Tour and Status History Migration

**Destination Table:** `care_recipient_leads` (MM PostgreSQL)

- **Status:** ✅ Completed
- **Script Location:** `src/migrations/lead-status-tour-history/lead-status-tour-history.ts`
- **Source:** DIR MySQL
- **Destination:** MM PostgreSQL `care_recipient_leads` table
- **Fields Migrated:**
  - `legacyLeadStatusAndTourHistory` - Tour and status change history summary
  - `leadPriority` - Lead priority (Hot, Warm, Cold)
  - `pipelineStage` - Current pipeline stage (Prospecting, Working, etc.)
  - `mldmMigratedModmonAt` - Migration timestamp
- **Migration Method:** Supports both time-based AND explicit lead ID list
- **State Tracking:** `migration-state/lead_status_tour_history/` (169 batches completed as of April 2, 2026)
- **Features:**
  - ✅ Can accept explicit list of legacy lead IDs via `--lead-ids` parameter
  - ✅ Batch processing with resumption support
  - ✅ Dry-run mode for testing

---

## 🆕 New Migrations

### Script 6: Care Recipient Notes Consolidation (`care-recipient-notes.ts`)
**Status:** ✅ READY - Consolidates to Single Text Column
**Script Location:** `src/migrations/care-recipient-notes/care-recipient-notes.ts`
**Purpose:** Consolidate all care_recipient_notes (AFFILIATE + INTERNAL) into single lead-level text field
**Destination:** `care_recipient_leads.legacyCareRecipientNotes` (VARCHAR 10000)

**Completed Work:**
- ✅ Consolidates all care_recipient_notes (AFFILIATE + INTERNAL) into single field
- ✅ Supports care seeker ID-based migration (`--ids` parameter)
- ✅ Supports time-based migration (default)
- ✅ Format: `[TYPE] DATE - NOTE TEXT` (newest first)
- ✅ Automatic truncation at 10,000 chars with message
- ✅ Full test coverage
- ✅ Documentation complete

**Database Change Required:**
```sql
ALTER TABLE care_recipient_leads
ADD COLUMN "legacyCareRecipientNotes" VARCHAR(10000) NULL;
```

**Use Case:** When you want a simple text summary of notes in the leads table itself.

---

### Script 7: Care Recipient Lead Notes Migration (`care-recipient-lead-notes.ts`)
**Status:** ✅ READY - Supports Care Seeker ID-Based Migration
**Script Location:** `src/migrations/care-recipient-notes/care-recipient-notes.ts`
**Purpose:** Migrate all care_recipient_notes (AFFILIATE + INTERNAL) to lead-level notes table
**Source:** MM `care_recipient_notes` table
**Destination:** MM `care_recipient_leads_notes` table

**Completed Work:**
- ✅ Migrates all care_recipient_notes (AFFILIATE + INTERNAL) as individual rows
- ✅ Supports care seeker ID-based migration (`--ids` parameter)
- ✅ Supports time-based migration (default)
- ✅ Marks migrated notes with `mldmMigratedModmonAt` to prevent Hubspot sync
- ✅ No size limits - each note stored separately (TEXT field)
- ✅ Full test coverage
- ✅ Documentation complete

**Database Changes Required:**
MM repo PR #5063 adds required column and sync exclusion:
```sql
ALTER TABLE care_recipient_leads_notes
ADD COLUMN "mldmMigratedModmonAt" TIMESTAMP WITH TIME ZONE;

CREATE INDEX "IDX_crln_mldm_migrated_modmon_at_not_null"
  ON care_recipient_leads_notes ("mldmMigratedModmonAt")
  WHERE "mldmMigratedModmonAt" IS NOT NULL;
```

Adapter updated to exclude migrated notes from Hubspot sync:
```typescript
.andWhere("crln.mldmMigratedModmonAt IS NULL")
```

**Ready for Execution:**
  1. Wait for MM PR #5063 to be merged and deployed
  2. Receive CSV file from business team with care seeker IDs
  3. Run dry-run: `npm run migrate:care-recipient-notes -- --ids ./care-seeker-ids.csv --dry-run`
  4. Review output for validation
  5. Run actual migration: `npm run migrate:care-recipient-notes -- --ids ./care-seeker-ids.csv`
  6. Verify notes inserted into `care_recipient_leads_notes` with `mldmMigratedModmonAt` set

**How It Works:**
1. Fetches all leads for care seekers (or by date range)
2. For each lead, gets all AFFILIATE + INTERNAL notes for that care recipient
3. Creates individual rows in `care_recipient_leads_notes` table:
   - `value`: `[TYPE] note text` (e.g., `[AFFILIATE] Great community`)
   - `creator`: `'MLDM Migration'`
   - `createdAt`: Original note's creation date
   - `mldmMigratedModmonAt`: NOW() (marks as migrated, excludes from Hubspot sync)
4. No truncation - all notes migrated completely

**Benefits:**
- **No data loss** - No truncation, unlimited note length
- **Proper data model** - One row per note (queryable, filterable)
- **No Hubspot impact** - Migrated notes marked to exclude from sync
- **User notes unaffected** - Only migrated notes excluded, user-created notes still sync
- **Scalable** - Can handle 1M+ notes without issues

**Data Statistics:**
- Average: ~5 notes per care recipient
- Average note length: ~293 characters
- Max note length: 122,674 characters (all fit without truncation)
- 95th percentile: 18 notes per care recipient

---

## 🚧 Pending Migrations

### 4. Inquiry Creation

**Destination Table:** `inquiries` (MM PostgreSQL)

- **Status:** ✅ ENHANCED - Now Supports Care Seeker ID-Based Migration
- **Script Location:** `src/migrations/inquiries/sync-inquiries.ts`
- **Source:** DIR MySQL `inquiries` table
- **Destination:** MM PostgreSQL `inquiries` table
- **Fields to Migrate:** All inquiry fields including:
  - Contact info: `firstName`, `lastName`, `email`, `phoneNumber`
  - Inquiry details: `inquiryFor`, `source`, `status`, `regionId`
  - Marketing data: UTM parameters, campaign data, GCLID, etc.
  - `mldmMigratedModmonAt` timestamp
- **Features:**
  - ✅ Supports care seeker ID-based migration (NEW)
  - ✅ Supports date range and explicit lead ID list migration
  - ✅ Optimal approach: starts from MM, filters to leads needing inquiries
  - Maps legacy inquiry IDs to care recipient leads
  - Batch processing with state tracking
  - Can create or update inquiries in MM

**Ready for Execution (Care Seeker ID Mode):**
  1. Receive CSV file from business team with **legacy care seeker IDs** (contact IDs)
  2. Run dry-run: `npm run migrate:inquiries -- --ids ./care-seeker-ids.csv --dry-run`
  3. Review output for validation
  4. Run actual migration: `npm run migrate:inquiries -- --ids ./care-seeker-ids.csv`
  5. Verify:
     - Inquiries created in MM `inquiries` table
     - `care_recipient_leads.inquiryId` updated correctly

**How It Works (Care Seeker ID Mode):**
1. Query MM for leads WHERE:
   - `care_seekers.legacyId` IN (input care seeker IDs)
   - `care_recipient_leads.legacyId` IS NOT NULL (has legacy mapping)
   - `care_recipient_leads.inquiryId` IS NULL (missing inquiry)
2. Get inquiry IDs from DIR for those leads
3. Fetch full inquiry data from DIR
4. Create inquiries in MM
5. Update `care_recipient_leads.inquiryId` mappings

**Optimizations:**
- Only processes leads that actually NEED inquiries (inquiryId IS NULL)
- Only processes leads that EXIST in MM
- Minimal data transfer between databases inquiries
- **Migration State Directory:** `migration-state/inquiries/` (ready to use when migration runs)

---

## 🎯 Business Requirement

**Timeline:** Migrate all data from **April 15, 2024** to **now**
**Input:** Business team will provide a list of legacy lead IDs
**Goal:** Ensure all data for these leads exists in MM database

---

## 📝 Work Pending Per Script

### Script 1: Affiliate Notes (`affiliate-notes.ts`)
**Status:** ✅ ENHANCED - Now Supports Lead-Based Migration
**Completed Work:**
- ✅ Added `--ids` and `--ids-inline` parameter support
- ✅ Implemented time window filtering (auto-detect from last run or `--from` override)
- ✅ Added diff detection (only migrates new notes, skips already-migrated)
- ✅ Added lead → care_recipient resolution from DIR
- ✅ Tracks lead_ids_count and lead_ids_sample in batch metadata
**Ready for Execution:**
  1. Receive CSV file from business team with legacy lead IDs (1 ID per line)
  2. Run dry-run: `npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv --dry-run`
  3. Review output for validation
  4. Run actual migration: `npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv`
  5. Time window auto-detects from last run (or use `--from 2024-04-15` to override)

---

### Script 2: Contact History (`contact-history.ts`)
**Status:** ✅ ENHANCED - Now Supports Care Seeker ID-Based Migration
**Completed Work:**
- ✅ Added `--ids` and `--ids-inline` parameter support for care seeker IDs
- ✅ Implemented smart timestamp recalculation (ALWAYS updated)
- ✅ Implemented conditional summary rebuild (only if new events since last run)
- ✅ Added care seeker ID → care_recipient resolution from DIR
- ✅ Checks for new events across all 6 source tables
- ✅ Efficient: Only rebuilds summaries when needed
**Ready for Execution:**
  1. Receive CSV file from business team with **legacy care seeker IDs** (contact IDs, 1 ID per line)
  2. Run dry-run: `npm run migrate:contact-history -- --ids ./care-seeker-ids.csv --dry-run`
  3. Review output for validation
  4. Run actual migration: `npm run migrate:contact-history -- --ids ./care-seeker-ids.csv`
  5. Verify all 3 fields are updated:
     - `legacyLastContactedAt` (always updated)
     - `legacyLastDealSentAt` (always updated)
     - `legacyContactHistorySummary` (rebuilt only if new events)

**Input Format:**
- **Care Seeker IDs** = Legacy Contact IDs from DIR (`contacts.id`)
- These map to `care_seekers.legacyId` in MM (synced by Data Bridge)
- Script resolves: contact.id → contact.care_recipient_id → care_recipients in MM

**Example CSV:**
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

---

### Script 3: Tour & Status History (`lead-status-tour-history.ts`)
**Status:** ✅ Already Run AND ✅ Supports `--lead-ids`
**Pending Work:**
- ✅ **READY TO USE:** Script already supports legacy lead ID list
- **Action Required:**
  1. Receive CSV file from business team with legacy lead IDs (1 ID per line)
  2. Run in dry-run mode: `npm run migrate:lead-status-tour-history -- --ids ./legacy-leads.csv --dry-run`
  3. Review output for any skipped/failed records
  4. Run actual migration: `npm run migrate:lead-status-tour-history -- --ids ./legacy-leads.csv`
  5. Verify all 4 fields are populated:
     - `legacyLeadStatusAndTourHistory`
     - `leadPriority`
     - `pipelineStage`
     - `mldmMigratedModmonAt`
- **Estimated Effort:** 2-3 hours (preparation + execution + validation)
- **No Code Changes Needed** ✅

---

### Script 4: Inquiries (`sync-inquiries.ts`)
**Status:** ⏸️ Script Created but NEVER Run
**Pending Work:**
- ✅ **READY TO USE:** Script already supports `--ids` parameter
- ⚠️ **NEVER EXECUTED:** This migration has never been run in production
- **Action Required:**
  1. Receive CSV file from business team with legacy lead IDs (1 ID per line)
  2. **CRITICAL:** Run in dry-run mode first to validate: `npm run migrate:inquiries -- --ids ./legacy-leads.csv --dry-run`
  3. Review logs for:
     - How many inquiries will be created vs updated
     - Any mapping issues (lead → inquiry)
     - Skipped records and reasons
  4. Run actual migration: `npm run migrate:inquiries -- --ids ./legacy-leads.csv`
  5. Validate inquiry records created with all fields including:
     - Contact info (firstName, lastName, email, phoneNumber)
     - UTM/marketing data
     - `mldmMigratedModmonAt` timestamp
  6. Consider running time-based migration as backup: `--from 2024-04-15`
- **Estimated Effort:** 4-6 hours (dry-run analysis + execution + validation + potential fixes)
- **No Code Changes Needed** ✅

---

## 📋 Summary: Work Required

| Script | Code Changes Needed? | Estimated Effort | Priority | Blocker? |
|--------|---------------------|------------------|----------|----------|
| Affiliate Notes | ✅ **DONE** - Lead-based support added | 1-2 hours (execution only) | High | No |
| Contact History | ✅ **DONE** - Lead-based support added | 1-2 hours (execution only) | High | No |
| Tour/Status History | ✅ No - Ready to execute | 2-3 hours | High | No |
| Inquiries | ✅ No - Ready to execute | 4-6 hours | High | No |
| **TOTAL** | **ALL SCRIPTS READY** | **8-13 hours** | | |

---

## 🚨 Critical Path to Completion

### Phase 1: Code Enhancements ✅ 100% COMPLETE
1. ~~**Affiliate Notes** - Add `--ids` parameter support~~ ✅ **DONE**
2. ~~**Contact History** - Add `--ids` parameter support~~ ✅ **DONE**

### Phase 2: Execution (8-13 hours) ✅ ALL READY
3. **Affiliate Notes** - Execute with business team's lead ID list (1-2 hours) ✅ **READY**
4. **Contact History** - Execute with business team's lead ID list (1-2 hours) ✅ **READY**
5. **Tour/Status History** - Execute with business team's lead ID list (2-3 hours) ✅ **READY**
6. **Inquiries** - Dry-run, validate, and execute (4-6 hours) ✅ **READY**

### Phase 3: Validation (4 hours)
8. Verify all data migrated correctly for sample legacy lead IDs
9. Check all `mldmMigratedModmonAt` timestamps are set
10. Generate final migration report
11. Document any skipped/failed records with reasons

**Total Estimated Time:** ~~24-27 hours~~ → ~~13-19 hours~~ → **8-13 hours** (1-2 business days) - ✅ **100% CODE COMPLETE**

---

## 📝 Action Items & Pending Tasks

### High Priority

1. ~~**Enhance Contact History Script**~~ ✅ **COMPLETE**
   - ~~Add `--ids` parameter support to `contact-history.ts`~~
   - ~~Implement smart timestamp recalculation (always updated)~~
   - ~~Implement conditional summary rebuild (only if new events)~~
   - ~~Check for new events across all 6 source tables~~

2. ~~**Enhance Affiliate Notes Script**~~ ✅ **COMPLETE**
   - ~~Add `--ids` parameter support to `affiliate-notes.ts`~~
   - ~~Add time window auto-detection with `--from` override~~
   - ~~Add diff detection for already-migrated notes~~
   - ~~Test with sample legacy lead IDs~~

3. **Execute All Migrations** ✅ **ALL READY - Waiting for CSV from business team**

   **Affiliate Notes:**
   - Receive CSV file from business team (1 legacy lead ID per line)
   - Run dry-run: `npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv --dry-run`
   - Review output and validate
   - Execute migration: `npm run migrate:notes:affiliate -- --ids ./legacy-leads.csv`
   - Validate results

   **Contact History:**
   - Use same CSV file from business team
   - Run dry-run: `npm run migrate:contact-history -- --ids ./legacy-leads.csv --dry-run`
   - Review and validate
   - Execute migration: `npm run migrate:contact-history -- --ids ./legacy-leads.csv`

   **Tour/Status History:**
   - Use same CSV file from business team
   - Run dry-run: `npm run migrate:lead-status-tour-history -- --ids ./legacy-leads.csv --dry-run`
   - Review and validate
   - Execute migration: `npm run migrate:lead-status-tour-history -- --ids ./legacy-leads.csv`

   **Inquiries (First Time!):**
   - Use same CSV file from business team
   - **CRITICAL:** Thorough dry-run first (never run before): `npm run migrate:inquiries -- --ids ./legacy-leads.csv --dry-run`
   - Review logs carefully
   - Execute migration: `npm run migrate:inquiries -- --ids ./legacy-leads.csv`
   - Validate carefully



### Medium Priority

5. **Validation & Testing**
   - Validate affiliate notes migration completeness
   - Verify contact history data accuracy
   - Test tour/status history migration with sample legacy IDs
   - Ensure all `mldmMigratedModmonAt` timestamps are set correctly

### Low Priority

6. **Documentation Updates**
   - Document why self-qualified notes were archived and not migrated
   - Add examples of using `--lead-ids` parameter for each migration script
   - Create runbook for inquiry migration execution

---

## 📊 Migration Scripts Summary

| Migration Type | Script Path | Status | Supports Lead ID List | State Directory |
|----------------|-------------|--------|----------------------|-----------------|
| Affiliate Notes | `src/migrations/notes/affiliate-notes.ts` | ✅ Done | ✅ **Yes** (enhanced) | `migration-state/affiliate_notes/` |
| Self-Qualified Notes | `src/migrations/notes/self-qualified-notes.ts` | 📦 Archived | ❌ No | N/A |
| Contact History | `src/migrations/contact-history/contact-history.ts` | ✅ Done | ✅ **Yes** (enhanced) | `migration-state/contact_history/` |
| Tour/Status History | `src/migrations/lead-status-tour-history/lead-status-tour-history.ts` | ✅ Done | ✅ Yes | `migration-state/lead_status_tour_history/` |
| Inquiries | `src/migrations/inquiries/sync-inquiries.ts` | ⏸️ Ready | ✅ Yes | `migration-state/inquiries/` |

---

## 🔧 Common Migration Parameters

All migration scripts support the following options:

- `--from <date>` - Start date for time-based migration (e.g., "2024-01-01")
- `--to <date>` - End date (optional, defaults to now)
- `--batch-size <number>` - Records per batch (default: 1000)
- `--dry-run` - Preview without making changes
- `--report` - Generate migration status report
- `--retry-failed` - Retry previously failed records
- `--ids <file>` - Migrate specific leads from CSV file (1 ID per line) - **Supported by: affiliate notes, tour/status, inquiries**
- `--ids-inline <ids>` - Migrate specific leads (comma-separated) - **Supported by: affiliate notes, tour/status, inquiries**

---

## 📁 Key Files & Directories

- **Migration Scripts:** `src/migrations/`
- **Migration State Tracking:** `migration-state/`
- **Utilities:** `src/utils/migration-cli.ts`, `src/utils/file-utils.ts`
- **Database Connections:** `src/db/mysql.ts`, `src/db/postgres.ts`
- **Ad-hoc SQL:** `database/ad-hoc/lead-notes-one-time-bidder-migration.sql`

---

## Notes

- All migrations use JSONL format for state tracking (`batches.jsonl`, `rows.jsonl`)
- Migration state allows for resumption after interruption
- Dry-run mode is recommended before any production migration
- Internal notes migration is handled by Data Bridge, not MLDM scripts
