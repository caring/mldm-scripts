# Summary

There are 4 scripts as of today, and the one-time migration has already been done for each of them.

The scripts are:

1. Notes
   - affiliate-notes
   - lead-notes (`care-recipient-lead-notes`)
2. Contact History
3. Lead Status and Tour History
4. Inquiries

## Incremental vs ID-based

- **Incremental migration** is time-bound and should process only new/changed records since the last run.
- **ID-based migration** is **not time-bound**.
- For ID-based runs, the input is a list of **care seeker IDs** and the script should treat that as a targeted backfill/rebuild for those families only.

## Brief logic for each ID-based migration

### 1. Affiliate Notes

Given a `care-seeker-id`:

1. Find MM `care_recipients` for that `care_seeker` using `care_seekers.legacyId -> care_recipients`.
2. Use the MM care recipient `legacyId` values as DIR `care_recipient_id`s.
3. In DIR, fetch all affiliate notes from `formatted_texts` joined through `inquiries` and `contacts`, scoped to those `care_recipient_id`s.
4. For each note:
   - map DIR `care_recipient_id` -> MM `careRecipientId`
   - map DIR `account_id` -> MM `agentAccountId` when possible
   - insert one row into MM `care_recipient_notes` with source = `affiliate_notes`
5. Skip rows where the MM care recipient does not exist; avoid duplicates using `(legacyId, source)`.

**Net effect:** for a given care seeker, we backfill the raw affiliate notes at the **care recipient note** level.

### 2. Lead Notes (`care-recipient-lead-notes`)

Given a `care-seeker-id`:

1. In MM, resolve `care_seeker -> care_recipients -> care_recipient_leads`.
2. For the affected MM `careRecipientId`s, fetch all relevant MM notes from `care_recipient_notes`:
   - all `affiliate_notes`
   - latest internal notes (top 20 per care recipient)
3. Build one concatenated lead-note payload per lead:
   - prefix each note with its type
   - keep affiliate notes first
   - include internal notes after that
   - cap the final payload to the max allowed length
4. Insert one row per MM lead into `care_recipient_leads_notes`.

**Net effect:** for a given care seeker, every lead under that family gets a regenerated **lead-level notes projection** from the care recipient notes.

### 3. Contact History

Given a `care-seeker-id` / legacy `contact.id`:

1. Split the provided IDs into fixed batches of **1000**.
2. For each batch, resolve `contacts.id -> contacts.care_recipient_id` in DIR.
3. Match those DIR care recipients to MM `care_recipients` using `legacyId`.
4. For each matched care recipient, fetch contact-history events from all 6 DIR sources:
   - calls
   - texts
   - inquiries
   - inquiry logs
   - formal affirmations
   - lead sends
5. Classify each requested input ID into:
   - `done` = already migrated and no new events after `mldmMigratedModmonAt`
   - `not_done` = MM row exists but was never migrated
   - `needs_refresh` = already migrated but new events exist after `mldmMigratedModmonAt`
   - `not_in_mm` / `no_events` / `not_found` = report-only skip buckets
6. Write a **timestamped classification file** for that batch under migration state.
7. In the same batch, process only actionable rows:
   - `not_done` -> populate contact history
   - `needs_refresh` -> recompute and overwrite contact history
   - `done` -> skip
8. For actionable rows, rebuild from the **full summary event set** and bulk update MM fields:
   - `legacyContactHistorySummary`
   - `legacyLastContactedAt`
   - `legacyLastDealSentAt`
   - `mldmMigratedModmonAt`

**Net effect:** for a given list of care seeker IDs, contact history runs in **1000-ID batches**, writes a classification file first, and only updates care recipients that are `not_done` or `needs_refresh`.

### 4. Lead Status and Tour History

Given a `care-seeker-id`:

1. In MM, resolve `care_seeker -> care_recipients -> care_recipient_leads`.
2. Take the MM lead `legacyId`s and fetch the corresponding DIR `local_resource_leads`.
3. For those DIR lead IDs, fetch **all** status/tour rows from DIR `lead_statuses`.
4. Aggregate the statuses into the lead summary text and derive supporting lead fields like priority/stage.
5. Bulk update MM `care_recipient_leads` with:
   - `legacyLeadStatusAndTourHistory`
   - `leadPriority`
   - `pipelineStage`

**Net effect:** for a given care seeker, we rebuild the **lead-level status/tour summary** for all leads under that family.

for subsequent run think of classifciation (done/ not_done / needs_refresh)

### 5. Inquiries

Given a `care-seeker-id`:

1. In MM, resolve `care_seeker -> care_recipients -> care_recipient_leads`.
2. Filter to MM leads where:
   - `legacyId` exists
   - `inquiryId` is still `NULL`
3. In DIR, map `local_resource_leads.id -> inquiry_id` for those legacy lead IDs.
4. Fetch the full DIR inquiry rows for the mapped inquiry IDs.
5. In MM:
   - create or update the `inquiries` row
   - set `care_recipient_leads.inquiryId` to the MM inquiry UUID

**Net effect:** for a given care seeker, we backfill missing **lead -> inquiry links** and ensure the underlying MM inquiry record exists.

## Recommended handling rule for ID-based runs

For all ID-based migrations, the mental model should be:

- input = specific families to repair/backfill
- scope = all related records for those families
- no date filtering
- recompute or backfill only the target entities touched by those care seekers

That keeps ID-based migration useful for re-runs, production fixes, and support-driven cleanup without depending on timestamps.