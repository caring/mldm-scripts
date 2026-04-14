# Business Team Input Requirements for MLDM

## What We Need From You

### CSV File with Legacy Lead IDs

**Format:** 1 legacy lead ID per line

**File name:** Any name (e.g., `legacy-leads.csv`, `april-leads.csv`, etc.)

**Content example:**
```csv
58001234
58001235
58001236
58001237
58001238
```

**Requirements:**
- One lead ID per line
- No header row needed (but okay if present - will be ignored if it's not a number)
- Plain text CSV file
- Comments allowed (lines starting with `#` will be ignored)

---

## What We Will Migrate

Using the lead IDs you provide, we will migrate the following data from **April 15, 2024** to **now**:

### 1. Affiliate Notes
- Notes associated with inquiries for these leads
- Only NEW notes (already-migrated notes will be skipped)
- Source: DIR `formatted_texts` → Destination: MM `care_recipient_notes`

### 2. Contact History
- Contact interaction summary
- Last contacted timestamp
- Last deal sent timestamp
- Source: DIR multiple tables → Destination: MM `care_recipients`

### 3. Tour and Status History
- Lead status changes
- Tour history
- Lead priority (Hot/Warm/Cold)
- Pipeline stage
- Source: DIR → Destination: MM `care_recipient_leads`

### 4. Inquiries
- Inquiry details (name, email, phone, etc.)
- UTM/marketing data
- Source information
- Source: DIR `inquiries` → Destination: MM `inquiries`

---

## Sample CSV File Formats (All Supported)

### Simple Format (Recommended)
```csv
58001234
58001235
58001236
```

### With Header
```csv
legacy_lead_id
58001234
58001235
58001236
```

### With Comments
```csv
# Batch 1 - April leads
58001234
58001235

# Batch 2 - May leads
58001236
58001237
```

---

## How to Send the File

1. Export lead IDs from your system as CSV
2. Ensure 1 ID per line
3. Send to engineering team via:
   - Slack
   - Email
   - Shared drive

---

## Timeline Expectations

Once we receive your CSV file:

1. **Day 1:** Run dry-run migrations to validate
   - We'll share statistics with you (how many notes, contacts, etc.)
   - Estimated time: 2-3 hours

2. **Day 1-2:** Execute actual migrations (if dry-run looks good)
   - Affiliate notes: ~1-2 hours
   - Tour/status history: ~2-3 hours
   - Inquiries: ~4-6 hours (first time, so extra validation)
   - Contact history: Waiting on code enhancement (~6-8 hours)

3. **Day 2-3:** Validation and reporting
   - Verify all data migrated correctly
   - Share migration summary with business team

**Total estimated time:** 2-3 business days after receiving CSV

---

## Questions?

Contact the engineering team if you need clarification on:
- Which lead IDs to include
- How to export from your system
- Any special cases or exceptions
