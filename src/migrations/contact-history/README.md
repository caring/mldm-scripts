# Contact History Migration

Migrates contact history from DIR (MySQL) to MM (PostgreSQL) for care recipients created in the last 2 years.

## Overview

This script aggregates contact history from 6 different DIR sources and projects them into 3 legacy fields on the MM `care_recipients` table:

- `legacyContactHistorySummary` - Compact text summary (max 1000 chars) of top 10 most recent events
- `legacyLastContactedAt` - Timestamp of most recent contact interaction
- `legacyLastDealSentAt` - Timestamp of most recent lead send (deal send)

## Source Tables (DIR)

1. **call_center_calls** - Consumer calls
2. **call_center_texts** - Text messages
3. **inquiries** - Inquiry creation (excluding AgentInquiry)
4. **inquiry_logs** - Contact merges and medical alerts
5. **formal_affirmations** - Consent affirmations
6. **local_resource_leads** - Lead sends to providers

## How It Works

1. **Fetch care recipients from DIR** - Get care recipients created in last 2 years
2. **For each care recipient:**
   - Fetch top 10 events from each of the 6 source tables (60 events max)
   - Combine and sort all events by timestamp (descending)
   - Take top 10 most recent events overall
   - Build summary string (max 1000 chars)
   - Extract timestamps (last contacted, last lead send)
3. **Update MM:**
   - Look up care recipient by `legacyId`
   - Skip if not found in MM or already migrated
   - Update the 3 legacy fields + set `mldmMigratedAt`

## Before Running Migration

**Get record counts first to estimate scope:**

See `quick-count.sql` for queries to run in DIR (MySQL) and MM (PostgreSQL).

## Usage

```bash
# Default: Process care recipients from last 2 years
npm run migrate:contact-history

# Process care recipients from last 2 days
npm run migrate:contact-history -- --from "2 days"

# Process care recipients from specific date
npm run migrate:contact-history -- --from "2024-01-01"

# Process care recipients from date range
npm run migrate:contact-history -- --from "2024-01-01" --to "2024-12-31"

# Dry run (don't actually update MM)
npm run migrate:contact-history -- --from "2 days" --dry-run

# Custom batch size
npm run migrate:contact-history -- --from "7 days" --batch-size 50

# Generate report
npm run migrate:contact-history -- --report
```

## CLI Options

- `--from <date>` - Start date (default: "2 years")
  - Relative: "2 days", "1 week", "3 months", "2 years"
  - Absolute: "2024-01-01"
- `--to <date>` - End date (optional, defaults to now)
- `--batch-size <number>` - Number of care recipients per batch (default: 1000)
- `--dry-run` - Run without updating MM (for testing)
- `--report` - Generate migration report
- `--help` - Show help

## Output Files

All progress is tracked in `migration-state/<environment>/contact_history/`:

- `batches.jsonl` - Batch processing log
- `rows.jsonl` - Individual care recipient results
- `summary.json` - Overall migration summary

Where `<environment>` is:
- `prod` when `ENVIRONMENT=prod`
- `stage` when `ENVIRONMENT=stage`

## Example Summary Output

```
[CALL] Outbound call - Qualified - 5m 23s - John Smith - Mar 20, 2024
[TEXT] Sent text - delivered - "Hi! We found some great options..." - Mar 18, 2024
[LEAD_SEND] BidderLead sent to Sunrise Senior Living - Assisted Living - sent_to_provider - Mar 15, 2024
[INQUIRY] ProviderInquiry - Assisted Living - San Francisco, CA - Mar 15, 2024
[CALL] Inbound call - Not interested - 2m 10s - Jane Doe - Mar 10, 2024
[TEXT] Received text - received - "Thanks, I'm interested in touring" - Mar 8, 2024
[FORMAL_AFFIRMATION] TCPA Consent - Consented - John Smith - Mar 5, 2024
[INQUIRY] SelfQualifiedInquiry - Memory Care - Seattle, WA - Feb 28, 2024
[CALL] Outbound call - Left voicemail - 0m 45s - Mike Johnson - Feb 25, 2024
[CONTACT_MERGE] Contact merge - Duplicate contacts merged - Feb 20, 2024
```

## Performance

- Fetches 10 events per table (6 tables) = 60 events max per care recipient
- Uses hashmaps to avoid looping over lists
- Processes in batches (default 1000 care recipients)
- Parallel queries for all 6 source tables
- Estimated runtime: Several hours for full historical data

## Notes

- Only processes care recipients created in DIR in last 2 years
- Skips care recipients not yet in MM
- Skips care recipients already migrated (unless `--force` is added in future)
- Uses `mldmMigratedAt` to track migration timestamp
- Summary is capped at 1000 characters
- Top 10 events are selected AFTER combining all sources (not 10 per table)

