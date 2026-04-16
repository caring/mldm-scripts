# affiliate-notes

npm run migrate:notes:affiliate -- --from 2026-03-23T00:00:00.000Z
npm run migrate:notes:affiliate -- --ids ./care-seeker-ids.csv --from "2 years"

# First run: Process notes from last 2 years
npm run migrate:care-recipient-lead-notes -- --from "2 years"

# Subsequent runs: Process notes created since last run
npm run migrate:care-recipient-lead-notes -- --from "2026-04-08T05:00:00.000Z"

# Dry run to test
npm run migrate:care-recipient-lead-notes -- --from "2026-04-08T05:00:00.000Z" --dry-run


---

# Week 1: Initial migration (1.7M leads)
npm run migrate:lead-status-tour-history -- --from "2 years"
# Result: Processes 1,700,000 leads

# Week 2: Incremental update (only new statuses)
npm run migrate:lead-status-tour-history -- --from "2026-04-08T05:00:00.000Z"
# Result: Processes 5,000 leads with new statuses

# Week 3: Incremental update
npm run migrate:lead-status-tour-history -- --from "2026-04-15T05:00:00.000Z"
# Result: Processes 3,200 leads with new statuses

# Ad-hoc: Fix specific care seekers
npm run migrate:lead-status-tour-history -- --ids ./care-seeker-ids.csv
# Result: Processes 1,234 leads for 150 care seekers


---

### Incremetnal run commands


dry run
```
npm run migrate:notes:affiliate -- --ids ./care-seeker-ids.csv --dry-run
npm run migrate:care-recipient-lead-notes -- --ids ./care-seeker-ids.csv --dry-run
npm run migrate:contact-history -- --ids ./care-seeker-ids.csv --dry-run
npm run migrate:lead-status-tour-history -- --ids ./care-seeker-ids.csv --dry-run
npm run migrate:inquiries -- --ids ./care-seeker-ids.csv --dry-run
```

```
npm run migrate:notes:affiliate -- --ids ./care-seeker-ids.csv
npm run migrate:care-recipient-lead-notes -- --ids ./care-seeker-ids.csv
npm run migrate:contact-history -- --ids ./care-seeker-ids.csv
npm run migrate:lead-status-tour-history -- --ids ./care-seeker-ids.csv
npm run migrate:inquiries -- --ids ./care-seeker-ids.csv
```

---

ENVIRONMENT=stage npm run migrate:notes:affiliate -- --from "2 years" --batch-size 10
ENVIRONMENT=stage npm run migrate:contact-history -- --from "2 years" --batch-size 10
ENVIRONMENT=stage npm run migrate:lead-status-tour-history -- --from "2 years" --batch-size 10
ENVIRONMENT=stage npm run migrate:inquiries -- --from "2 years" --batch-size 10
ENVIRONMENT=stage npm run migrate:care-recipient-lead-notes -- --from "2 years" --batch-size 10


ENVIRONMENT=prod npm run migrate:notes:affiliate -- --from "2 years" --batch-size 10
ENVIRONMENT=prod npm run migrate:contact-history -- --from "2 years" --batch-size 10
ENVIRONMENT=prod npm run migrate:lead-status-tour-history -- --from "2 years" --batch-size 10
ENVIRONMENT=prod npm run migrate:inquiries -- --from "2 years" --batch-size 10
ENVIRONMENT=prod npm run migrate:care-recipient-lead-notes -- --from "2 years" --batch-size 10

ENVIRONMENT=stage npm run migrate:notes:affiliate -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=stage npm run migrate:contact-history -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=stage npm run migrate:lead-status-tour-history -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=stage npm run migrate:inquiries -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=stage npm run migrate:care-recipient-lead-notes -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run


ENVIRONMENT=prod npm run migrate:notes:affiliate -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=prod npm run migrate:contact-history -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=prod npm run migrate:lead-status-tour-history -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=prod npm run migrate:inquiries -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run
ENVIRONMENT=prod npm run migrate:care-recipient-lead-notes -- --ids stage-care-seeker-ids.csv --batch-size 10 --dry-run