## Talk Contact History migration approach

### Goal

Support the legacy Talk **Contact History** left-rail widget in MM without
creating a new history-events table.

### Decision

Instead of creating a new `care_recipient_call_history` table, we will add
legacy Talk projection fields directly to `care_recipients`:

- `legacy_contact_history_summary`
- `legacy_last_contacted_at`
- `legacy_last_lead_created_at`

This is intentionally a **legacy compatibility projection**, not a canonical MM
history model.

### Why this approach

- Talk Contact History is a **derived timeline**, not a single source table
- the left-rail widget only needs a compact summary, not a fully queryable
  event store
- a new table would be heavier than needed for this use case
- prefixing fields with `legacy_` makes it clear the data comes from the legacy
  Talk / DIR experience

### Source data in DIR

Talk `contact { history }` is built from `Contact::HistoryEvent.find_for_contact`
using these sources:

- `call_center_calls`
- `call_center_texts`
- `inquiries`
- `inquiry_logs`
- `formal_affirmations`

The DIR widget normalizes these sources into history events, sorts by timestamp,
and displays the recent items in the left rail.

### Mapping strategy

The projection will be stored at the **care recipient** level in MM.

Legacy mapping path:

- contact-scoped source row
- `contact_id` / `target_id`
- `contacts.id`
- `contacts.care_recipient_id`
- MM `care_recipients.legacyId`

### Validation results

The following source families were validated for `contact -> care_recipient`
mapping:

- `call_center_calls`
  - raw rows: `43,495,765`
  - missing contact: `1,400`
  - missing care recipient: `0`
- `call_center_texts`
  - raw rows: `2,315,168`
  - missing contact: `673`
  - missing care recipient: `0`
- `formal_affirmations`
  - raw rows: `231,212`
  - missing contact: `51`
  - missing care recipient: `0`
- `inquiries` excluding `AgentInquiry`
  - raw rows: `8,641,143`
  - missing contact: `10`
  - missing care recipient: `0`

Conclusion:

- these sources are safe to project onto `care_recipients`
- orphaned rows should be skipped and logged

`inquiry_logs` was not exhaustively validated because the table is very large
and the Talk-specific `message LIKE ...` filter is not indexed. The join path is
still expected to be compatible with the same care-recipient mapping approach.

### UI-driven constraint

The Talk left-rail widget shows up to **10 recent history items**.

To keep the summary compact enough for the widget, the plan is:

- store a compact summary string
- cap summary content at about **1000 chars max**
- build the summary from the most recent Talk-relevant events

This summary is for display compatibility only; it is not intended to preserve
full raw legacy event detail.

### Field intent

- `legacy_contact_history_summary`
  - compact text summary used for the Talk-style history preview
- `legacy_last_contacted_at`
  - timestamp representing the most recent contact-history interaction used for
    freshness / ordering in the legacy compatibility flow
- `legacy_last_lead_created_at`
  - timestamp for the most recent lead creation associated with the care
    recipient in the legacy compatibility flow

### Migration policy

- aggregate Talk-relevant history sources by legacy care recipient mapping
- sort by event timestamp descending
- build a compact summary from the recent events
- write summary + timestamps to the three `legacy_` fields on
  `care_recipients`
- skip and log orphaned source rows that cannot map to a contact / care
  recipient

### Final recommendation

Use `care_recipients` as the storage location for a **legacy Talk Contact
History projection** via:

- `legacy_contact_history_summary`
- `legacy_last_contacted_at`
- `legacy_last_lead_created_at`

This keeps the solution simple, explicitly legacy-scoped, and aligned with the
actual needs of the Talk left-rail widget.