# CARE-1736 Care Recipient Notes Migration Decision

## Decision

We plan to move legacy **affiliate notes** and **self-qualified notes** into MM
`care_recipient_notes`.

This fits the target model because **internal notes are already moved to
`care_recipient_notes`**, so this gives us one recipient-level note store in MM.

## Source Mapping

### 1. Self-qualified notes

- Legacy source: `contacts.self_qualified_notes`
- Legacy relationship: `contacts.care_recipient_id -> care_recipients.id`
- Scope in DIR: contact-scoped, but directly mappable to care recipient

### 2. Affiliate notes

- Legacy storage: `formatted_texts`
- Relationship chain:
  - `formatted_texts.owner_type = 'Inquiry'`
  - `formatted_texts.name = 'affiliate_notes'`
  - `formatted_texts.owner_id = inquiries.id`
  - `inquiries.contact_id = contacts.id`
  - `contacts.care_recipient_id = care_recipients.id`
- Scope in DIR: inquiry/contact-scoped, but mappable to care recipient

## Why `care_recipient_notes`

- MM `inquiries` is not the destination of legacy DIR inquiries via data bridge
- MM `inquiries.notes` and `inquiries.affiliateNotes` are populated through
  current application flows, not legacy inquiry migration
- `care_recipient_notes` is already the canonical recipient-level note table
- internal notes already land in `care_recipient_notes`
- this gives a single merged note destination for recipient-facing notes

## Why not `inquiries`

- there is no confirmed legacy inquiry data-bridge path into MM `inquiries`
- using `inquiries` would mix migrated legacy note history with current inquiry
  ingestion / HubSpot sync fields
- affiliate notes in DIR are not originally one stable MM inquiry record per row

## Validation Results

### Self-qualified notes

Validation query:

- non-blank `contacts.self_qualified_notes` with `care_recipient_id IS NULL`

Result:

- `0`

Conclusion:

- self-qualified notes are safe to move to `care_recipient_notes`

### Affiliate notes

Validation queries showed:

- affiliate notes with missing `care_recipient_id`: `4`
- care recipients with notes coming from more than one contact: `2`
- raw affiliate note rows: `96,563`
- deduped rows by `(care_recipient_id, TRIM(note))`: `85,031`

Conclusion:

- affiliate notes are safe to move to `care_recipient_notes`
- there is a very small unmappable exception set
- dedupe should happen at the care-recipient level

## Migration Policy

### Self-qualified notes

- move all non-blank rows from `contacts.self_qualified_notes`
- map through `contacts.care_recipient_id`
- insert into `care_recipient_notes`

### Affiliate notes

- move non-blank affiliate note rows from legacy inquiry formatted text storage
- map through `inquiries.contact_id -> contacts.care_recipient_id`
- insert into `care_recipient_notes`
- dedupe by `(care_recipient_id, normalized note text)`
- skip and log the `4` rows with no care recipient mapping

## Final Recommendation

Use **`care_recipient_notes`** as the MM destination for:

- legacy affiliate notes
- legacy self-qualified notes

This is the cleanest option because it aligns with the MM recipient-level note
model and with the existing migration direction for internal notes.