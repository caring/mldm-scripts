## Talk UI widget data mapping

Reference screenshot:

![Talk UI highlighted widgets](<Screenshot 2026-03-10 at 1.47.09 PM.png>)

The screenshot above highlights the left-rail **Notes**, **Contact History**, and **Internal Notes** areas in Talk. The 4th widget below is the Talk **Lead / Advisor Messages** experience, which is where `lead_notes` are used.

| UI widget | Talk GraphQL usage | DIR GraphQL field / mutation | DIR table(s) | Aggregation / resolution logic | Centered around | Migration status |
| --- | --- | --- | --- | --- | --- | --- |
| Notes | `TalkFamilyAdvisorToolQuery` → `contact { affiliateNotes selfQualifiedNotes }` | `Contact.affiliateNotes`, `Contact.selfQualifiedNotes` | `inquiries.affiliate_notes`, `contacts.self_qualified_notes` | `affiliateNotes`: iterate over `contact.inquiries`, read `affiliate_notes`, remove blanks, `uniq`; `selfQualifiedNotes`: direct read from `contacts.self_qualified_notes`; Talk UI concatenates both lists for display | Contact / Inquiry | **Affiliate notes migrated; selfQualifiedNotes archived** |
| Contact History | `TalkFamilyAdvisorToolQuery` → `contact { history }` | `Contact.history` | `call_center_calls`, `call_center_texts`, `inquiries`, `inquiry_logs`, `formal_affirmations` | DIR runs `Contact::HistoryEvent.find_for_contact(contact)`, loads events from all 5 sources, sorts by timestamp, maps them into normalized `ContactHistoryEvent` rows, and returns latest results | Contact | **To be migrated** |
| Internal Notes | Talk internal notes query / component → `contact { careRecipient { internalNotes } }`; write via internal note flow | `CareRecipient.internalNotes`; write: `createCareRecipientInternalNote` | `care_recipient_notes` | Read: load `CareRecipientNote.latest` by `care_recipient_id` ordered newest first; Write: create a new `care_recipient_note` for the current care recipient and account | Care Recipient | **Already migrated** |
| Lead / Advisor Messages | Messaging and lead widgets use `localResourceLead { advisorMessages }` (Messaging Page, Saved Lead note popover, ProviderTool advisor messages) | `LocalResourceLead.advisorMessages`; writes via `familyAdvisorAddLeadNote`, `familyAdvisorAddHousingNote`, `familyAdvisorAddHomecareNote`, `familyAdvisorAddUpdateNote` | `lead_notes` | Read lead notes for a given `local_resource_lead`; UI displays them as advisor/sent messages; this is the main Talk surface that is truly lead-centered | Local Resource Lead | **To be migrated** |

## Key takeaway

- **Notes** is not `lead_notes`; it is a **contact / inquiry** combination.
- **Contact History** is not a single table; it is a **contact-centered aggregate timeline**.
- **Internal Notes** is **care-recipient-centered** and uses `care_recipient_notes`.
- **Lead / Advisor Messages** is the Talk widget family that uses **`lead_notes`** and is **local-resource-lead-centered**.

## Migration implication

If the migration scope is the 3 highlighted left-rail widgets, then:

- `lead_notes` is **not** the backing source for **Notes**, **Contact History**, or **Internal Notes**
- `lead_notes` is relevant only for the **Lead / Advisor Messages** experience in Talk