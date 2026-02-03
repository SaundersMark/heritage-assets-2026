# Heritage Assets - Tasks

## Pending

<!-- Add tasks here for Claude Code to implement. Format:
### Task title
Description of what needs to be done.
-->
## Future Ideas

<!-- Ideas not ready to implement yet. Can be rough notes, questions, or half-formed thoughts. -->
- There is an "owner_id" which is a number (e.g. 248234.9).  It is not possible to know who that owner actually is but I can see that there are different owner_id's associated with a particular location.  That is not always the case low.  Please investigate this and see whether there might be a way of grouping a collection of individual owner_ids together in a helpful way (e.g. Corsham Court, Corfe Castle).  This should be in a separate multi- owners_id to a single group id.  The group id should have a human readable name (like Corfe Castle).  Please investigate this and (underneath this) report back on suggestions

  **Investigation findings (2026-02-03):**

  - 665 unique owner_ids across 36,167 assets (all owner_ids have multiple assets)
  - Some addresses are **solicitors/agents** (FARRER & CO, CURREY & CO LLP, FORSTERS LLP) who manage multiple estates across different locations - not useful for grouping collections
  - Some addresses are **actual estates/collections** - these are the real groupings:
    - CORSHAM COURT (534 assets, 2 owner_ids, Wiltshire)
    - HOLKHAM ESTATE OFFICE (422 assets, 1 owner_id, Norfolk)
    - MOUNT STUART (394 assets, Argyll & Bute)
    - HATFIELD HOUSE (210 assets)
    - HAREWOOD HOUSE TRUST (28 assets)
  - THE COMPTROLLER (3,901 assets) appears to be Royal collections
  - Combining **location + address_line1** gives better grouping than either alone (separates generic "THE ESTATE OFFICE" entries by region)

  **Suggested approach:**
  1. Create `collection_groups` table: group_id, name, description
  2. Create `owner_collection_map` table: owner_id → group_id
  3. Auto-detect potential groups using (location, address_line1) as initial grouping key
  4. Filter out solicitor/agent addresses (contain "LLP", "SOLICITOR", etc.)
  5. Allow manual curation - rename groups to friendly names, merge duplicates

  **Complexity:** The data is messy. Same estate can have multiple owner_ids (Corsham Court has 2). Generic addresses like "THE ESTATE OFFICE" appear in multiple locations. Would need manual review of the ~100 largest groups to assign proper names.


---

## Done

<!-- Completed tasks are appended here with date. -->

- **2026-02-03**: Format empty strings on single line - Fixed "(empty string)" display to stay on one line instead of wrapping. Added `white-space: nowrap` and `align-items: baseline` to the details grid CSS.
- **2026-02-03**: Consistent row heights in details grid - Fixed inconsistent row heights between empty and non-empty values. Added explicit `line-height: 20px` and `min-height: 20px` to ensure uniform row heights.
- **2026-02-03**: Fix text selection collapsing expanded rows - Added check for `window.getSelection()` in click handler to prevent row collapse when user is selecting text to copy.
- **2026-02-03**: Add HMRC link to browse page - Added "View on HMRC" link in details metadata section.
- **2026-02-03**: Capture Land & Buildings and Collections with undertakings - Discovered HMRC has three separate databases (Works of Art, Land & Buildings, Collections). Scraped 553 items (364 L&B, 189 Collections) with undertakings (legal access obligations). Created `LandBuilding` model, scraper (`scripts/scrape_land_buildings.py`), API endpoints (`/land-buildings`, `/land-buildings-stats`), and updated browse UI with data source toggle.
