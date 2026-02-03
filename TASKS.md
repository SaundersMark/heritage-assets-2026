# Heritage Assets - Tasks

## Pending

<!-- Add tasks here for Claude Code to implement. Format:
### Task title
Description of what needs to be done.
-->
- Create a repo (including 2026 in the name) on github
## Future Ideas

<!-- Ideas not ready to implement yet. Can be rough notes, questions, or half-formed thoughts. -->
- There is an "owner_id" which is a number (e.g. 248234.9).  It is not possible to know who that owner actually is but I can see that there are different owner_id's associated with a particular location.  That is not always the case low.  Please investigate this and see whether there might be a way of grouping a collection of individual owner_ids together in a helpful way (e.g. Corsham Court, Corfe Castle).  This should be in a separate multi- owners_id to a single group id.  The group id should have a human readable name (like Corfe Castle).  Please investigate this and (underneath this) report back on suggestsions


---

## Done

<!-- Completed tasks are appended here with date. -->

- **2026-02-03**: Format empty strings on single line - Fixed "(empty string)" display to stay on one line instead of wrapping. Added `white-space: nowrap` and `align-items: baseline` to the details grid CSS.
- **2026-02-03**: Consistent row heights in details grid - Fixed inconsistent row heights between empty and non-empty values. Added explicit `line-height: 20px` and `min-height: 20px` to ensure uniform row heights.
- **2026-02-03**: Fix text selection collapsing expanded rows - Added check for `window.getSelection()` in click handler to prevent row collapse when user is selecting text to copy.
