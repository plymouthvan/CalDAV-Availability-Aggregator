# Changelog

## [2026-07-06] - Transparency Fixes & Batch Chunking

### Added
- **Google API Batch Chunking**: Added a `MAX_BATCH_SIZE = 50` limit to `batch_create_events` and `batch_delete_events` in `google_client.py`. Large sync payloads are now properly chunked into compliant Google batch requests with a 0.5s sleep between chunks to respect rate limits.
- **Silent Event Creation**: Appended `sendUpdates=none` to Google API create requests. This ensures that even if attendees were ever added to the projection, Google would not send unwanted email notifications during bulk syncs or self-healing phases.

### Fixed
- **Transparency (Busy/Free) Syncing**: 
  - *Bug*: CalDAV events (e.g., from Daylite) explicitly marked as "Free" (`TRANSPARENT`) were incorrectly appearing as "Busy" in Google Calendar.
  - *Cause*: The `WindowReconciler` relies on a flattened `ProjectedEvent` representation which dropped the `transparency` property during projection. Google's API defaults to `opaque` (Busy) when omitted. Additionally, transparency was excluded from `compute_hash()`, preventing any existing mismatched events from self-healing.
  - *Solution*: Added the `transparency` field to `ProjectedEvent`, explicitly mapped it to Google's `transparent`/`opaque` values in `to_google_event()`, and included it in the `compute_hash()` payload.

- **Recurring Events Deletion Regression (`NameError`)**:
  - *Bug*: Recurring events were completely removed from the production Google Calendar during the initial rollout of the transparency fix.
  - *Cause*: An invalid variable reference (`ev.transparency` instead of `master.transparency`) was introduced into `_project_recurring()`. A broad `try/except` block swallowed the resulting `NameError`, causing the projection step to silently drop the entire recurring series. The reconciler then assumed the series was deleted from the source and removed it from Google Calendar.
  - *Solution*: Corrected the variable references to use `master.transparency` for recurring masters and `ev.transparency` for single instances/exceptions, restoring the projection of recurring series.

- **Endless Churn Loop on Transparent Events (Field Mask Bug)**:
  - *Bug*: After fixing the recurring event variable names, transparent events entered an endless create/delete loop, never achieving a steady synced state.
  - *Cause*: The `WindowReconciler` compares desired event hashes against observed hashes recomputed from Google Calendar. However, the `list_events_window` API call used a strict `fields` mask (`items(id, recurringEventId, ..., extendedProperties)`) that *omitted* the `transparency` field. Consequently, the API response never included transparency, causing the observed hash to incorrectly fall back to `OPAQUE`. This resulted in a permanent mismatch against the desired `TRANSPARENT` hash, triggering constant replacement logic.
  - *Solution*: Added `transparency` to the Google API `fields` mask in `google_client.py` so the reconciler can accurately read back and hash the true state of transparency.
