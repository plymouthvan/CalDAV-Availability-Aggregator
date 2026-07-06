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

### Changed
- **Daylite Deletion Latency (`DAYLITE_SELF_HEAL_INTERVAL_HOURS`)**:
  - *Context*: Hard deletions from a Daylite source calendar could take up to 12 hours to propagate to the target Google Calendar. This was **pre-existing behavior**, not a regression from the transparency work. iCloud sources are unaffected because they perform a full snapshot + DB diff on every sync cycle.
  - *Cause*: Daylite's incremental `sync-token` responses do not reliably surface deletions as `404` entries, and the Daylite server does not support the efficient `calendar-query` REPORT (returns HTTP 400). Deletion detection therefore depends entirely on the "self-heal" fallback in `daylite.py::_find_deleted_events_fallback()`, which performs a full `sync-collection` baseline snapshot and diffs it against the database. Because that full-baseline fetch (~1,700 events) is comparatively heavy, it was interval-gated to run only once every 12 hours (`DAYLITE_SELF_HEAL_INTERVAL_HOURS`, default `12`) — a deliberate load/latency tradeoff, acceptable given that Daylite events are far more often *cancelled* (synced immediately as a status update) than hard-deleted.
  - *Solution*: Set `DAYLITE_SELF_HEAL_INTERVAL_HOURS=1` in the deployment `.env` to reduce deletion-propagation latency from ~12 hours to ~1 hour, at the cost of one extra `sync-collection` baseline REPORT per hour. This is a configuration change only; no Python code was modified. To match iCloud's per-cycle immediacy, this value could be lowered further, at proportionally higher API load.
  - *Note*: Verified in production — a stale deleted event (DB had 1707 UIDs vs. 1706 on the server) was detected by the self-heal audit and removed from Google Calendar within one cycle of the change, after which the projection converged cleanly (`desired_keys: 68, google_keys: 68`).
