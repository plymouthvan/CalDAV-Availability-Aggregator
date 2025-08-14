"""
Reconciliation module for CalDAV Mirror

Compares the state of the local database with the state of Google Calendar
and generates the necessary operations to make Google Calendar a mirror of
the database.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
from dateutil import rrule
import pytz

from .google_client import GoogleClient
from database import Database
from .event_model import EventModel

logger = logging.getLogger(__name__)

class Reconciler:
    """
    Orchestrates the reconciliation between the local database and Google
    Calendar for a specific source, with full support for recurring events.
    """

    def __init__(self, google_client: GoogleClient, database: Database):
        self.google = google_client
        self.db = database

    # Diagnostic helper: verify that a recurrence_id maps to a generated instance
    # of the master event's RRULE (with its current anchor and timezone).
    def _rid_valid_against_master(self, master: EventModel, rid: Optional[str]) -> Tuple[bool, str]:
        try:
            tzname = master.timezone or 'UTC'
            try:
                event_tz = pytz.timezone(tzname)
            except Exception:
                event_tz = pytz.UTC

            # Resolve master dtstart
            if master.start_datetime:
                dtstart = master.start_datetime
                if dtstart.tzinfo:
                    dtstart = dtstart.astimezone(event_tz)
                else:
                    dtstart = event_tz.localize(dtstart)
            elif master.start_date:
                try:
                    dtstart = event_tz.localize(datetime.strptime(master.start_date, '%Y-%m-%d'))
                except Exception:
                    return False, f"Invalid master start_date={master.start_date}"
            else:
                return False, "Master has no start time"

            if not master.rrule:
                return False, "Master has no RRULE"

            # Parse RID to localized datetime
            if not rid:
                return False, "No recurrence_id"
            try:
                if len(rid) == 8 and rid.isdigit():
                    rid_local = event_tz.localize(datetime.strptime(rid, '%Y%m%d'))
                else:
                    if rid.endswith('Z'):
                        rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                    else:
                        rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                    rid_local = rid_dt_utc.astimezone(event_tz)
            except Exception as e:
                return False, f"Failed to parse RID '{rid}': {e}"

            # Build rule and test membership (inclusive)
            try:
                rule = rrule.rrulestr(master.rrule, dtstart=dtstart)
            except Exception as e:
                return False, f"Failed to parse RRULE '{master.rrule}': {e}"

            candidate = rule.after(rid_local - timedelta(seconds=1), inc=True)
            is_member = candidate == rid_local
            detail = (
                f"tz={getattr(event_tz, 'zone', str(event_tz))}, "
                f"dtstart={dtstart.isoformat()}, "
                f"rrule={master.rrule}, "
                f"rid_local={rid_local.isoformat()}, "
                f"candidate={candidate.isoformat() if candidate else None}"
            )
            return is_member, detail
        except Exception as e:
            return False, f"RID validation error: {e}"

    async def _purge_all_google_artifacts_for_uid(self, source_name: str, uid: str):
        """No-op: sweep disabled in disown-before-delete model."""
        logger.debug(f"[DELETE][RECURRING][{source_name}] sweep disabled for UID={uid}")

    async def reconcile_source(self, source_name: str):
        """
        Performs a full, recurrence-aware reconciliation for a given source.
        """
        logger.info(f"Starting reconciliation for source: {source_name}")

        db_events_raw = await self.db.get_all_events_for_source(source_name)
        logger.debug(f"[{source_name}] Fetched {len(db_events_raw)} raw events from DB.")
        
        google_events_raw, _ = await self.google.list_all_mirrored_events(source_name)
        logger.debug(f"[{source_name}] Fetched {len(google_events_raw)} active events from Google.")

        desired_instances = {
            key: EventModel.from_dict(val['event_data'])
            for key, val in db_events_raw.items()
        }
        for key, event in desired_instances.items():
            logger.debug(f"[KEY GEN] Source Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        # Active events only (non-cancelled) for comparison
        google_active_instances = {
            key: EventModel.from_google_event(val)
            for key, val in google_events_raw.items()
        }
        
        # Tombstones ignored in disown-before-delete model
        google_tombstones = {}
        for key, event in google_active_instances.items():
            logger.debug(f"[KEY GEN] Google Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        # Detect unauthorized deletions of recurring instances on Google (CANCELLED items)
        # IMPORTANT: Only consider CANCELLED items that reference the CURRENT active master.
        # Stale tombstones from a previous master (different GID) must be ignored to prevent repeat replacements.
        cancelled_map = await self.google.list_cancelled_by_source(source_name)
        # Build active master GID map for quick lookup
        active_master_by_uid: Dict[str, EventModel] = {
            uid: model for (uid, rid), model in google_active_instances.items()
            if rid is None and model and model.google_event_id
        }
        uids_with_cancelled_instances = set()
        for (uid, rid), raw_item in cancelled_map.items():
            active_master = active_master_by_uid.get(uid)
            if not active_master:
                continue
            current_master_gid = active_master.google_event_id
            # CANCELLED master: only count if the cancelled item's ID equals the CURRENT master GID (shouldn't in steady state)
            if rid is None:
                if raw_item.get('id') == current_master_gid:
                    uids_with_cancelled_instances.add(uid)
            else:
                # CANCELLED instance: only count if the tombstone references the CURRENT master via recurringEventId
                if raw_item.get('recurringEventId') == current_master_gid:
                    uids_with_cancelled_instances.add(uid)
        if uids_with_cancelled_instances:
            logger.info(f"[DETECT][CANCELLED_INSTANCES][{source_name}] UIDs with deleted instances on Google (current-master scoped): {sorted(list(uids_with_cancelled_instances))}")

        # --- RID Validation Diagnostics ---
        try:
            masters_by_uid = {uid: model for (uid, rid), model in desired_instances.items() if rid is None}
            for (uid, rid), model in desired_instances.items():
                if rid is None:
                    continue
                master = masters_by_uid.get(uid)
                if not master:
                    logger.debug(f"[RID VALIDATION][SKIP] UID={uid}, RID={rid} → No master in desired set.")
                    continue
                ok, detail = self._rid_valid_against_master(master, rid)
                if ok:
                    logger.debug(f"[RID VALIDATION] UID={uid}, RID={rid} is valid. {detail}")
                else:
                    logger.warning(f"[RID VALIDATION][MISMATCH] UID={uid}, RID={rid} not generated by master. {detail}")
        except Exception as e:
            logger.warning(f"[RID VALIDATION] Unexpected error while validating: {e}")

        db_keys = set(desired_instances.keys())
        # Only consider active Google items for structural keys
        google_keys = set(google_active_instances.keys())

        logger.debug(f"[{source_name}] DB keys ({len(db_keys)}): {db_keys}")
        logger.debug(f"[{source_name}] Google keys ({len(google_keys)}): {google_keys}")

        # --- Series-level Reconciliation ---
        series_to_replace = set()
        
        # Deferred per-instance actions for recurring series (avoid whole-series replacement churn)
        deferred_creates: List[EventModel] = []
        deferred_updates: List[Tuple[str, EventModel]] = []
        deferred_deletes: List[str] = []
        all_uids = {key[0] for key in db_keys | google_keys}

        for uid in all_uids:
            db_series_keys = {k for k in db_keys if k[0] == uid}
            google_series_keys = {k for k in google_keys if k[0] == uid}

            # A series is recurring if it has exceptions or if its master event has an RRULE.
            is_recurring = any(k[1] is not None for k in db_series_keys | google_series_keys)
            if not is_recurring:
                master_model = desired_instances.get((uid, None)) or google_active_instances.get((uid, None))
                if master_model and master_model.rrule:
                    is_recurring = True

            # DIAG: master presence and key counts for this UID
            try:
                master_active_present = (uid, None) in google_active_instances
                logger.debug(f"[SERIES DIAG][{source_name}] UID={uid} master_active={master_active_present}, db_keys_count={len(db_series_keys)}, google_active_keys_count={len(google_series_keys)}")
            except Exception:
                logger.debug(f"[SERIES DIAG][{source_name}] UID={uid} diagnostics failed.")

            # If any CANCELLED instances were detected for this UID, replace the series.
            if uid in uids_with_cancelled_instances and db_series_keys:
                logger.info(f"[{source_name}] CANCELLED instances detected on Google for UID={uid}. Replacing series.")
                series_to_replace.add(uid)
                continue

            # Tombstones are ignored in disown-before-delete model; do not trigger series replacement based on their presence.

            if not is_recurring:
                continue # Skip to next UID, non-recurring events are handled later

            # If the set of instances is different, the whole series must be replaced.
            if db_series_keys != google_series_keys:
                logger.info(f"[{source_name}] Series {uid} has a structural mismatch. DB keys: {db_series_keys}, Google keys: {google_series_keys}. Replacing.")
                series_to_replace.add(uid)
                continue

            # If the structure is the same, check for content changes.
            for key in db_series_keys:
                db_model = desired_instances[key]
                google_model = google_active_instances.get(key)

                if not google_model:
                    # This case should be caught by the structural mismatch check, but as a safeguard:
                    logger.warning(f"[{source_name}] Mismatch: DB key {key} not found in Google results for series {uid}. Replacing.")
                    series_to_replace.add(uid)
                    break

                # Ensure desired exceptions carry the master recurringEventId BEFORE hashing
                if db_model.recurrence_id and not db_model.google_recurring_event_id:
                    master_key = (db_model.uid, None)
                    master_gcal_event = google_active_instances.get(master_key)
                    if master_gcal_event and master_gcal_event.google_event_id:
                        db_model.google_recurring_event_id = master_gcal_event.google_event_id

                if db_model.compute_hash() != google_model.compute_hash():
                    logger.info(f"[{source_name}] Series {uid} has a content mismatch in instance {key}. Replacing.")
                    series_to_replace.add(uid)
                    break
        
        # --- Build Plan ---
        to_create: List[EventModel] = []
        to_update: List[Tuple[str, EventModel]] = []
        to_delete: List[str] = []

        # Merge deferred per-instance actions accumulated during series checks
        if deferred_creates:
            to_create.extend(deferred_creates)
        if deferred_updates:
            to_update.extend(deferred_updates)
        if deferred_deletes:
            to_delete.extend(deferred_deletes)

        # Track recurring series to delete individually (not batch)
        recurring_series_to_delete: Dict[str, Dict[str, List[str]]] = {}
        
        # Plan replacement for entire series
        for uid in series_to_replace:
            # Collect all Google event IDs for this UID (masters only; tombstones ignored)
            masters_to_delete: List[str] = []
            
            # Active masters
            for key, event in google_active_instances.items():
                if key[0] == uid and key[1] is None and event.google_event_id:
                    masters_to_delete.append(event.google_event_id)
                    logger.info(f"[DELETE][SERIES][{source_name}] Will delete master for UID={uid}, GID={event.google_event_id}")
            
            # Store for individual deletion (not batch)
            if masters_to_delete:
                recurring_series_to_delete[uid] = {
                    'masters': masters_to_delete
                }
                logger.info(f"[DELETE][SERIES][{source_name}] UID={uid} summary: masters={len(masters_to_delete)}")

            # Add all desired DB events for this UID to the create list (master + exceptions)
            db_events_in_series = [d for k, d in desired_instances.items() if k[0] == uid]
            
            # Log exception creation plan
            db_exceptions = [e for e in db_events_in_series if e.recurrence_id]
            if db_exceptions:
                exc_rids = [e.recurrence_id for e in db_exceptions]
                logger.info(f"[PLAN][EXC_CREATE][{source_name}] UID={uid} will create {len(db_exceptions)} exceptions: RIDs={exc_rids}")
            
            to_create.extend(db_events_in_series)

        # Handle non-recurring events that are not part of a series being replaced
        non_recurring_db_keys = {k for k in db_keys if k[0] not in series_to_replace}
        non_recurring_google_keys = {k for k in google_keys if k[0] not in series_to_replace}

        nr_to_create_keys = non_recurring_db_keys - non_recurring_google_keys
        nr_to_delete_keys = non_recurring_google_keys - non_recurring_db_keys
        nr_to_compare_keys = non_recurring_db_keys.intersection(non_recurring_google_keys)

        for key in nr_to_create_keys:
            to_create.append(desired_instances[key])
        
        for key in nr_to_delete_keys:
            if google_active_instances[key].google_event_id:
                to_delete.append(google_active_instances[key].google_event_id)

        for key in nr_to_compare_keys:
            db_model = desired_instances[key]
            google_model = google_active_instances[key]
            if db_model.compute_hash() != google_model.compute_hash():
                logger.info(
                    f"[{source_name}] Change detected for non-recurring event. "
                    f"UID: {db_model.uid}. Google Event ID: {google_model.google_event_id}."
                )
                db_model.google_event_id = google_model.google_event_id
                to_update.append((db_model.google_event_id, db_model))

        # NOTE: Do not convert creates to updates during series replacement.
        # We intentionally delete the old series and re-create from DB to avoid churn and ensure mirror correctness.

        # --- Ordered execution to avoid invalid exception creations ---
        # Treat all non-exception events (masters and non-recurring) as "primary"
        primary_to_create: List[EventModel] = [e for e in to_create if not e.recurrence_id]
        exceptions_to_create: List[EventModel] = [e for e in to_create if e.recurrence_id]

        primary_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if not e.recurrence_id]
        exceptions_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if e.recurrence_id]

        # De-duplicate deletes to avoid "cannotOperateOnSameResourceMultipleTimesInBatch".
        to_delete_dedup = list(dict.fromkeys(to_delete))

        logger.info(
            f"Reconciliation plan: {len(to_create)} create, {len(to_update)} update, {len(to_delete_dedup)} delete. "
            f"(primary_create={len(primary_to_create)}, exceptions_create={len(exceptions_to_create)}, "
            f"primary_update={len(primary_to_update)}, exceptions_update={len(exceptions_to_update)})"
        )
        if logger.isEnabledFor(logging.DEBUG):
            try:
                plan_creates = [(e.uid, e.recurrence_id, bool(e.is_master_event)) for e in to_create]
                plan_updates = [(gid, e.uid, e.recurrence_id) for (gid, e) in to_update]
                plan_deletes = list(to_delete_dedup)
                logger.debug(f"[PLAN][CREATE] {plan_creates}")
                logger.debug(f"[PLAN][UPDATE] {plan_updates}")
                logger.debug(f"[PLAN][DELETE] {plan_deletes}")
                # Extra diagnostics: for any master scheduled to create, check raw Google presence
                for e in to_create:
                    if e.recurrence_id is None:
                        raw = google_events_raw.get((e.uid, None))
                        raw_status = (raw.get('status') if raw else None)
                        raw_id = (raw.get('id') if raw else None)
                        logger.debug(f"[PLAN][MASTER_CREATE_DIAG][{source_name}] UID={e.uid} raw_present={bool(raw)} raw_status={raw_status} raw_id={raw_id}")
            except Exception:
                logger.debug("[PLAN] Failed to enumerate plan details for debug output.")
        if logger.isEnabledFor(logging.DEBUG):
            try:
                plan_creates = [(e.uid, e.recurrence_id, bool(e.is_master_event)) for e in to_create]
                plan_updates = [(gid, e.uid, e.recurrence_id) for (gid, e) in to_update]
                plan_deletes = list(to_delete_dedup)
                logger.debug(f"[PLAN][CREATE] {plan_creates}")
                logger.debug(f"[PLAN][UPDATE] {plan_updates}")
                logger.debug(f"[PLAN][DELETE] {plan_deletes}")
            except Exception:
                logger.debug("[PLAN] Failed to enumerate plan details for debug output.")

        # Ensure created_map always exists for downstream exception creation
        created_map: Dict[Tuple[str, Optional[str]], str] = {}

        # 0) Delete non-recurring events in batch
        if to_delete_dedup:
            await self.google.batch_delete_events(to_delete_dedup)
        
        # Disown-then-delete recurring series masters (no tombstone handling)
        for uid, google_ids in recurring_series_to_delete.items():
            masters_ids = recurring_series_to_delete[uid].get('masters', [])

            # Disown all active items for this UID (master + exceptions) so any later trash tombstones
            # won't match our privateExtendedProperty filter.
            try:
                # Include CANCELLED so we can disown tombstones for this UID as well.
                items_for_uid = await self.google.list_events_for_uid(source_name, uid, include_cancelled=True)
            except Exception as e:
                logger.warning(f"[DISOWN][RECURRING][{source_name}] UID={uid} failed to list items for disown: {e}")
                items_for_uid = []

            disowned_count = 0
            for item in items_for_uid:
                gid = item.get('id')
                if not gid:
                    continue
                ok = await self.google.disown_event(gid)
                if ok:
                    disowned_count += 1
            logger.info(f"[DISOWN][RECURRING][{source_name}] UID={uid} disowned {disowned_count} items prior to deletion")

            logger.info(f"[DELETE][RECURRING][{source_name}] UID={uid} deleting masters={len(masters_ids)}")
            for google_id in masters_ids:
                logger.debug(f"[DELETE][RECURRING][{source_name}] Deleting master GID={google_id}")
                success = await self.google.delete_event(google_id)
                if not success:
                    logger.warning(f"[DELETE][RECURRING][{source_name}] Failed to delete master GID={google_id}")

        # 1) Update primary (masters and non-recurring) first
        if primary_to_update:
            await self.google.batch_update_events(primary_to_update)

        # 2) Create primary next
        if primary_to_create:
            created_map = await self.google.batch_create_events(primary_to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)

        # 3) Update exceptions (originalStartTime suppression now matches current master)
        if exceptions_to_update:
            await self.google.batch_update_events(exceptions_to_update)

        # 4) Create exceptions after primary are settled
        if exceptions_to_create:
            # After primary events are created, their Google IDs are available in the created_map.
            # We must now set the `google_recurring_event_id` on our exception models before creating them.
            newly_created_masters = {k: v for k, v in created_map.items() if k[1] is None}
            
            valid_exceptions_to_create = []
            for exception_model in exceptions_to_create:
                master_key = (exception_model.uid, None)
                master_id = newly_created_masters.get(master_key)

                if not master_id:
                    # This can happen if the master already existed and was not part of this create batch.
                    # We need to find its ID from the original google_instances map.
                    master_gcal_event = google_active_instances.get(master_key)
                    if master_gcal_event and master_gcal_event.google_event_id:
                         master_id = master_gcal_event.google_event_id

                if master_id:
                    exception_model.google_recurring_event_id = master_id
                    valid_exceptions_to_create.append(exception_model)
                else:
                    logger.error(f"[{source_name}] CRITICAL: Cannot find master Google ID for exception {exception_model.uid}/{exception_model.recurrence_id}. Skipping creation.")

            if valid_exceptions_to_create:
                created_map_exceptions = await self.google.batch_create_events(valid_exceptions_to_create)
                if created_map_exceptions:
                    await self.db.bulk_update_google_ids(source_name, created_map_exceptions)


        logger.info(f"Reconciliation finished for source: {source_name}")

