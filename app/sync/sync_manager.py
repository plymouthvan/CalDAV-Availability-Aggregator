"""
Sync Manager for CalDAV Mirror

Orchestrates the synchronization process between a CalDAV source and
the Google Calendar destination.
"""

import logging
from datetime import datetime
from typing import List

from database import Database
from .caldav_client import CalDAVClient
from .google_client import GoogleClient
from .event_model import EventModel
from .window_reconciler import WindowReconciler

logger = logging.getLogger(__name__)


class SyncManager:
    """Manages the sync process for a single CalDAV source."""

    def __init__(self, caldav_client: CalDAVClient, google_client: GoogleClient, database: Database):
        self.caldav = caldav_client
        self.google = google_client
        self.db = database
        self.source_name = caldav_client.name
        self.window_reconciler = WindowReconciler(google_client, database)

    def _sort_key_for_new_exception(self, event: EventModel) -> int:
        """
        Robust sort key for new exceptions (EventModel instances). Returns epoch seconds UTC.
        """
        return self._safe_epoch(
            dt_val=event.start_datetime,
            date_str=event.start_date,
            rid=event.recurrence_id,
            ctx=f"new:{event.uid}:{event.recurrence_id}"
        )

    def _sort_key_for_existing_exception(self, row: dict) -> int:
        """
        Robust sort key for existing exceptions (DB rows). Returns epoch seconds UTC.
        """
        data = (row or {}).get('event_data') or {}
        return self._safe_epoch(
            dt_val=data.get('start_datetime'),
            date_str=data.get('start_date'),
            rid=data.get('recurrence_id'),
            ctx=f"old:{row.get('caldav_uid')}:{row.get('recurrence_id')}"
        )

    def _safe_epoch(self, dt_val, date_str, rid, ctx: str) -> int:
        """
        Prefer start_datetime (datetime or ISO string), fallback to start_date (YYYY-MM-DD),
        then recurrence_id, else epoch(0). Returns integer epoch seconds UTC.
        Logs fallbacks for diagnostics.
        """
        try:
            from datetime import timezone  # local import to avoid changing module imports
            # 1) start_datetime
            if isinstance(dt_val, datetime):
                try:
                    d = dt_val
                    if d.tzinfo is None:
                        d = d.replace(tzinfo=timezone.utc)
                    return int(d.astimezone(timezone.utc).timestamp())
                except Exception:
                    logger.debug(f"[{self.source_name}] _safe_epoch: failed datetime-&gt;epoch for {ctx}")
            elif isinstance(dt_val, str) and dt_val:
                try:
                    iso = dt_val.replace('Z', '+00:00')
                    parsed = datetime.fromisoformat(iso)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    return int(parsed.astimezone(timezone.utc).timestamp())
                except Exception:
                    logger.warning(f"[{self.source_name}] Non-ISO start_datetime for {ctx}: {dt_val}")

            # 2) start_date (YYYY-MM-DD) at midnight UTC
            if isinstance(date_str, str) and date_str:
                try:
                    d0 = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    logger.debug(f"[{self.source_name}] Fallback sort by start_date for {ctx}: {date_str}")
                    return int(d0.timestamp())
                except Exception:
                    logger.warning(f"[{self.source_name}] Bad start_date for {ctx}: {date_str}")

            # 3) recurrence_id (YYYYMMDD or YYYYMMDDTHHMMSS[Z])
            if isinstance(rid, str) and rid:
                try:
                    if len(rid) == 8 and rid.isdigit():
                        d1 = datetime.strptime(rid, "%Y%m%d").replace(tzinfo=timezone.utc)
                    else:
                        if rid.endswith('Z'):
                            d1 = datetime.strptime(rid, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                        else:
                            d1 = datetime.strptime(rid, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
                    logger.debug(f"[{self.source_name}] Fallback sort by recurrence_id for {ctx}: {rid}")
                    return int(d1.timestamp())
                except Exception:
                    logger.warning(f"[{self.source_name}] Bad recurrence_id for {ctx}: {rid}")

            # 4) ultimate fallback
            logger.warning(f"[{self.source_name}] Using minimal sort key for {ctx}")
            epoch0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
            return int(epoch0.timestamp())
        except Exception:
            # Super defensive fallback
            return 0
    async def run_sync(self):
        """
        Run a full synchronization cycle for the source.
        """
        logger.info(f"Starting sync for source: {self.source_name}")
        try:
            # 1. Fetch changes from CalDAV source
            new_events, deleted_uids, new_sync_state = await self.caldav.sync_events()
            logger.info(f"[{self.source_name}] Fetched {len(new_events)} new/updated events and {len(deleted_uids)} deletions from CalDAV.")
            logger.debug(f"[{self.source_name}] New/updated events: {[e.uid for e in new_events]}")
            logger.debug(f"[{self.source_name}] Deleted UIDs: {deleted_uids}")

            # 2. Process deletions in the database
            if deleted_uids:
                logger.info(f"[{self.source_name}] Processing {len(deleted_uids)} deletions in the database.")
                for uid in deleted_uids:
                    logger.debug(f"[{self.source_name}] Deleting event series with UID: {uid}")
                    await self.db.delete_event_series(self.source_name, uid)

            # 3. Process new and updated events in the database
            if new_events:
                logger.info(f"[{self.source_name}] Storing {len(new_events)} new/updated events in the database.")

                # Group events by UID to process each series together
                events_by_uid = {}
                for event in new_events:
                    events_by_uid.setdefault(event.uid, []).append(event)

                for uid, events_for_uid in events_by_uid.items():
                    # --- Smart Google ID Mapping ---
                    # Get existing instances from DB to preserve Google Event IDs across modifications.
                    existing_instances_raw = await self.db.get_all_event_instances_for_uid(self.source_name, uid)
                    google_id_map = {}

                    # Separate master and exceptions for both new and existing events
                    new_master = next((e for e in events_for_uid if e.is_master_event), None)
                    # Diagnostics: enumerate exception timing fields before sorting
                    try:
                        new_exc_list = [e for e in events_for_uid if not e.is_master_event]
                        old_exc_list = [i for i in existing_instances_raw if not i['event_data'].get('is_master_event')]
                        logger.debug(f"[{self.source_name}] UID {uid}: exception diagnostics: new_count={len(new_exc_list)}, existing_count={len(old_exc_list)}")
                        for e in new_exc_list:
                            logger.debug(f"[{self.source_name}] NEW_EXC uid={uid} rid={e.recurrence_id} start_datetime={e.start_datetime} type={(type(e.start_datetime).__name__ if e.start_datetime is not None else 'NoneType')} start_date={e.start_date}")
                        for irow in old_exc_list:
                            data = (irow or {}).get('event_data') or {}
                            sd = data.get('start_datetime')
                            logger.debug(f"[{self.source_name}] OLD_EXC uid={uid} rid={irow.get('recurrence_id')} start_datetime={sd} type={(type(sd).__name__ if sd is not None else 'NoneType')} start_date={data.get('start_date')}")
                    except Exception:
                        logger.debug(f"[{self.source_name}] UID {uid}: exception diagnostics failed")
                    new_exceptions = sorted([e for e in events_for_uid if not e.is_master_event], key=self._sort_key_for_new_exception)

                    existing_master = next((i for i in existing_instances_raw if i['event_data'].get('is_master_event')), None)
                    existing_exceptions = sorted(
                        [i for i in existing_instances_raw if not i['event_data'].get('is_master_event')],
                        key=self._sort_key_for_existing_exception
                    )

                    # 1. Map the master event's Google ID
                    if new_master and existing_master:
                        google_id_map[(uid, new_master.recurrence_id)] = existing_master.get('google_event_id')

                    # 2. Heuristic: If exception counts match, map by sorted order to handle modifications.
                    if len(new_exceptions) > 0 and len(new_exceptions) == len(existing_exceptions):
                        logger.debug(f"[{self.source_name}] UID {uid}: Exception count matches ({len(new_exceptions)}). Applying modification heuristic.")
                        for i, new_ex in enumerate(new_exceptions):
                            existing_ex = existing_exceptions[i]
                            google_id_map[(uid, new_ex.recurrence_id)] = existing_ex.get('google_event_id')
                    else:
                        # Fallback for additions/deletions: map by recurrence ID to preserve IDs for unmodified exceptions.
                        logger.debug(f"[{self.source_name}] UID {uid}: Exception count mismatch (new: {len(new_exceptions)}, old: {len(existing_exceptions)}). Using fallback mapping.")
                        existing_map = {
                            (inst['caldav_uid'], inst['recurrence_id']): inst.get('google_event_id')
                            for inst in existing_instances_raw
                        }
                        for event in events_for_uid:
                            if (event.uid, event.recurrence_id) in existing_map:
                                google_id_map[(event.uid, event.recurrence_id)] = existing_map.get((event.uid, event.recurrence_id))
                    
                    # --- Clear and Replace ---
                    # 1. Clear out all old entries for this UID
                    await self.db.delete_event_series(self.source_name, uid)

                    # 2. Store the new, correct set of events using the smart map
                    for event in events_for_uid:
                        google_event_id = google_id_map.get((event.uid, event.recurrence_id))
                        logger.debug(f"[{self.source_name}] Storing event: UID={event.uid}, RecurrenceID={event.recurrence_id}, IsMaster={event.is_master_event}, MappedGoogleID={google_event_id}")
                        await self.db.store_event(
                            self.source_name,
                            event.uid,
                            event.recurrence_id,
                            event.to_dict(),
                            event.compute_hash(),
                            event.is_master_event,
                            google_event_id=google_event_id,
                            google_recurring_event_id=event.google_recurring_event_id
                        )

            # 4. Trigger the windowed, flattened-instance reconciliation
            await self.window_reconciler.reconcile_window(self.source_name)

            # 5. Update sync state
            if new_sync_state:
                await self.db.update_sync_state(
                    self.source_name,
                    self.caldav.sync_method,
                    sync_token=new_sync_state.get("sync_token"),
                    ctag=new_sync_state.get("ctag")
                )
                logger.info(f"Successfully updated sync state for {self.source_name}.")

            logger.info(f"Sync finished for source: {self.source_name}")

        except Exception as e:
            logger.error(f"Error during sync for {self.source_name}: {e}", exc_info=True)
