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

    async def reconcile_source(self, source_name: str):
        """
        Performs a full, recurrence-aware reconciliation for a given source.
        """
        logger.info(f"Starting reconciliation for source: {source_name}")

        db_events_raw = await self.db.get_all_events_for_source(source_name)
        logger.debug(f"[{source_name}] Fetched {len(db_events_raw)} raw events from DB.")
        
        google_events_raw = await self.google.list_all_mirrored_events(source_name)
        logger.debug(f"[{source_name}] Fetched {len(google_events_raw)} events from Google.")

        desired_instances = {
            key: EventModel.from_dict(val['event_data'])
            for key, val in db_events_raw.items()
        }
        for key, event in desired_instances.items():
            logger.debug(f"[KEY GEN] Source Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        google_instances = {
            key: EventModel.from_google_event(val)
            for key, val in google_events_raw.items()
        }
        for key, event in google_instances.items():
            logger.debug(f"[KEY GEN] Google Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        db_keys = set(desired_instances.keys())
        google_keys = set(google_instances.keys())

        logger.debug(f"[{source_name}] DB keys ({len(db_keys)}): {db_keys}")
        logger.debug(f"[{source_name}] Google keys ({len(google_keys)}): {google_keys}")

        to_create_keys = db_keys - google_keys
        to_delete_keys = google_keys - db_keys
        to_compare_keys = db_keys.intersection(google_keys)

        logger.debug(f"[{source_name}] Keys to create: {to_create_keys}")
        logger.debug(f"[{source_name}] Keys to delete: {to_delete_keys}")
        logger.debug(f"[{source_name}] Keys to compare: {to_compare_keys}")

        to_create: List[EventModel] = []
        to_update: List[Tuple[str, EventModel]] = []
        to_delete: List[str] = []

        # Events to create
        for key in to_create_keys:
            model = desired_instances[key]
            if model.recurrence_id:
                master_key = (model.uid, None)
                master_gcal_event = google_instances.get(master_key)
                if master_gcal_event and master_gcal_event.google_event_id:
                    model.google_recurring_event_id = master_gcal_event.google_event_id
                else:
                    logger.warning(f"[{source_name}] Cannot create exception {key} because its master is not in Google yet.")
                    continue
            to_create.append(model)

        # Events to delete
        for key in to_delete_keys:
            google_model = google_instances[key]
            if google_model.google_event_id:
                to_delete.append(google_model.google_event_id)

        # Events to update
        for key in to_compare_keys:
            db_model = desired_instances[key]
            google_model = google_instances[key]
            
            db_hash = db_model.compute_hash()
            google_hash = google_model.compute_hash()

            if db_hash != google_hash:
                logger.info(
                    f"[{source_name}] Change detected for event. "
                    f"UID: {db_model.uid}, RecurrenceID: {db_model.recurrence_id}. "
                    f"Google Event ID: {google_model.google_event_id}. "
                    f"Old Hash: {google_hash}\nNew Hash: {db_hash}."
                )
                db_model.google_event_id = google_model.google_event_id
                if db_model.recurrence_id:
                    master_key = (db_model.uid, None)
                    master_gcal_event = google_instances.get(master_key)
                    if master_gcal_event:
                        db_model.google_recurring_event_id = master_gcal_event.google_event_id
                to_update.append((db_model.google_event_id, db_model))

        # --- Orphan Sweep Step ---
        all_uids = {key[0] for key in db_keys | google_keys}
        orphans_to_delete_gids = set()
        orphans_to_delete_keys = set()

        for uid in all_uids:
            # 1. Build source and google exception sets for the current UID
            source_master = desired_instances.get((uid, None))
            source_exdates = {exdate.strftime('%Y%m%d') for exdate in source_master.exdates} if source_master and source_master.exdates else set()
            
            source_exceptions = {
                key[1] for key in db_keys if key[0] == uid and key[1] is not None
            }
            
            google_exceptions = [
                g_event for g_key, g_event in google_instances.items()
                if g_key[0] == uid and g_event.recurrence_id is not None
            ]

            # 2. Compute delete candidates
            delete_candidates = []

            for g_event in google_exceptions:
                rid = g_event.recurrence_id
                rid_date_str = ""
                if rid:
                    try:
                        # Normalize recurrence ID to YYYYMMDD format for comparison with EXDATEs
                        rid_dt = datetime.strptime(rid, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                        rid_date_str = rid_dt.strftime('%Y%m%d')
                    except (ValueError, TypeError):
                        try:
                            rid_dt = datetime.strptime(rid, '%Y%m%d').replace(tzinfo=timezone.utc)
                            rid_date_str = rid_dt.strftime('%Y%m%d')
                        except (ValueError, TypeError):
                             logger.warning(f"[{source_name}] Could not parse recurrence_id '{rid}' for UID {uid}")
                             continue

                # A: exceptions where rid is in source_exdates
                if rid_date_str and rid_date_str in source_exdates:
                    delete_candidates.append((g_event, "EXDATE found in source master"))
                    continue

                # B: exceptions where rid is not in source_exceptions
                if rid not in source_exceptions:
                    delete_candidates.append((g_event, "Recurrence ID not found in source exceptions"))
                    continue
            
            # C: Tagged, standalone Google events with our extProps
            standalone_google_events = [
                g_event for g_key, g_event in google_instances.items()
                if g_key[0] == uid and g_event.google_recurring_event_id is None and g_event.recurrence_id is not None
            ]
            for g_event in standalone_google_events:
                 if g_event.recurrence_id not in source_exceptions:
                    delete_candidates.append((g_event, "Standalone Google event not in source exceptions"))

            # 3. Log and prepare for deletion
            if delete_candidates:
                plan_log = f"[{source_name}] [ORPHAN_DELETE_PLAN] UID: {uid}\n"
                for g_event, reason in delete_candidates:
                    plan_log += f"  - Deleting GID: {g_event.google_event_id}, RID: {g_event.recurrence_id}. Reason: {reason}\n"
                    orphans_to_delete_gids.add(g_event.google_event_id)
                    orphans_to_delete_keys.add((uid, g_event.recurrence_id))
                logger.info(plan_log)

        if orphans_to_delete_gids:
            to_delete.extend(list(orphans_to_delete_gids))
            # We also need to ensure these are removed from the DB tracking.
            # This assumes the main deletion logic will handle DB removal.
            # If not, we would call: await self.db.bulk_delete_events(source_name, orphans_to_delete_keys)

        logger.info(f"Reconciliation plan: {len(to_create)} create, {len(to_update)} update, {len(to_delete)} delete.")

        if to_create:
            created_map = await self.google.batch_create_events(to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)
        
        if to_update:
            await self.google.batch_update_events(to_update)
        
        if to_delete:
            await self.google.batch_delete_events(to_delete)

        logger.info(f"Reconciliation finished for source: {source_name}")

