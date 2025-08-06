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

        google_instances = {
            key: EventModel.from_google_event(val)
            for key, val in google_events_raw.items()
        }

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

