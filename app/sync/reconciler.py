"""
Reconciliation module for CalDAV Mirror

Compares the state of the local database with the state of Google Calendar
and generates the necessary operations to make Google Calendar a mirror of
the database.
"""

import logging
from typing import Dict, Any, List, Tuple

from .google_client import GoogleClient
from database import Database
from .event_model import EventModel

logger = logging.getLogger(__name__)

class Reconciler:
    """
    Orchestrates the reconciliation between the local database and Google
    Calendar for a specific source.
    """

    def __init__(self, google_client: GoogleClient, database: Database):
        """
        Initializes the Reconciler.

        Args:
            google_client: An instance of the GoogleClient.
            database: An instance of the Database.
        """
        self.google = google_client
        self.db = database

    async def reconcile_source(self, source_name: str):
        """
        Performs a full reconciliation for a given source.

        This method will:
        1. Fetch all events for the source from the local database (desired state).
        2. Fetch all corresponding events from Google Calendar (actual state).
        3. Compare the two states to determine what needs to be created,
           updated, or deleted on Google Calendar.
        4. Execute the necessary batch operations.

        Args:
            source_name: The name of the source to reconcile.
        """
        logger.info(f"Starting reconciliation for source: {source_name}")

        # 1. Get the desired state from our local database
        db_events = await self.db.get_all_events_for_source(source_name)
        logger.info(f"Found {len(db_events)} events in the database for {source_name}.")
        logger.info(f"Trigger One")

        # 2. Get the actual state from Google Calendar
        google_events = await self.google.list_mirrored_events(source_name)
        logger.info(f"Found {len(google_events)} mirrored events on Google Calendar for {source_name}.")
        logger.info(f"Trigger Two")

        # 3. Compare the two states to find differences
        to_create: List[EventModel] = []
        to_update: List[Tuple[str, EventModel]] = []
        to_delete: List[str] = []
        logger.info(f"Trigger Three")

        db_uids = set(db_events.keys())
        google_uids = set(google_events.keys())
        logger.info(f"Trigger Four")

        # Events to create are in DB but not in Google
        for uid in db_uids - google_uids:
            event_data = db_events[uid]['event_data']
            to_create.append(EventModel.from_dict(event_data))
            logger.info(f"Trigger Five (Loop)")

        # Events to delete are in Google but not in DB
        for uid in google_uids - db_uids:
            to_delete.append(google_events[uid]['id'])
            logger.info(f"Trigger Six (Loop)")

        # Events that exist in both need to be checked for updates
        for uid in db_uids.intersection(google_uids):
            db_event_data = db_events[uid]
            google_event_id = db_event_data.get('google_event_id')
            logger.info(f"Trigger Seven ")
            
            if not google_event_id:
                # This should not happen, but as a safeguard...
                logger.warning(f"DB event {uid} is missing a Google ID. Re-creating.")
                to_create.append(EventModel.from_dict(db_event_data['event_data']))
                continue

            # Compare hashes to detect changes
            google_event = google_events[uid]
            google_hash = google_event.get('extendedProperties', {}).get('private', {}).get('caldav-mirror-hash')
            logger.info(f"Trigger Eight ")

            # Create an EventModel from the Google Calendar data to compute its hash
            google_event_model = EventModel.from_google_event(google_event)
            google_event_hash = google_event_model.compute_hash()

            logger.debug(f"Comparing hashes for UID {uid}:")
            logger.debug(f"  DB Hash:     {db_event_data['event_hash']}")
            logger.debug(f"  Google Hash: {google_event_hash}")

            if db_event_data['event_hash'] != google_event_hash:
                logger.info(f"Hash mismatch for UID {uid}. Event will be updated.")
                event_model = EventModel.from_dict(db_event_data['event_data'])
                to_update.append((google_event_id, event_model))
                logger.info(f"Trigger Nine (Loop)")

        logger.info(f"Reconciliation plan: {len(to_create)} to create, {len(to_update)} to update, {len(to_delete)} to delete.")

        # 4. Execute the batch operations
        if to_create:
            created_map = await self.google.batch_create_events(to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)
                logger.info(f"Successfully created {len(created_map)} new events.")
            else:
                logger.error("Failed to create new events during reconciliation.")

        if to_update:
            if await self.google.batch_update_events(to_update):
                logger.info(f"Successfully updated {len(to_update)} events.")
            else:
                logger.error("Failed to update events during reconciliation.")

        if to_delete:
            if await self.google.batch_delete_events(to_delete):
                logger.info(f"Successfully deleted {len(to_delete)} events.")
            else:
                logger.error("Failed to delete events during reconciliation.")
        
        logger.info(f"Reconciliation finished for source: {source_name}")
