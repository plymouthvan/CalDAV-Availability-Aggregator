"""
Sync Manager for CalDAV Mirror

Orchestrates the synchronization process between a CalDAV source and
the Google Calendar destination.
"""

import logging
from typing import List

from database import Database
from .caldav_client import CalDAVClient
from .google_client import GoogleClient
from .event_model import EventModel
from .reconciler import Reconciler

logger = logging.getLogger(__name__)


class SyncManager:
    """Manages the sync process for a single CalDAV source."""

    def __init__(self, caldav_client: CalDAVClient, google_client: GoogleClient, database: Database):
        self.caldav = caldav_client
        self.google = google_client
        self.db = database
        self.source_name = caldav_client.name
        self.reconciler = Reconciler(google_client, database)

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
                for event in new_events:
                    # Check if the event instance already exists to preserve the google_event_id
                    existing_event = await self.db.get_event_instance(
                        self.source_name, event.uid, event.recurrence_id
                    )
                    google_event_id = existing_event['google_event_id'] if existing_event else None
                    
                    logger.debug(f"[{self.source_name}] Storing event: UID={event.uid}, RecurrenceID={event.recurrence_id}, IsMaster={event.is_master_event}, ExistingGoogleID={google_event_id}")
                    await self.db.store_event(
                        self.source_name,
                        event.uid,
                        event.recurrence_id,
                        event.to_dict(),
                        event.compute_hash(),
                        event.is_master_event,
                        google_event_id=google_event_id
                    )

            # 4. Trigger the reconciliation process
            await self.reconciler.reconcile_source(self.source_name)

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
