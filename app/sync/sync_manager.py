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
            logger.info(f"Fetched {len(new_events)} new/updated events and {len(deleted_uids)} deletions.")

            # 2. Process deletions in the database
            for uid in deleted_uids:
                # This now needs to handle deleting all instances for a given UID.
                # A more robust implementation might mark them as deleted.
                # For now, we assume a deleted UID means the whole series is gone.
                logger.info(f"Deleting all instances for UID: {uid}")
                await self.db.delete_event_series(self.source_name, uid)

            # 3. Process new and updated events in the database
            for event in new_events:
                # Check if the event instance already exists to preserve the google_event_id
                existing_event = await self.db.get_event_instance(
                    self.source_name, event.uid, event.recurrence_id
                )
                google_event_id = existing_event['google_event_id'] if existing_event else None

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
