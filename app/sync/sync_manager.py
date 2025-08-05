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

logger = logging.getLogger(__name__)


class SyncManager:
    """Manages the sync process for a single CalDAV source."""

    def __init__(self, caldav_client: CalDAVClient, google_client: GoogleClient, database: Database):
        self.caldav = caldav_client
        self.google = google_client
        self.db = database
        self.source_name = caldav_client.name

    async def run_sync(self):
        """
        Run a full synchronization cycle for the source.
        """
        logger.info(f"Starting sync for source: {self.source_name}")
        try:
            # 1. Fetch changes from CalDAV source
            new_events, deleted_uids, new_sync_state = await self.caldav.sync_events()
            logger.info(f"Fetched {len(new_events)} new/updated events and {len(deleted_uids)} deletions.")

            # 2. Process deleted events
            await self._process_deletions(deleted_uids)

            # 3. Process new and updated events
            await self._process_updates(new_events)

            # 4. Update sync state in the database *after* all operations are successful
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

    async def _process_deletions(self, deleted_uids: List[str]):
        """Process events that were deleted from the CalDAV source."""
        # This can be further optimized with batch deletes if the Google API supports it well.
        # For now, we'll keep it simple.
        for uid in deleted_uids:
            logger.info(f"Processing deletion for CalDAV UID: {uid}")
            google_event_id = await self.db.delete_event(self.source_name, uid)
            if google_event_id:
                await self.google.delete_event(google_event_id)

    async def _process_updates(self, events: List[EventModel]):
        """Process new and updated events from the CalDAV source."""
        to_create = []
        to_update = []

        for event in events:
            stored_event = await self.db.get_event_by_caldav_uid(self.source_name, event.uid)
            if stored_event:
                if stored_event["event_hash"] != event.compute_hash():
                    to_update.append((stored_event["google_event_id"], event))
            else:
                to_create.append(event)

        # Batch create new events
        if to_create:
            created_map = await self.google.batch_create_events(to_create)
            for event in to_create:
                if event.uid in created_map:
                    google_event_id = created_map[event.uid]
                    await self.db.store_event(
                        self.source_name, event.uid, event.to_dict(), event.compute_hash(), google_event_id
                    )
                else:
                    logger.error(f"Failed to create Google event for new CalDAV event: {event.uid}")

        # Update existing events (can also be batched)
        if to_update:
            for google_event_id, event in to_update:
                logger.info(f"Event changed, updating: {event.summary} ({event.uid})")
                if google_event_id:
                    await self.google.update_event(google_event_id, event)
                    await self.db.store_event(
                        self.source_name, event.uid, event.to_dict(), event.compute_hash(), google_event_id
                    )
                else:
                    # This case should be rare, but we handle it by creating a new event.
                    logger.warning(f"No Google event ID for updated event {event.uid}, creating new one.")
                    new_google_id = await self.google.create_event(event)
                    if new_google_id:
                        await self.db.store_event(
                            self.source_name, event.uid, event.to_dict(), event.compute_hash(), new_google_id
                        )