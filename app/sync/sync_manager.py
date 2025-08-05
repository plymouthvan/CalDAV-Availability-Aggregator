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
            new_events, deleted_uids = await self.caldav.sync_events()
            logger.info(f"Fetched {len(new_events)} new/updated events and {len(deleted_uids)} deletions.")

            # 2. Process deleted events
            await self._process_deletions(deleted_uids)

            # 3. Process new and updated events
            await self._process_updates(new_events)

            logger.info(f"Sync finished for source: {self.source_name}")

        except Exception as e:
            logger.error(f"Error during sync for {self.source_name}: {e}", exc_info=True)

    async def _process_deletions(self, deleted_uids: List[str]):
        """Process events that were deleted from the CalDAV source."""
        for uid in deleted_uids:
            logger.info(f"Processing deletion for CalDAV UID: {uid}")
            # Get the Google event ID from our database before deleting the record
            google_event_id = await self.db.delete_event(self.source_name, uid)

            if google_event_id:
                # Delete the event from Google Calendar
                success = await self.google.delete_event(google_event_id)
                if success:
                    logger.info(f"Successfully deleted Google event for CalDAV UID: {uid}")
                else:
                    logger.error(f"Failed to delete Google event for CalDAV UID: {uid}")
            else:
                logger.warning(f"No corresponding Google event found for deleted CalDAV UID: {uid}")

    async def _process_updates(self, events: List[EventModel]):
        """Process new and updated events from the CalDAV source."""
        for event in events:
            event_hash = event.compute_hash()
            
            # Check if we have seen this event before
            stored_event = await self.db.get_event_by_caldav_uid(self.source_name, event.uid)

            if stored_event:
                # Event exists, check if it has changed
                if stored_event["event_hash"] != event_hash:
                    logger.info(f"Event changed, updating: {event.summary} ({event.uid})")
                    # Update Google Calendar event
                    if stored_event["google_event_id"]:
                        await self.google.update_event(stored_event["google_event_id"], event)
                    else:
                        logger.warning(f"No Google event ID for updated event {event.uid}, creating new one.")
                        new_google_id = await self.google.create_event(event)
                        stored_event["google_event_id"] = new_google_id

                    # Update database
                    await self.db.store_event(
                        self.source_name, event.uid, event.to_dict(), event_hash, stored_event["google_event_id"]
                    )
                else:
                    logger.debug(f"Event unchanged, skipping: {event.summary} ({event.uid})")
            else:
                # New event, create it in Google Calendar
                logger.info(f"New event found, creating: {event.summary} ({event.uid})")
                google_event_id = await self.google.create_event(event)

                if google_event_id:
                    # Store the new event in our database
                    await self.db.store_event(
                        self.source_name, event.uid, event.to_dict(), event_hash, google_event_id
                    )
                else:
                    logger.error(f"Failed to create Google event for new CalDAV event: {event.uid}")