"""
Google Calendar client for CalDAV Mirror

Handles all interactions with the Google Calendar API, including creating,
updating, and deleting events in the destination calendar.
"""

import asyncio
import aiohttp
import logging
from typing import Dict, Any, Optional, List

from .event_model import EventModel
from auth.google_oauth import GoogleOAuth

logger = logging.getLogger(__name__)


class GoogleClient:
    """Google Calendar API client."""

    API_BASE_URL = "https://www.googleapis.com/calendar/v3"

    def __init__(self, oauth_handler: GoogleOAuth, calendar_id: str = "primary"):
        self.oauth = oauth_handler
        self.calendar_id = calendar_id
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authorization headers with a valid access token."""
        access_token = await self.oauth.get_access_token()
        if not access_token:
            raise Exception("Failed to get Google access token")
        return {"Authorization": f"Bearer {access_token}"}

    async def create_event(self, event: EventModel) -> Optional[str]:
        """
        Create a new event in Google Calendar.

        Args:
            event: The normalized event to create.

        Returns:
            The Google event ID if successful, otherwise None.
        """
        logger.info(f"Creating Google event for CalDAV UID: {event.uid}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"
        
        google_event_data = event.to_google_event()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=google_event_data, headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Successfully created Google event ID: {data['id']}")
                        return data["id"]
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to create Google event: {response.status} - {error_text}"
                        )
                        return None
        except Exception as e:
            logger.error(f"Error creating Google event: {e}", exc_info=True)
            return None

    async def update_event(self, google_event_id: str, event: EventModel) -> bool:
        """
        Update an existing event in Google Calendar.

        Args:
            google_event_id: The ID of the Google event to update.
            event: The normalized event with updated data.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"Updating Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"

        google_event_data = event.to_google_event()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(
                    url, json=google_event_data, headers=headers
                ) as response:
                    if response.status == 200:
                        logger.info(f"Successfully updated Google event ID: {google_event_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to update Google event: {response.status} - {error_text}"
                        )
                        return False
        except Exception as e:
            logger.error(f"Error updating Google event: {e}", exc_info=True)
            return False

    async def delete_event(self, google_event_id: str) -> bool:
        """
        Delete an event from Google Calendar.

        Args:
            google_event_id: The ID of the Google event to delete.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"Deleting Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    if response.status == 204:
                        logger.info(f"Successfully deleted Google event ID: {google_event_id}")
                        return True
                    elif response.status == 410: # Gone, already deleted
                        logger.warning(f"Google event ID {google_event_id} was already deleted.")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to delete Google event: {response.status} - {error_text}"
                        )
                        return False
        except Exception as e:
            logger.error(f"Error deleting Google event: {e}", exc_info=True)
            return False

    async def get_event(self, google_event_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an event from Google Calendar by its ID.

        Args:
            google_event_id: The ID of the Google event to retrieve.

        Returns:
            The event data as a dictionary if found, otherwise None.
        """
        logger.debug(f"Fetching Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        logger.info(f"Google event ID {google_event_id} not found.")
                        return None
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to get Google event: {response.status} - {error_text}"
                        )
                        return None
        except Exception as e:
            logger.error(f"Error getting Google event: {e}", exc_info=True)
            return None