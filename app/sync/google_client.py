"""
Google Calendar client for CalDAV Mirror

Handles all interactions with the Google Calendar API, including creating,
updating, and deleting events in the destination calendar.
"""

import asyncio
import aiohttp
import logging
import json
from typing import Dict, Any, Optional, List, Tuple
import uuid

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

    async def batch_create_events(self, events: List[EventModel]) -> Dict[str, str]:
        """
        Create multiple events in a single batch request.

        Args:
            events: A list of normalized events to create.

        Returns:
            A dictionary mapping CalDAV UIDs to new Google event IDs.
        """
        if not events:
            return {}

        logger.info(f"Batch creating {len(events)} Google events...")
        batch_url = f"{self.API_BASE_URL}/batch"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "multipart/mixed; boundary=batch_boundary"

        # Construct the multipart request body
        body = ""
        for event in events:
            event_data = event.to_google_event()
            body += "--batch_boundary\n"
            body += "Content-Type: application/http\n"
            body += "Content-ID: <item{}>\n\n".format(uuid.uuid4())
            body += f"POST /calendar/v3/calendars/{self.calendar_id}/events\n"
            body += "Content-Type: application/json\n\n"
            body += json.dumps(event_data) + "\n"
        body += "--batch_boundary--"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(batch_url, data=body.encode('utf-8'), headers=headers) as response:
                    if response.status == 200:
                        # Process the multipart response
                        return await self._parse_batch_response(response, events)
                    else:
                        error_text = await response.text()
                        logger.error(f"Batch create failed: {response.status} - {error_text}")
                        return {}
        except Exception as e:
            logger.error(f"Error in batch create: {e}", exc_info=True)
            return {}

    async def _parse_batch_response(self, response: aiohttp.ClientResponse, original_events: List[EventModel]) -> Dict[str, str]:
        """Parse the multipart/mixed response from a batch request."""
        # This is a simplified parser. A more robust solution would use a proper MIME parser.
        content_type = response.headers.get('Content-Type', '')
        boundary = None
        for part in content_type.split(';'):
            if 'boundary=' in part:
                boundary = part.strip().split('=')[1]
                break
        
        if not boundary:
            logger.error("Batch response is missing boundary.")
            return {}

        body = await response.text()
        parts = body.split(f'--{boundary}')
        
        results = {}
        event_idx = 0
        for part in parts:
            if '"id":' in part:
                try:
                    json_part = part[part.find('{'):part.rfind('}') + 1]
                    data = json.loads(json_part)
                    if 'id' in data and event_idx < len(original_events):
                        caldav_uid = original_events[event_idx].uid
                        results[caldav_uid] = data['id']
                        event_idx += 1
                except json.JSONDecodeError:
                    continue
        
        logger.info(f"Successfully processed {len(results)} events from batch response.")
        return results