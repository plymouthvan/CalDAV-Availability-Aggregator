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
        """
        logger.info(f"Creating Google event for CalDAV UID: {event.uid}, Recurrence ID: {event.recurrence_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"
        
        google_event_data = event.to_google_event()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=google_event_data, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Successfully created Google event ID: {data['id']}")
                        return data["id"]
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to create Google event: {response.status} - {error_text}")
                        return None
        except Exception as e:
            logger.error(f"Error creating Google event: {e}", exc_info=True)
            return None

    async def update_event(self, google_event_id: str, event: EventModel) -> bool:
        """
        Update an existing event in Google Calendar.
        """
        logger.info(f"Updating Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"
        google_event_data = event.to_google_event()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(url, json=google_event_data, headers=headers) as response:
                    if response.status == 200:
                        logger.info(f"Successfully updated Google event ID: {google_event_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to update Google event: {response.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error updating Google event: {e}", exc_info=True)
            return False

    async def delete_event(self, google_event_id: str) -> bool:
        """
        Delete an event from Google Calendar.
        """
        logger.info(f"Deleting Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(url, headers=headers) as response:
                    if response.status in [204, 410]: # 204 No Content, 410 Gone
                        logger.info(f"Successfully deleted Google event ID: {google_event_id}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to delete Google event: {response.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error deleting Google event: {e}", exc_info=True)
            return False

    async def _list_events_paginated(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Helper to fetch events with pagination."""
        all_items = []
        page_token = None
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events"
        headers = await self._get_auth_headers()

        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    if page_token:
                        params['pageToken'] = page_token
                    
                    # Filter out None values from params before the request
                    request_params = {k: v for k, v in params.items() if v is not None}
                    
                    logger.debug(f"Requesting events with params: {request_params}")
                    async with session.get(url, headers=headers, params=request_params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Failed to list events: {response.status} - {error_text}")
                            break
                        data = await response.json()
                        items = data.get('items', [])
                        active_items = [item for item in items if item.get('status') != 'cancelled']
                        
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Received page with {len(items)} items, {len(active_items)} active.")

                        all_items.extend(active_items)
                        page_token = data.get('nextPageToken')
                        if not page_token:
                            break
        except Exception as e:
            logger.error(f"Error fetching events: {e}", exc_info=True)
        
        return all_items

    async def list_mirrored_master_events(self, source_name: str) -> Dict[str, Dict[str, Any]]:
        """Fetches only the master events from Google Calendar."""
        logger.info(f"Listing mirrored master events for source: {source_name}")
        params = {
            'privateExtendedProperty': f"caldav-mirror-source={source_name}",
            'maxResults': 2500
        }
        items = await self._list_events_paginated(params)
        
        events = {}
        for item in items:
            private_props = item.get('extendedProperties', {}).get('private', {})
            uid = private_props.get('caldav-mirror-uid')
            if uid:
                events[uid] = item
        
        logger.info(f"Found {len(events)} mirrored master events for source: {source_name}")
        return events

    async def list_all_mirrored_events(self, source_name: str) -> Dict[Tuple[str, Optional[str]], Dict[str, Any]]:
        """Fetches all mirrored events (masters and instances) from Google Calendar."""
        logger.info(f"Listing all mirrored events for source: {source_name}")
        params = {
            'privateExtendedProperty': f"caldav-mirror-source={source_name}",
            'maxResults': 2500
        }
        items = await self._list_events_paginated(params)

        events = {}
        for item in items:
            private_props = item.get('extendedProperties', {}).get('private', {})
            uid = private_props.get('caldav-mirror-uid')
            if uid:
                model = EventModel.from_google_event(item)
                key = (uid, model.recurrence_id)
                events[key] = item
        
        logger.info(f"Found {len(events)} mirrored events for source: {source_name}")
        return events

    async def get_event_instances(self, google_event_id: str) -> List[Dict[str, Any]]:
        """Fetches all instances for a given recurring event ID."""
        logger.info(f"Fetching instances for Google event ID: {google_event_id}")
        instances = []
        page_token = None
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}/instances"
        headers = await self._get_auth_headers()
        params = {'maxResults': 2500}

        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    if page_token:
                        params['pageToken'] = page_token
                    async with session.get(url, headers=headers, params=params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Failed to get event instances for {google_event_id}: {response.status} - {error_text}")
                            break
                        data = await response.json()
                        items = data.get('items', [])
                        active_items = [item for item in items if item.get('status') != 'cancelled']
                        
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Received page with {len(items)} items, {len(active_items)} active.")
                        
                        instances.extend(active_items)
                        page_token = data.get('nextPageToken')
                        if not page_token:
                            break
        except Exception as e:
            logger.error(f"Error fetching event instances: {e}", exc_info=True)

        logger.info(f"Found {len(instances)} instances for Google event ID: {google_event_id}")
        return instances

    async def batch_create_events(self, events: List[EventModel]) -> Dict[Tuple[str, Optional[str]], str]:
        if not events: return {}
        logger.info(f"Batch creating {len(events)} Google events...")
        batch_url = "https://www.googleapis.com/batch/calendar/v3"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "multipart/mixed; boundary=batch_boundary"

        body = ""
        for event in events:
            event_data = event.to_google_event()
            logger.debug(f"Batch Create Payload for UID {event.uid}: {json.dumps(event_data, indent=2)}")
            if event.is_master_event:
                logger.debug(f"  Master Event Details: UID={event.uid}, Recurrence={event_data.get('recurrence')}")
            elif event.recurrence_id:
                logger.debug(f"  Exception Details: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
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
                        return await self._parse_batch_create_response(response, events)
                    else:
                        error_text = await response.text()
                        logger.error(f"Batch create failed: {response.status} - {error_text}")
                        return {}
        except Exception as e:
            logger.error(f"Error in batch create: {e}", exc_info=True)
            return {}

    async def _parse_batch_create_response(self, response: aiohttp.ClientResponse, original_events: List[EventModel]) -> Dict[Tuple[str, Optional[str]], str]:
        """Parse the multipart/mixed response from a batch request."""
        results = {}
        content_type = response.headers.get('Content-Type', '')
        boundary = [p.split('=')[1] for p in content_type.split(';') if 'boundary=' in p][0]
        
        body = await response.text()
        parts = body.split(f'--{boundary}')
        
        event_idx = 0
        for part in parts:
            if '"id":' in part and '200 OK' in part:
                try:
                    json_part = part[part.find('{'):part.rfind('}') + 1]
                    data = json.loads(json_part)
                    if 'id' in data and event_idx < len(original_events):
                        event = original_events[event_idx]
                        key = (event.uid, event.recurrence_id)
                        results[key] = data['id']
                except json.JSONDecodeError:
                    continue
            if 'Content-ID' in part:
                event_idx += 1
        
        logger.info(f"Successfully processed {len(results)} events from batch response.")
        return results

    async def batch_update_events(self, events_to_update: List[Tuple[str, EventModel]]) -> bool:
        if not events_to_update: return True
        logger.info(f"Batch updating {len(events_to_update)} Google events...")
        batch_url = "https://www.googleapis.com/batch/calendar/v3"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "multipart/mixed; boundary=batch_boundary"

        body = ""
        for google_event_id, event in events_to_update:
            event_data = event.to_google_event()
            logger.debug(f"Batch Update Payload for GID {google_event_id} (UID {event.uid}): {json.dumps(event_data, indent=2)}")
            if event.is_master_event:
                logger.debug(f"  Master Event Details: UID={event.uid}, Recurrence={event_data.get('recurrence')}")
            elif event.recurrence_id:
                logger.debug(f"  Exception Details: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
            body += "--batch_boundary\n"
            body += "Content-Type: application/http\n"
            body += "Content-ID: <item{}>\n\n".format(uuid.uuid4())
            body += f"PUT /calendar/v3/calendars/{self.calendar_id}/events/{google_event_id}\n"
            body += "Content-Type: application/json\n\n"
            body += json.dumps(event_data) + "\n"
        body += "--batch_boundary--"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(batch_url, data=body.encode('utf-8'), headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Batch update successful. Response: {response_text}")
                        return True
                    else:
                        logger.error(f"Batch update failed: {response.status} - {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error in batch update: {e}", exc_info=True)
            return False

    async def batch_delete_events(self, google_event_ids: List[str]) -> bool:
        if not google_event_ids: return True
        logger.info(f"Batch deleting {len(google_event_ids)} Google events...")
        batch_url = "https://www.googleapis.com/batch/calendar/v3"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "multipart/mixed; boundary=batch_boundary"

        body = ""
        for google_event_id in google_event_ids:
            body += "--batch_boundary\n"
            body += "Content-Type: application/http\n"
            body += "Content-ID: <item{}>\n\n".format(uuid.uuid4())
            body += f"DELETE /calendar/v3/calendars/{self.calendar_id}/events/{google_event_id}\n"
        body += "--batch_boundary--"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(batch_url, data=body.encode('utf-8'), headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Batch delete successful. Response: {response_text}")
                        return True
                    else:
                        logger.error(f"Batch delete failed: {response.status} - {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error in batch delete: {e}", exc_info=True)
            return False