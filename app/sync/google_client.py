"""
Google Calendar client for CalDAV Mirror

Handles all interactions with the Google Calendar API, including creating,
updating, and deleting events in the destination calendar.
"""

import asyncio
import aiohttp
import logging
import json
import os
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

    def _log_api_call(self, action: str, http_method: str, url: str, payload: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Emit a structured JSON log for outbound Google Calendar API calls.
        Quiet by default: logs only a payload summary at INFO.
        Set GOOGLE_API_LOG_VERBOSE=true to include full payloads.
        """
        try:
            verbose = str(os.getenv("GOOGLE_API_LOG_VERBOSE", "false")).lower() in ("1", "true", "yes", "on")
            log_obj = {
                "type": "API_CALL",
                "action": action,
                "http_method": http_method,
                "url": url,
                "calendar_id": self.calendar_id,
                "context": context or {}
            }
            if verbose:
                log_obj["payload"] = payload
            else:
                log_obj["payload_summary"] = self._summarize_payload(action, payload)
            logger.info(json.dumps(log_obj, default=str))
        except Exception:
            logger.debug(f"[API_CALL][{action}] Failed to serialize payload for logging.")
    
    def _summarize_payload(self, action: str, payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Create a compact summary for API payloads to keep logs readable.
        - For batch operations: include counts only
        - For others: include serialized size
        """
        try:
            if payload is None:
                return None
            if isinstance(payload, dict):
                if "parts" in payload:
                    parts = payload.get("parts") or []
                    return {"parts_count": len(parts)}
                if "ids" in payload:
                    ids = payload.get("ids") or []
                    return {"ids_count": len(ids)}
            # Fallback: just report approximate size in bytes
            return {"size_bytes": len(json.dumps(payload, default=str))}
        except Exception:
            return {"summary": "unserializable"}

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

        # Structured API_CALL payload log
        self._log_api_call(
            action="events.insert",
            http_method="POST",
            url=url,
            payload=google_event_data,
            context={"uid": event.uid, "recurrence_id": event.recurrence_id}
        )

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

        # Structured API_CALL payload log
        self._log_api_call(
            action="events.update",
            http_method="PUT",
            url=url,
            payload=google_event_data,
            context={"uid": event.uid, "recurrence_id": event.recurrence_id, "google_event_id": google_event_id}
        )

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
                async with session.delete(url, headers=headers, params={'sendUpdates': 'none'}) as response:
                    if response.status in [204, 404, 410]: # 204 No Content, 404 Not Found, 410 Gone
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
                    # Default to only active events unless explicitly requested
                    if 'showDeleted' not in request_params:
                        request_params['showDeleted'] = "false"
                    
                    logger.debug(f"Requesting events with params: {request_params}")
                    async with session.get(url, headers=headers, params=request_params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Failed to list events: {response.status} - {error_text}")
                            break
                        data = await response.json()
                        items = data.get('items', [])
                        
                        # if logger.isEnabledFor(logging.DEBUG):
                        #     for item in items:
                        #         logger.debug(
                        #             f"[RAW GOOGLE EVENT] ID: {item.get('id')}, "
                        #             f"Summary: {item.get('summary')}, "
                        #             f"Status: {item.get('status')}, "
                        #             f"Start: {item.get('start', {}).get('dateTime')}, "
                        #             f"OriginalStart: {item.get('originalStartTime')}, "
                        #             f"ExtendedProps: {item.get('extendedProperties')}"
                        #         )

                        logger.debug(f"Received page with {len(items)} items.")

                        all_items.extend(items)
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

    async def list_all_mirrored_events(self, source_name: str) -> Tuple[Dict[Tuple[str, Optional[str]], Dict[str, Any]], Dict[Tuple[str, Optional[str]], Dict[str, Any]]]:
        """
        Fetch mirrored events for a source.
        Pure disown-before-delete model: only consider ACTIVE events and ignore CANCELLED tombstones.
        Returns (active_events, {}) for backward compatibility with call sites.
        """
        logger.info(f"Listing all mirrored events for source: {source_name}")
        params = {
            'privateExtendedProperty': f"caldav-mirror-source={source_name}",
            'maxResults': 2500
        }
        items = await self._list_events_paginated(params)

        active_events = {}
        collisions = 0
        resolved = 0

        for item in items:
            private_props = item.get('extendedProperties', {}).get('private', {})
            uid = private_props.get('caldav-mirror-uid')
            if not uid:
                continue

            status = (item.get('status') or '').upper()
            if status == 'CANCELLED':
                # Ignore tombstones entirely
                continue

            model = EventModel.from_google_event(item)
            if not model:
                continue

            key = (uid, model.recurrence_id)

            logger.debug(f"[Google KEY GEN] GID: {item.get('id')}, Key: {key}, Status: {status}")

            if key in active_events:
                collisions += 1
                existing = active_events[key]
                existing_has_recur = bool(existing.get('recurrence'))
                candidate_has_recur = bool(item.get('recurrence'))

                # Prefer the candidate that declares recurrence if conflict
                prefer_candidate = candidate_has_recur and not existing_has_recur
                chosen = item if prefer_candidate else existing
                dropped = existing if prefer_candidate else item
                logger.debug(
                    f"[Google KEY COLLISION][RESOLVE] Key={key} "
                    f"kept_id={chosen.get('id')} "
                    f"dropped_id={dropped.get('id')}"
                )
                active_events[key] = chosen
                resolved += 1
            else:
                active_events[key] = item

        if collisions:
            logger.debug(f"[Google KEY COLLISION][DIAG] Total collisions observed: {collisions}, resolved={resolved}")

        logger.info(f"Found {len(active_events)} active mirrored events for source: {source_name}")
        return active_events, {}

    async def list_cancelled_by_source(self, source_name: str) -> Dict[Tuple[str, Optional[str]], Dict[str, Any]]:
        """
        Return CANCELLED Google events for a source, keyed by (uid, recurrence_id).
        Used only to detect unauthorized deletions or edits on Google so we can
        trigger a disown->delete->recreate flow for the affected series.
        """
        logger.info(f"Listing CANCELLED events for source: {source_name}")
        params = {
            'privateExtendedProperty': f"caldav-mirror-source={source_name}",
            'maxResults': 2500,
            'showDeleted': "true"
        }
        items = await self._list_events_paginated(params)

        cancelled: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
        for item in items:
            status = (item.get('status') or '').upper()
            if status != 'CANCELLED':
                continue
            private_props = item.get('extendedProperties', {}).get('private', {})
            uid = private_props.get('caldav-mirror-uid')
            if not uid:
                continue

            model = EventModel.from_google_event(item)
            if not model:
                continue

            key = (uid, model.recurrence_id)
            cancelled[key] = item

        logger.info(f"Found {len(cancelled)} CANCELLED events for source: {source_name}")
        return cancelled

    async def list_events_for_uid(self, source_name: str, uid: str, include_cancelled: bool = False) -> List[Dict[str, Any]]:
        """
        List Google Calendar events for a specific source + CalDAV UID.

        Args:
            include_cancelled: When True, include CANCELLED items (for detection/disown). Default False returns active only.
        """
        logger.info(f"Listing events for source '{source_name}', UID={uid}")
        params = {
            'privateExtendedProperty': [f"caldav-mirror-source={source_name}", f"caldav-mirror-uid={uid}"],
            'maxResults': 2500,
            'showDeleted': "true" if include_cancelled else None
        }
        items = await self._list_events_paginated(params)
        return items

    async def purge_tombstone(self, google_event_id: str) -> bool:
        """
        Deprecated: Tombstone handling has been removed in the pure disown-before-delete model.
        This stub remains for compatibility and no-ops successfully.
        """
        logger.debug(f"purge_tombstone({google_event_id}) called, but tombstone handling is disabled.")
        return True

    async def disown_event(self, google_event_id: str) -> bool:
        """
        Disown a Google event by clearing CalDAV Mirror extendedProperties, so any subsequent
        CANCELLED tombstones created by deletion won't be matched by our privateExtendedProperty filter.
        """
        logger.info(f"Disowning Google event ID: {google_event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "application/json"

        payload = {
            # Replace the private map with an empty dict to remove 'caldav-mirror-*' keys
            "extendedProperties": {
                "private": {}
            }
        }

        # Structured API_CALL payload log
        self._log_api_call(
            action="events.patch",
            http_method="PATCH",
            url=url,
            payload=payload,
            context={"purpose": "disown", "google_event_id": google_event_id}
        )

        try:
            async with aiohttp.ClientSession() as session:
                # PATCH so we don't need to send the full event body
                async with session.patch(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        logger.info(f"Successfully disowned Google event ID: {google_event_id}")
                        return True
                    elif response.status in [404, 410]:
                        # Already gone; treat as disowned
                        logger.info(f"Event {google_event_id} not found while disowning; treating as already disowned.")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to disown Google event: {response.status} - {error_text}")
                        return False
        except Exception as e:
            logger.error(f"Error disowning Google event: {e}", exc_info=True)
            return False

    async def get_event_instances(self, google_event_id: str) -> List[Dict[str, Any]]:
        """Fetches all instances for a given recurring event ID."""
        logger.info(f"Fetching instances for Google event ID: {google_event_id}")
        instances = []
        page_token = None
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{google_event_id}/instances"
        headers = await self._get_auth_headers()
        params = {'maxResults': 2500, 'showDeleted': "true"}

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
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Received page with {len(items)} items.")
                        
                        instances.extend(items)
                        page_token = data.get('nextPageToken')
                        if not page_token:
                            break
        except Exception as e:
            logger.error(f"Error fetching event instances: {e}", exc_info=True)

        logger.info(f"Found {len(instances)} instances for Google event ID: {google_event_id}")
        return instances

    async def list_events_by_icaluid(self, ical_uid: str, include_cancelled: bool = False) -> List[Dict[str, Any]]:
        """
        List all events across the calendar that share the given iCalUID.
        Useful for diagnosing split-series where a successor series lacks our ownership property.
        """
        logger.info(f"Listing events by iCalUID: {ical_uid} include_cancelled={include_cancelled}")
        params = {
            'iCalUID': ical_uid,
            'showDeleted': "true" if include_cancelled else "false",
            'maxResults': 2500,
            'fields': 'items(id,iCalUID,recurringEventId,recurrence,sequence,status,summary,updated,start,end,extendedProperties/private),nextPageToken',
        }
        items = await self._list_events_paginated(params)
        logger.info(f"Found {len(items)} items for iCalUID {ical_uid}")
        return items

    async def list_events_window(self, time_min: str, time_max: str, single_events: bool = True) -> List[Dict[str, Any]]:
        """
        List events in a narrow time window across the entire calendar (not restricted to our privateExtendedProperty).
        Useful for detecting split-successor series (R2) by probing around a specific timestamp.

        Args:
            time_min: RFC3339 UTC (e.g., 2024-01-01T00:00:00Z)
            time_max: RFC3339 UTC
            single_events: When true, expands recurring instances (required for orderBy=startTime)

        Returns:
            List of raw Google event dicts
        """
        logger.info(f"Listing events window: {time_min} to {time_max}, singleEvents={single_events}")
        params = {
            'timeMin': time_min,
            'timeMax': time_max,
            'singleEvents': "true" if single_events else None,
            'orderBy': "startTime" if single_events else None,
            'showDeleted': "false",
            'maxResults': 2500,
            'fields': 'items(id,iCalUID,recurringEventId,originalStartTime,recurrence,status,start,end,summary,extendedProperties/private),nextPageToken',
        }
        items = await self._list_events_paginated(params)
        logger.info(f"Found {len(items)} items in window.")
        return items

    async def get_event_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single Google Calendar event by ID with a minimal field set.
        Used to fetch the master of a recurring instance candidate during split detection.
        """
        logger.debug(f"Fetching Google event by id: {event_id}")
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events/{event_id}"
        headers = await self._get_auth_headers()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params={
                    'fields': 'id,iCalUID,recurrence,summary,start,end,updated,sequence,extendedProperties/private'
                }) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status in [404, 410]:
                        logger.info(f"Event {event_id} not found (status={response.status})")
                        return None
                    else:
                        txt = await response.text()
                        logger.error(f"get_event_by_id failed: {response.status} - {txt}")
                        return None
        except Exception as e:
            logger.error(f"Error in get_event_by_id({event_id}): {e}", exc_info=True)
            return None
    async def is_calendar_empty(self) -> bool:
        """
        Return True if the destination Google Calendar has no active (non-deleted) events.
        Ignores CANCELLED tombstones and emits diagnostics for confirmation.
        """
        logger.info("Checking if Google Calendar is empty (active events only, full scan)...")
        try:
            # Perform a definitive active-only scan. This excludes CANCELLED tombstones by design.
            items_active = await self.list_all_events_active()
            empty = len(items_active) == 0

            # Emit structured diagnostics with a small sample of IDs
            try:
                logger.info(json.dumps({
                    "type": "CALENDAR_EMPTY_CHECK",
                    "calendar_id": self.calendar_id,
                    "method": "full_active_scan",
                    "active_count": len(items_active),
                    "decision_empty": empty,
                    "active_sample_ids": [it.get('id') for it in items_active[:3]]
                }))
            except Exception:
                pass

            return empty
        except Exception as e:
            logger.error(f"Failed to check calendar emptiness: {e}", exc_info=True)
            # Be conservative: treat as not empty if we cannot determine
            return False

    async def list_all_events_active(self) -> List[Dict[str, Any]]:
        """
        List all active (non-deleted) events in the destination calendar, without filtering
        by privateExtendedProperty. This is used for the global sweep that removes any events
        that do not originate from our database.
        """
        logger.info("Listing all active events in destination calendar (no filtering).")
        params = {
            'maxResults': 2500,
            'showDeleted': "false",
            # Include originalStartTime so exceptions can be correctly keyed by RECURRENCE-ID
            'fields': 'items(id,iCalUID,recurringEventId,originalStartTime,recurrence,status,summary,start,end,extendedProperties/private),nextPageToken',
        }
        items = await self._list_events_paginated(params)
        logger.info(f"Found {len(items)} active events in destination calendar.")
        return items
    async def batch_create_events(self, events: List[EventModel]) -> Dict[Tuple[str, Optional[str]], str]:
        if not events: return {}
        logger.info(f"Batch creating {len(events)} Google events...")
        batch_url = "https://www.googleapis.com/batch/calendar/v3"
        headers = await self._get_auth_headers()
        headers["Content-Type"] = "multipart/mixed; boundary=batch_boundary"

        body = ""
        payloads: List[Dict[str, Any]] = []
        for event in events:
            event_data = event.to_google_event()
            payloads.append(event_data)
            logger.debug(f"Batch Create Payload for UID {event.uid}: {json.dumps(event_data, indent=2)}")
            if event.is_master_event:
                logger.debug(f"  Master Event Details: UID={event.uid}, Recurrence={event_data.get('recurrence')}")
            elif event.recurrence_id:
                logger.debug(f"  Exception Details: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
                # DIAG: Exceptions should include originalStartTime to suppress the generated instance
                if 'originalStartTime' not in event_data:
                    logger.warning(f"[Google PUSH][DIAG] Batch create exception missing originalStartTime: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
                else:
                    logger.debug(f"[Google PUSH][DIAG] Batch create exception has originalStartTime: UID={event.uid}, RecurrenceID={event.recurrence_id}, originalStartTime={event_data.get('originalStartTime')}")
            body += "--batch_boundary\n"
            body += "Content-Type: application/http\n"
            body += "Content-ID: <item{}>\n\n".format(uuid.uuid4())
            body += f"POST /calendar/v3/calendars/{self.calendar_id}/events\n"
            body += "Content-Type: application/json\n\n"
            body += json.dumps(event_data) + "\n"
        body += "--batch_boundary--"

        # Structured API_CALL payload log (batch)
        self._log_api_call(
            action="batch/events.insert",
            http_method="POST",
            url=batch_url,
            payload={"parts": payloads},
            context={"count": len(events)}
        )

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
            # DIAG: Extract per-part status and content-id for better visibility
            status_code = None
            content_id = None
            for line in part.splitlines():
                if line.startswith('HTTP/1.1 '):
                    try:
                        status_code = int(line.split()[1])
                    except Exception:
                        status_code = None
                if line.lower().startswith('content-id:'):
                    content_id = line.split(':', 1)[1].strip()

            # Attempt to correlate to original event using current index prior to increment
            ev = original_events[event_idx] if event_idx < len(original_events) else None
            ev_uid = ev.uid if ev else None
            ev_rid = ev.recurrence_id if ev else None

            if status_code is not None:
                if status_code != 200:
                    logger.warning(f"[BATCH CREATE][DIAG] Non-200 part. Status={status_code}, Content-ID={content_id}, For UID={ev_uid}, RID={ev_rid}. Part snippet: {part[:500]}")
                else:
                    logger.debug(f"[BATCH CREATE][DIAG] 200 OK part for UID={ev_uid}, RID={ev_rid}, Content-ID={content_id}")

            if '"id":' in part and '200 OK' in part:
                try:
                    json_part = part[part.find('{'):part.rfind('}') + 1]
                    data = json.loads(json_part)
                    if 'id' in data and event_idx < len(original_events):
                        event = original_events[event_idx]
                        key = (event.uid, event.recurrence_id)
                        results[key] = data['id']
                except json.JSONDecodeError:
                    logger.warning(f"[BATCH CREATE][DIAG] JSON decode failed for UID={ev_uid}, RID={ev_rid}. Part snippet: {part[:500]}")
                    continue
            else:
                # DIAG: 200 OK but no 'id' found can indicate an error payload or partial failure
                if status_code == 200 and '"id":' not in part:
                    logger.debug(f"[BATCH CREATE][DIAG] 200 OK part without id for UID={ev_uid}, RID={ev_rid}. Part snippet: {part[:300]}")

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
        payloads: List[Dict[str, Any]] = []
        for google_event_id, event in events_to_update:
            event_data = event.to_google_event()
            payloads.append({"google_event_id": google_event_id, "event": event_data})
            logger.debug(f"Batch Update Payload for GID {google_event_id} (UID {event.uid}): {json.dumps(event_data, indent=2)}")
            if event.is_master_event:
                logger.debug(f"  Master Event Details: UID={event.uid}, Recurrence={event_data.get('recurrence')}")
            elif event.recurrence_id:
                logger.debug(f"  Exception Details: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
                # DIAG: Exceptions should include originalStartTime to suppress the generated instance
                if 'originalStartTime' not in event_data:
                    logger.warning(f"[Google PUSH][DIAG] Batch update exception missing originalStartTime: UID={event.uid}, RecurrenceID={event.recurrence_id}, recurringEventId={event_data.get('recurringEventId')}")
                else:
                    logger.debug(f"[Google PUSH][DIAG] Batch update exception has originalStartTime: UID={event.uid}, RecurrenceID={event.recurrence_id}, originalStartTime={event_data.get('originalStartTime')}")
            body += "--batch_boundary\n"
            body += "Content-Type: application/http\n"
            body += "Content-ID: <item{}>\n\n".format(uuid.uuid4())
            body += f"PUT /calendar/v3/calendars/{self.calendar_id}/events/{google_event_id}\n"
            body += "Content-Type: application/json\n\n"
            body += json.dumps(event_data) + "\n"
        body += "--batch_boundary--"

        # Structured API_CALL payload log (batch)
        self._log_api_call(
            action="batch/events.update",
            http_method="POST",
            url=batch_url,
            payload={"parts": payloads},
            context={"count": len(events_to_update)}
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(batch_url, data=body.encode('utf-8'), headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Batch update successful.")
                        logger.debug(f"Response: {response_text}")
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
            body += f"DELETE /calendar/v3/calendars/{self.calendar_id}/events/{google_event_id}?sendUpdates=none\n"
        body += "--batch_boundary--"

        # Structured API_CALL payload log (batch delete)
        self._log_api_call(
            action="batch/events.delete",
            http_method="POST",
            url=batch_url,
            payload={"ids": google_event_ids},
            context={"count": len(google_event_ids)}
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(batch_url, data=body.encode('utf-8'), headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Batch delete successful.")
                        logger.debug(f"Response: {response_text}")
                        return True
                    else:
                        logger.error(f"Batch delete failed: {response.status} - {response_text}")
                        return False
        except Exception as e:
            logger.error(f"Error in batch delete: {e}", exc_info=True)
            return False
    async def list_owned_events_in_window(self, source_name: str, time_min: str, time_max: str) -> Tuple[Dict[Tuple[str, Optional[str]], EventModel], Dict[Tuple[str, Optional[str]], Dict[str, Any]]]:
        """
        Return our owned events within a window, keyed by (uid, recurrence_id).

        Filters by extendedProperties.private.caldav-mirror-source == source_name and status != CANCELLED.
        """
        items = await self.list_events_window(time_min, time_max, single_events=True)
        models: Dict[Tuple[str, Optional[str]], EventModel] = {}
        raws: Dict[Tuple[str, Optional[str]], Dict[str, Any]] = {}
        for it in items:
            status = (it.get('status') or '').upper()
            if status == 'CANCELLED':
                continue
            private = ((it.get('extendedProperties') or {}).get('private')) or {}
            if private.get('caldav-mirror-source') != source_name:
                continue
            model = EventModel.from_google_event(it)
            if not model or not model.uid:
                continue
            key = (model.uid, model.recurrence_id)
            models[key] = model
            raws[key] = it
        return models, raws

    async def fetch_next_sync_token(self) -> Optional[str]:
        """
        Perform a full incremental baseline scan to obtain nextSyncToken for the calendar.
        This is a lightweight scan (fields limited). Returns the token or None.
        """
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events"
        headers = await self._get_auth_headers()
        params: Dict[str, Any] = {
            'showDeleted': "true",
            'maxResults': 2500,
            # Limit fields for speed: only need pagination + token + minimal items
            'fields': 'nextPageToken,nextSyncToken,items/id'
        }

        next_page = None
        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    if next_page:
                        params['pageToken'] = next_page
                    async with session.get(url, headers=headers, params=params) as resp:
                        if resp.status != 200:
                            txt = await resp.text()
                            logger.error(f"fetch_next_sync_token failed: {resp.status} - {txt}")
                            return None
                        data = await resp.json()
                        next_page = data.get('nextPageToken')
                        if not next_page:
                            return data.get('nextSyncToken')
        except Exception as e:
            logger.error(f"Error in fetch_next_sync_token: {e}", exc_info=True)
            return None

    async def has_changes_since(self, sync_token: str) -> Tuple[bool, Optional[str]]:
        """
        Check whether there are any changes since the provided sync token.

        Returns:
            (changed, next_sync_token or None if token invalid/expired)
        """
        url = f"{self.API_BASE_URL}/calendars/{self.calendar_id}/events"
        headers = await self._get_auth_headers()
        params: Dict[str, Any] = {
            'syncToken': sync_token,
            'showDeleted': "true",
            'maxResults': 2500,
            'fields': 'nextPageToken,nextSyncToken,items/id,items/status'
        }

        next_page = None
        any_items = False
        try:
            async with aiohttp.ClientSession() as session:
                while True:
                    if next_page:
                        params['pageToken'] = next_page
                    async with session.get(url, headers=headers, params=params) as resp:
                        if resp.status == 410:
                            # Token too old/invalid
                            logger.info("Google sync token expired (410). Full baseline required.")
                            return True, None
                        if resp.status != 200:
                            txt = await resp.text()
                            logger.error(f"has_changes_since failed: {resp.status} - {txt}")
                            # Be conservative: assume changes
                            return True, None
                        data = await resp.json()
                        items = data.get('items', [])
                        if items:
                            any_items = True
                        next_page = data.get('nextPageToken')
                        if not next_page:
                            return any_items, data.get('nextSyncToken')
        except Exception as e:
            logger.error(f"Error in has_changes_since: {e}", exc_info=True)
            # Be conservative: assume changes
            return True, None


    async def list_all_owned_events_raw(self, source_name: str) -> List[Dict[str, Any]]:
        """
        List all ACTIVE events owned by this source across the entire calendar, without grouping/dedup.
        Useful for garbage-collecting out-of-window instances in the flattened projection model.
        """
        params: Dict[str, Any] = {
            'privateExtendedProperty': f"caldav-mirror-source={source_name}",
            'maxResults': 2500,
            'showDeleted': "false",
            'fields': 'items(id,recurrence,recurringEventId,status,start,end,extendedProperties/private),nextPageToken'
        }
        items = await self._list_events_paginated(params)
        return items