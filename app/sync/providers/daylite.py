"""
Daylite CalDAV client for CalDAV Mirror
"""

import aiohttp
import logging
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from icalendar import Calendar

from .base import BaseCalDAVClient
from ..event_model import EventModel

logger = logging.getLogger(__name__)


class DayliteCalDAVClient(BaseCalDAVClient):
    """Daylite-specific CalDAV client."""

    def __init__(self, name: str, url: str, username: str, password: str, sync_method: str, database=None):
        super().__init__(name, url, username, password, database)
        self.sync_method = sync_method.lower()
        self._auth = aiohttp.BasicAuth(username, password)

    async def sync_events(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        # Daylite supports sync-token, so we'll use that.
        return await self._sync_with_sync_token()

    async def _sync_with_sync_token(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        sync_state = await self.database.get_sync_state(self.name) if self.database else None
        sync_token = sync_state.get('sync_token') if sync_state else None

        sync_body = f'''<?xml version="1.0" encoding="utf-8" ?>
        <D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:sync-token>{sync_token or ''}</D:sync-token>
            <D:sync-level>1</D:sync-level>
            <D:prop>
                <D:getetag />
                <C:calendar-data />
            </D:prop>
        </D:sync-collection>'''

        headers = {'Content-Type': 'application/xml; charset=utf-8', 'Depth': '1'}

        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request('REPORT', self.url, data=sync_body, headers=headers) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"Daylite sync failed: {response.status}")
                        return [], [], None
                    
                    xml_content = await response.text()
                    events, deleted_uids, new_sync_token = self._parse_sync_collection_response(xml_content)
                    
                    new_sync_state = {"sync_token": new_sync_token} if new_sync_token else None
                    return events, deleted_uids, new_sync_state
        except Exception as e:
            logger.error(f"Daylite sync error: {e}")
            return [], [], None

    def _parse_sync_collection_response(self, xml_content: str) -> Tuple[List[EventModel], List[str], Optional[str]]:
        events = []
        deleted_uids = []
        sync_token = None

        try:
            root = ET.fromstring(xml_content)
            namespaces = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav', 'A': 'urn:ietf:params:xml:ns:caldav'}
            
            sync_token_elem = root.find('.//D:sync-token', namespaces)
            if sync_token_elem is not None:
                sync_token = sync_token_elem.text

            for response in root.findall('.//D:response', namespaces):
                href = response.find('D:href', namespaces)
                if href is None: continue
                status = response.find('.//D:status', namespaces)
                if status is None: continue

                if '404' in status.text:
                    resource_uid = self._extract_uid_from_href(href.text)
                    if resource_uid: deleted_uids.append(resource_uid)
                elif '200' in status.text:
                    calendar_data = response.find('.//A:calendar-data', namespaces)
                    if calendar_data is not None and calendar_data.text:
                        try:
                            cal = Calendar.from_ical(calendar_data.text)
                            for component in cal.walk():
                                if component.name == "VEVENT":
                                    events.append(EventModel.from_icalendar(component))
                        except Exception as e:
                            logger.error(f"Failed to parse calendar data from {href.text}: {e}")
                    else:
                        logger.warning(f"No calendar data found for {href.text}")
        except ET.ParseError as e:
            logger.error(f"Failed to parse sync-collection response: {e}")

        return events, deleted_uids, sync_token

    def _extract_uid_from_href(self, href: str) -> Optional[str]:
        parts = href.rstrip('/').split('/')
        if parts:
            filename = parts[-1]
            return filename[:-4] if filename.endswith('.ics') else filename
        return None

    async def test_connection(self) -> bool:
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.options(self.url) as response:
                    return response.status < 400
        except Exception as e:
            logger.error(f"Connection test failed for {self.name}: {e}")
            return False