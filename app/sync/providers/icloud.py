"""
iCloud CalDAV client for CalDAV Mirror
"""

import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin
import xml.etree.ElementTree as ET
from icalendar import Calendar

from .base import BaseCalDAVClient
from ..event_model import EventModel

logger = logging.getLogger(__name__)


class iCloudCalDAVClient(BaseCalDAVClient):
    """iCloud-specific CalDAV client."""

    def __init__(self, name: str, url: str, username: str, password: str, sync_method: str, database=None):
        super().__init__(name, url, username, password, database)
        self._auth = aiohttp.BasicAuth(username, password)

    async def sync_events(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        # iCloud supports sync-token, but we'll use calendar-query for robustness
        return await self._sync_with_calendar_query()

    async def _sync_with_calendar_query(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        sync_body = """<?xml version="1.0" encoding="utf-8" ?>
        <C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:prop>
                <D:getetag />
                <C:calendar-data />
            </D:prop>
            <C:filter>
                <C:comp-filter name="VCALENDAR" />
            </C:filter>
        </C:calendar-query>"""
        headers = {'Content-Type': 'application/xml; charset=utf-8', 'Depth': '1'}

        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request('REPORT', self.url, data=sync_body, headers=headers) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"iCloud sync failed: {response.status}")
                        return [], [], None
                    
                    xml_content = await response.text()
                    events, deleted_uids, sync_token = self._parse_calendar_query_response(xml_content)
                    
                    # We don't get a sync token from this query, so we'll use ctag as the sync state
                    new_ctag = await self._get_ctag()
                    new_sync_state = {"ctag": new_ctag} if new_ctag else None

                    return events, deleted_uids, new_sync_state
        except Exception as e:
            logger.error(f"iCloud sync error: {e}")
            return [], [], None

    def _parse_calendar_query_response(self, xml_content: str) -> Tuple[List[EventModel], List[str], Optional[str]]:
        events = []
        try:
            root = ET.fromstring(xml_content)
            namespaces = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav'}
            for response in root.findall('.//D:response', namespaces):
                calendar_data = response.find('.//C:calendar-data', namespaces)
                if calendar_data is not None and calendar_data.text:
                    try:
                        cal = Calendar.from_ical(calendar_data.text)
                        for component in cal.walk():
                            if component.name == "VEVENT":
                                events.append(EventModel.from_icalendar(component))
                    except Exception as e:
                        logger.error(f"Failed to parse calendar data: {e}")
        except ET.ParseError as e:
            logger.error(f"Failed to parse calendar query response: {e}")
        
        # We don't get deleted UIDs from this query, so we have to find them by comparing
        deleted_uids = asyncio.run(self._find_deleted_events(events))
        return events, deleted_uids, None

    async def _get_ctag(self) -> Optional[str]:
        propfind_body = '''<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
            <D:prop><CS:getctag /></D:prop>
        </D:propfind>'''
        headers = {'Content-Type': 'application/xml; charset=utf-8', 'Depth': '0'}
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request('PROPFIND', self.url, data=propfind_body, headers=headers) as response:
                    if response.status in [200, 207]:
                        xml_content = await response.text()
                        root = ET.fromstring(xml_content)
                        namespaces = {'D': 'DAV:', 'CS': 'http://calendarserver.org/ns/'}
                        ctag_elem = root.find('.//CS:getctag', namespaces)
                        return ctag_elem.text if ctag_elem is not None else None
        except Exception as e:
            logger.error(f"Could not fetch ctag: {e}")
        return None

    async def _find_deleted_events(self, current_events: List[EventModel]) -> List[str]:
        if not self.database: return []
        current_uids = {event.uid for event in current_events}
        stored_events = await self.database.get_events_by_source(self.name)
        stored_uids = {event['caldav_uid'] for event in stored_events}
        return list(stored_uids - current_uids)

    async def test_connection(self) -> bool:
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.options(self.url) as response:
                    return response.status < 400
        except Exception as e:
            logger.error(f"Connection test failed for {self.name}: {e}")
            return False