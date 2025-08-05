"""
Generic CalDAV client for CalDAV Mirror
"""

import asyncio
import aiohttp
import logging
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from icalendar import Calendar, Event

from .base import BaseCalDAVClient
from ..event_model import EventModel

logger = logging.getLogger(__name__)


class GenericCalDAVClient(BaseCalDAVClient):
    """Generic CalDAV client with support for multiple sync strategies."""

    def __init__(self, name: str, url: str, username: str, password: str, sync_method: str, database=None):
        super().__init__(name, url, username, password, database)
        self.sync_method = sync_method.lower()
        self._auth = aiohttp.BasicAuth(username, password)

    async def sync_events(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        logger.info(f"Syncing events for {self.name} using {self.sync_method}")
        if self.sync_method == 'sync-token':
            return await self._sync_with_sync_token()
        elif self.sync_method == 'ctag':
            return await self._sync_with_ctag()
        else:
            raise ValueError(f"Unsupported sync method: {self.sync_method}")

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
                        logger.error(f"Sync-token sync failed: {response.status}")
                        return [], [], None
                    
                    xml_content = await response.text()
                    events, deleted_uids, new_sync_token = await self._parse_sync_collection_response(xml_content)
                    
                    new_sync_state = {"sync_token": new_sync_token} if new_sync_token else None
                    return events, deleted_uids, new_sync_state
        except Exception as e:
            logger.error(f"Sync-token sync error: {e}")
            return [], [], None

    async def _sync_with_ctag(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        sync_state = await self.database.get_sync_state(self.name) if self.database else None
        current_ctag = sync_state.get('ctag') if sync_state else None

        propfind_body = '''<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
            <D:prop><CS:getctag /></D:prop>
        </D:propfind>'''
        headers = {'Content-Type': 'application/xml; charset=utf-8', 'Depth': '0'}

        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request('PROPFIND', self.url, data=propfind_body, headers=headers) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"CTag fetch failed: {response.status}")
                        return [], [], None
                    
                    xml_content = await response.text()
                    new_ctag = self._parse_ctag_response(xml_content)
                    
                    if new_ctag == current_ctag:
                        logger.info(f"No changes detected for {self.name} (ctag unchanged)")
                        return [], [], None
                    
                    events, deleted_uids, _ = await self._sync_with_sync_token()
                    new_sync_state = {"ctag": new_ctag}
                    return events, deleted_uids, new_sync_state
        except Exception as e:
            logger.error(f"CTag sync error: {e}")
            return [], [], None

    async def _parse_sync_collection_response(self, xml_content: str) -> Tuple[List[EventModel], List[str], Optional[str]]:
        events = []
        deleted_uids = []
        sync_token = None
        event_urls_to_fetch = []

        try:
            root = ET.fromstring(xml_content)
            namespaces = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav'}
            
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
                    calendar_data = response.find('.//C:calendar-data', namespaces)
                    if calendar_data is not None and calendar_data.text:
                        try:
                            cal = Calendar.from_ical(calendar_data.text)
                            for component in cal.walk():
                                if component.name == "VEVENT":
                                    events.append(EventModel.from_icalendar(component, self.name))
                        except Exception as e:
                            logger.error(f"Failed to parse calendar data: {e}")
                    else:
                        event_urls_to_fetch.append(urljoin(self.url, href.text))
        except ET.ParseError as e:
            logger.error(f"Failed to parse sync-collection response: {e}")

        if event_urls_to_fetch:
            fetched_events = await self._fetch_event_data(event_urls_to_fetch)
            events.extend(fetched_events)

        return events, deleted_uids, sync_token

    async def _fetch_event_data(self, urls: List[str]) -> List[EventModel]:
        tasks = [self._fetch_and_parse_event(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return [event for event in results if event]

    async def _fetch_and_parse_event(self, url: str) -> Optional[EventModel]:
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        event_text = await response.text()
                        cal = Calendar.from_ical(event_text)
                        for component in cal.walk():
                            if component.name == "VEVENT":
                                return EventModel.from_icalendar(component, self.name)
        except Exception as e:
            logger.error(f"Failed to fetch event data from {url}: {e}")
        return None

    def _parse_ctag_response(self, xml_content: str) -> Optional[str]:
        try:
            root = ET.fromstring(xml_content)
            namespaces = {'D': 'DAV:', 'CS': 'http://calendarserver.org/ns/'}
            ctag_elem = root.find('.//CS:getctag', namespaces)
            return ctag_elem.text if ctag_elem is not None else None
        except ET.ParseError as e:
            logger.error(f"Failed to parse ctag response: {e}")
            return None

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

    def _parse_calendar_query_response(self, xml_content: str) -> List[EventModel]:
        """Parse calendar-query REPORT response."""
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
                                events.append(EventModel.from_icalendar(component, self.name))
                    except Exception as e:
                        logger.error(f"Failed to parse calendar data: {e}")
        except ET.ParseError as e:
            logger.error(f"Failed to parse calendar query response: {e}")
        return events

    async def _find_deleted_events(self, current_events: List[EventModel]) -> List[str]:
        """Find events that were deleted by comparing with stored events."""
        if not self.database: return []
        current_uids = {event.uid for event in current_events}
        stored_events = await self.database.get_events_by_source(self.name)
        stored_uids = {event['caldav_uid'] for event in stored_events}
        return list(stored_uids - current_uids)