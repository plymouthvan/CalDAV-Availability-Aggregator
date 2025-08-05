"""
CalDAV client for CalDAV Mirror

Handles CalDAV server communication and implements different sync strategies
(sync-token, ctag, gtag) for efficient event fetching.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from icalendar import Calendar, Event
import base64

from .event_model import EventModel

logger = logging.getLogger(__name__)


class CalDAVClient:
    """CalDAV client with support for multiple sync strategies."""
    
    def __init__(self, name: str, url: str, username: str, password: str, 
                 sync_method: str, database=None):
        self.name = name
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.sync_method = sync_method.lower()
        self.database = database
        
        # Validate sync method
        if self.sync_method not in ['sync-token', 'ctag', 'gtag']:
            raise ValueError(f"Unsupported sync method: {sync_method}")
        
        # HTTP session with authentication
        self._auth = aiohttp.BasicAuth(username, password)
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def discover_calendars(self) -> List[Dict[str, str]]:
        """
        Discover available calendars on the CalDAV server.
        
        Returns:
            List of calendar dictionaries with 'name', 'url', and 'displayname'
        """
        logger.info(f"Discovering calendars for {self.name}")
        
        # PROPFIND request to discover calendars
        propfind_body = '''<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:prop>
                <D:displayname />
                <D:resourcetype />
                <C:supported-calendar-component-set />
            </D:prop>
        </D:propfind>'''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
            'Depth': '1'
        }
        
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request(
                    'PROPFIND', self.url, data=propfind_body, headers=headers
                ) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"Calendar discovery failed: {response.status}")
                        return []
                    
                    xml_content = await response.text()
                    return self._parse_calendar_discovery(xml_content)
                
        except Exception as e:
            logger.error(f"Calendar discovery error: {e}")
            return []
    
    def _parse_calendar_discovery(self, xml_content: str) -> List[Dict[str, str]]:
        """Parse PROPFIND response to extract calendar information."""
        calendars = []
        
        try:
            root = ET.fromstring(xml_content)
            
            # Define namespaces
            namespaces = {
                'D': 'DAV:',
                'C': 'urn:ietf:params:xml:ns:caldav'
            }
            
            for response in root.findall('.//D:response', namespaces):
                href = response.find('D:href', namespaces)
                if href is None:
                    continue
                
                calendar_url = href.text
                
                # Check if this is a calendar collection
                resourcetype = response.find('.//D:resourcetype', namespaces)
                if resourcetype is None:
                    continue
                
                is_calendar = resourcetype.find('C:calendar', namespaces) is not None
                if not is_calendar:
                    continue
                
                # Get display name
                displayname_elem = response.find('.//D:displayname', namespaces)
                displayname = displayname_elem.text if displayname_elem is not None else calendar_url
                
                # Check if it supports VEVENT components
                supported_components = response.find('.//C:supported-calendar-component-set', namespaces)
                supports_events = True  # Assume true if not specified
                
                if supported_components is not None:
                    supports_events = any(
                        comp.get('name') == 'VEVENT' 
                        for comp in supported_components.findall('.//C:comp', namespaces)
                    )
                
                if supports_events:
                    calendars.append({
                        'name': calendar_url.split('/')[-1] or calendar_url.split('/')[-2],
                        'url': urljoin(self.url, calendar_url),
                        'displayname': displayname
                    })
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse calendar discovery response: {e}")
        
        logger.info(f"Discovered {len(calendars)} calendars for {self.name}")
        return calendars
    
    async def sync_events(self) -> Tuple[List[EventModel], List[str]]:
        """
        Sync events using the configured sync method.
        
        Returns:
            Tuple of (new/updated events, deleted event UIDs)
        """
        logger.info(f"Syncing events for {self.name} using {self.sync_method}")
        
        if self.sync_method == 'sync-token':
            return await self._sync_with_sync_token()
        elif self.sync_method == 'ctag':
            return await self._sync_with_ctag()
        elif self.sync_method == 'gtag':
            return await self._sync_with_gtag()
        else:
            raise ValueError(f"Unsupported sync method: {self.sync_method}")
    
    async def _sync_with_sync_token(self) -> Tuple[List[EventModel], List[str]]:
        """Sync using WebDAV sync-token method (RFC 6578)."""
        # Get current sync state
        sync_state = await self.database.get_sync_state(self.name) if self.database else None
        sync_token = sync_state.get('sync_token') if sync_state else None
        
        # Build sync-collection REPORT request
        if sync_token:
            sync_body = f'''<?xml version="1.0" encoding="utf-8" ?>
            <D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
                <D:sync-token>{sync_token}</D:sync-token>
                <D:sync-level>1</D:sync-level>
                <D:prop>
                    <D:getetag />
                    <C:calendar-data />
                </D:prop>
            </D:sync-collection>'''
        else:
            # Initial sync
            sync_body = '''<?xml version="1.0" encoding="utf-8" ?>
            <D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
                <D:sync-token />
                <D:sync-level>1</D:sync-level>
                <D:prop>
                    <D:getetag />
                    <C:calendar-data />
                </D:prop>
            </D:sync-collection>'''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
            'Depth': '1'
        }
        
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request(
                    'REPORT', self.url, data=sync_body, headers=headers
                ) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"Sync-token sync failed: {response.status}")
                        return [], []
                    
                    xml_content = await response.text()
                    events, deleted_uids, new_sync_token = self._parse_sync_collection_response(xml_content)
                    
                    # Update sync state
                    if new_sync_token and self.database:
                        await self.database.update_sync_state(
                            self.name, 'sync-token', sync_token=new_sync_token
                        )
                    
                    return events, deleted_uids
                
        except Exception as e:
            logger.error(f"Sync-token sync error: {e}")
            return [], []
    
    async def _sync_with_ctag(self) -> Tuple[List[EventModel], List[str]]:
        """Sync using CalDAV ctag method."""
        # Get current sync state
        sync_state = await self.database.get_sync_state(self.name) if self.database else None
        current_ctag = sync_state.get('ctag') if sync_state else None
        
        # Get collection ctag
        propfind_body = '''<?xml version="1.0" encoding="utf-8" ?>
        <D:propfind xmlns:D="DAV:" xmlns:CS="http://calendarserver.org/ns/">
            <D:prop>
                <CS:getctag />
            </D:prop>
        </D:propfind>'''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
            'Depth': '0'
        }
        
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request(
                    'PROPFIND', self.url, data=propfind_body, headers=headers
                ) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"CTag fetch failed: {response.status}")
                        return [], []
                    
                    xml_content = await response.text()
                    new_ctag = self._parse_ctag_response(xml_content)
                    
                    if new_ctag == current_ctag:
                        logger.info(f"No changes detected for {self.name} (ctag unchanged)")
                        return [], []
                    
                    # CTag changed, fetch all events
                    events = await self._fetch_all_events()
                    
                    # Update sync state
                    if self.database:
                        await self.database.update_sync_state(
                            self.name, 'ctag', ctag=new_ctag
                        )
                    
                    # For ctag, we need to compare with existing events to find deletions
                    deleted_uids = await self._find_deleted_events(events)
                    
                    return events, deleted_uids
                
        except Exception as e:
            logger.error(f"CTag sync error: {e}")
            return [], []
    
    async def _sync_with_gtag(self) -> Tuple[List[EventModel], List[str]]:
        """Sync using Google-style gtag method (if supported)."""
        # This is a placeholder for gtag implementation
        # Most CalDAV servers don't support gtag, but some Google-compatible ones might
        logger.warning(f"GTag sync not yet implemented for {self.name}")
        return [], []
    
    def _parse_sync_collection_response(self, xml_content: str) -> Tuple[List[EventModel], List[str], Optional[str]]:
        """Parse sync-collection REPORT response."""
        events = []
        deleted_uids = []
        sync_token = None
        
        try:
            root = ET.fromstring(xml_content)
            
            namespaces = {
                'D': 'DAV:',
                'C': 'urn:ietf:params:xml:ns:caldav'
            }
            
            # Extract new sync token
            sync_token_elem = root.find('.//D:sync-token', namespaces)
            if sync_token_elem is not None:
                sync_token = sync_token_elem.text
            
            # Process responses
            for response in root.findall('.//D:response', namespaces):
                href = response.find('D:href', namespaces)
                if href is None:
                    continue
                
                status = response.find('.//D:status', namespaces)
                if status is None:
                    continue
                
                status_code = status.text
                
                if '404' in status_code:
                    # Deleted resource
                    resource_uid = self._extract_uid_from_href(href.text)
                    if resource_uid:
                        deleted_uids.append(resource_uid)
                elif '200' in status_code:
                    # Updated/new resource
                    calendar_data = response.find('.//C:calendar-data', namespaces)
                    if calendar_data is not None and calendar_data.text:
                        try:
                            cal = Calendar.from_ical(calendar_data.text)
                            for component in cal.walk():
                                if component.name == "VEVENT":
                                    event = EventModel.from_icalendar(component)
                                    events.append(event)
                        except Exception as e:
                            logger.error(f"Failed to parse calendar data: {e}")
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse sync-collection response: {e}")
        
        return events, deleted_uids, sync_token
    
    def _parse_ctag_response(self, xml_content: str) -> Optional[str]:
        """Parse PROPFIND response to extract ctag."""
        try:
            root = ET.fromstring(xml_content)
            
            namespaces = {
                'D': 'DAV:',
                'CS': 'http://calendarserver.org/ns/'
            }
            
            ctag_elem = root.find('.//CS:getctag', namespaces)
            return ctag_elem.text if ctag_elem is not None else None
            
        except ET.ParseError as e:
            logger.error(f"Failed to parse ctag response: {e}")
            return None
    
    async def _fetch_all_events(self) -> List[EventModel]:
        """Fetch all events from the calendar."""
        calendar_query_body = '''<?xml version="1.0" encoding="utf-8" ?>
        <C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:prop>
                <D:getetag />
                <C:calendar-data />
            </D:prop>
            <C:filter>
                <C:comp-filter name="VCALENDAR">
                    <C:comp-filter name="VEVENT" />
                </C:comp-filter>
            </C:filter>
        </C:calendar-query>'''
        
        headers = {
            'Content-Type': 'application/xml; charset=utf-8',
            'Depth': '1'
        }
        
        events = []
        
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request(
                    'REPORT', self.url, data=calendar_query_body, headers=headers
                ) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"Calendar query failed: {response.status}")
                        return events
                    
                    xml_content = await response.text()
                    events = self._parse_calendar_query_response(xml_content)
                
        except Exception as e:
            logger.error(f"Failed to fetch all events: {e}")
        
        return events
    
    def _parse_calendar_query_response(self, xml_content: str) -> List[EventModel]:
        """Parse calendar-query REPORT response."""
        events = []
        
        try:
            root = ET.fromstring(xml_content)
            
            namespaces = {
                'D': 'DAV:',
                'C': 'urn:ietf:params:xml:ns:caldav'
            }
            
            for response in root.findall('.//D:response', namespaces):
                calendar_data = response.find('.//C:calendar-data', namespaces)
                if calendar_data is not None and calendar_data.text:
                    try:
                        cal = Calendar.from_ical(calendar_data.text)
                        for component in cal.walk():
                            if component.name == "VEVENT":
                                event = EventModel.from_icalendar(component)
                                events.append(event)
                    except Exception as e:
                        logger.error(f"Failed to parse calendar data: {e}")
        
        except ET.ParseError as e:
            logger.error(f"Failed to parse calendar query response: {e}")
        
        return events
    
    async def _find_deleted_events(self, current_events: List[EventModel]) -> List[str]:
        """Find events that were deleted by comparing with stored events."""
        if not self.database:
            return []
        
        current_uids = {event.uid for event in current_events}
        stored_events = await self.database.get_events_by_source(self.name)
        stored_uids = {event['caldav_uid'] for event in stored_events}
        
        deleted_uids = list(stored_uids - current_uids)
        return deleted_uids
    
    def _extract_uid_from_href(self, href: str) -> Optional[str]:
        """Extract UID from resource href."""
        # This is a simple implementation - might need adjustment based on server
        parts = href.rstrip('/').split('/')
        if parts:
            filename = parts[-1]
            if filename.endswith('.ics'):
                return filename[:-4]  # Remove .ics extension
            return filename
        return None
    
    async def test_connection(self) -> bool:
        """Test connection to CalDAV server."""
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.options(self.url) as response:
                    return response.status < 400
        except Exception as e:
            logger.error(f"Connection test failed for {self.name}: {e}")
            return False