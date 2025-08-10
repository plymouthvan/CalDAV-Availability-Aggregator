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

    def __init__(self, name: str, url: str, username: str, password: str, database=None, **kwargs):
        super().__init__(name, url, username, password, database)
        self.sync_method = 'calendar-query'
        self._auth = aiohttp.BasicAuth(username, password)
        logger.debug(f"Initialized iCloudCalDAVClient for source: {name}")

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
                    all_events, deleted_uids, sync_token = await self._parse_calendar_query_response(xml_content)
                    
                    # We don't get a sync token from this query, so we'll use ctag as the sync state
                    new_ctag = await self._get_ctag()
                    new_sync_state = {"ctag": new_ctag} if new_ctag else None

                    # Filter out unchanged events
                    new_and_updated_events = []
                    if self.database:
                        stored_events = await self.database.get_all_events_for_source(self.name)
                        for event in all_events:
                            if event.uid not in stored_events or event.compute_hash() != stored_events[event.uid]['event_hash']:
                                new_and_updated_events.append(event)
                    else:
                        new_and_updated_events = all_events

                    return new_and_updated_events, deleted_uids, new_sync_state
        except Exception as e:
            logger.error(f"iCloud sync error: {e}")
            return [], [], None

    async def _parse_calendar_query_response(self, xml_content: str) -> Tuple[List[EventModel], List[str], Optional[str]]:
        raw_events = []
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
                                raw_events.append(component)
                    except Exception as e:
                        logger.error(f"Failed to parse calendar data: {e}")
        except ET.ParseError as e:
            logger.error(f"Failed to parse calendar query response: {e}")

        # iCloud-specific logic to synthesize EXDATEs
        master_events = {event.get('UID'): event for event in raw_events if event.get('RRULE')}
        exceptions = [event for event in raw_events if 'RECURRENCE-ID' in event]
        
        logger.debug(f"Found {len(master_events)} master events and {len(exceptions)} exceptions.")

        # Diagnostics: log master anchors and exception RECURRENCE-ID params (e.g., RANGE=THISANDFUTURE)
        try:
            # Masters
            for uid_key, master in master_events.items():
                try:
                    dtstart_prop = master.get('DTSTART')
                    dtstart_dt = getattr(dtstart_prop, 'dt', None) if dtstart_prop else None
                    dtstart_params = getattr(dtstart_prop, 'params', {}) if dtstart_prop else {}
                    rrule_prop = master.get('RRULE')
                    rrule_str = None
                    if rrule_prop:
                        try:
                            rrule_str = rrule_prop.to_ical().decode('utf-8')
                        except Exception:
                            rrule_str = str(rrule_prop)
                    logger.debug(
                        f"[iCloud PARSE][MASTER] UID={str(uid_key)}, DTSTART={dtstart_dt}, "
                        f"DTSTART-params={dict(dtstart_params) if hasattr(dtstart_params, 'items') else dtstart_params}, "
                        f"RRULE={rrule_str}"
                    )
                except Exception as e:
                    logger.warning(f"[iCloud PARSE][MASTER] Logging failed for UID={str(uid_key)}: {e}")

            # Exceptions
            for ex in exceptions:
                try:
                    uid_prop = ex.get('UID')
                    uid_val = str(uid_prop) if uid_prop is not None else None
                    rid_prop = ex.get('RECURRENCE-ID')
                    rid_dt = getattr(rid_prop, 'dt', None) if rid_prop else None
                    rid_params = getattr(rid_prop, 'params', {}) if rid_prop else {}
                    rng = rid_params.get('RANGE') if hasattr(rid_params, 'get') else None
                    status_val = str(ex.get('STATUS', '')).upper() if ex.get('STATUS') else None
                    ex_dtstart_prop = ex.get('DTSTART')
                    ex_dtstart_dt = getattr(ex_dtstart_prop, 'dt', None) if ex_dtstart_prop else None

                    logger.debug(
                        f"[iCloud PARSE][EXCEPTION] UID={uid_val}, RECURRENCE-ID={rid_dt}, "
                        f"RID-params={dict(rid_params) if hasattr(rid_params, 'items') else rid_params}, "
                        f"STATUS={status_val}, DTSTART={ex_dtstart_dt}"
                    )
                    if rng:
                        logger.info(f"[iCloud PARSE][RANGE] UID={uid_val}, RANGE={rng}, RID={rid_dt}")
                except Exception as e:
                    logger.warning(f"[iCloud PARSE][EXCEPTION] Logging failed: {e}")
        except Exception as e:
            logger.warning(f"[iCloud PARSE] Diagnostic logging failed: {e}")

        for event in exceptions:
            uid = event.get('UID')
            if uid in master_events:
                # An exception with a 'CANCELLED' status is how iCloud marks a deleted instance.
                # We must synthesize an EXDATE on the master event to reflect this.
                if str(event.get('STATUS', 'CONFIRMED')).upper() == 'CANCELLED':
                    master = master_events[uid]
                    recurrence_id = event.get('RECURRENCE-ID')
                    
                    if not recurrence_id:
                        continue

                    # Ensure exdate property exists
                    if 'EXDATE' not in master:
                        master.add('EXDATE', [])
                    
                    exdate_dt = recurrence_id.dt
                    
                    # Check if already excluded
                    is_excluded = False
                    exdates = master.get('exdate')
                    if exdates:
                        if not isinstance(exdates, list):
                            exdates = [exdates] # Handle single exdate case
                        for exdate_prop in exdates:
                            if exdate_dt in exdate_prop.dts:
                                is_excluded = True
                                break
                    
                    if not is_excluded:
                        master.add('EXDATE', exdate_dt)
                        logger.debug(f"Synthesized EXDATE for UID {uid} from CANCELLED exception with RECURRENCE-ID {exdate_dt}")

        # Filter out cancelled events, as their purpose (adding an EXDATE) is now fulfilled.
        # This prevents them from being treated as active events to be created in Google.
        final_components = [
            event for event in raw_events
            if str(event.get('STATUS', 'CONFIRMED')).upper() != 'CANCELLED'
        ]

        events = [EventModel.from_icalendar(component, self.name) for component in final_components]
        
        # We don't get deleted UIDs from this query, so we have to find them by comparing
        deleted_uids = await self._find_deleted_events(events)
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