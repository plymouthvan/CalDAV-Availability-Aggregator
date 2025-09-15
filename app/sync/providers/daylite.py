"""
Daylite CalDAV client for CalDAV Mirror
"""

import aiohttp
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
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
                    
                    # Self-heal (interval-guarded): augment deleted_uids by comparing DB vs live snapshot
                    try:
                        if self.database:
                            hours_str = os.getenv("DAYLITE_SELF_HEAL_INTERVAL_HOURS", "12")
                            try:
                                interval_hours = max(1, int(hours_str))
                            except Exception:
                                interval_hours = 12
                            guard_key = f"daylite_self_heal_last_run::{self.name}"
                            last_run_str = await self.database.get_app_state(guard_key)
                            now_utc = datetime.now(timezone.utc)
                            due = True
                            if last_run_str:
                                try:
                                    # Accept both Z and +00:00 forms
                                    last_dt = datetime.fromisoformat(last_run_str.replace("Z", "+00:00"))
                                    due = (now_utc - last_dt) >= timedelta(hours=interval_hours)
                                except Exception:
                                    due = True
                            if due:
                                fallback_deleted = await self._find_deleted_events_fallback()
                                if fallback_deleted:
                                    before = set(deleted_uids)
                                    merged = list(dict.fromkeys(deleted_uids + [u for u in fallback_deleted if u not in before]))
                                    if len(merged) != len(deleted_uids):
                                        logger.info(f"[{self.name}] Self-heal: adding {len(merged) - len(deleted_uids)} deleted UIDs via snapshot audit.")
                                    deleted_uids = merged
                                # Record last run time
                                try:
                                    await self.database.set_app_state(guard_key, now_utc.isoformat().replace("+00:00", "Z"))
                                except Exception:
                                    pass
                            else:
                                try:
                                    # Explicit observability when the self-heal audit is suppressed by the interval guard
                                    logger.info('{"type": "SELF_HEAL_SKIPPED", "source": "%s", "interval_hours": %d, "last_run": "%s"}', self.name, interval_hours, last_run_str)
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.debug(f"[{self.name}] Self-heal audit failed: {e}")
                    
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
                    calendar_data = response.find('.//{*}calendar-data', namespaces)
                    if calendar_data is not None and calendar_data.text:
                        try:
                            cal = Calendar.from_ical(calendar_data.text)
                            for component in cal.walk():
                                if component.name == "VEVENT":
                                    events.append(EventModel.from_icalendar(component, self.name))
                        except Exception as e:
                            logger.error(f"Failed to parse calendar data from {href.text}: {e}")
                    else:
                        logger.warning(f"No calendar data found for {href.text}")
        except ET.ParseError as e:
            logger.error(f"Failed to parse sync-collection response: {e}")

        return events, deleted_uids, sync_token
    async def _fetch_all_events_calendar_query(self) -> List[EventModel]:
        """
        Snapshot fetch of ALL current VEVENTs via CalDAV calendar-query.
        Used as a fallback to detect DB rows whose UIDs no longer exist on the server.
        """
        query_body = """<?xml version="1.0" encoding="utf-8" ?>
        <C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:prop>
                <D:getetag />
                <C:calendar-data />
            </D:prop>
            <C:filter>
                <C:comp-filter name="VCALENDAR" />
            </C:filter>
        </C:calendar-query>"""
        headers = {"Content-Type": "application/xml; charset=utf-8", "Depth": "1"}
        events: List[EventModel] = []
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request("REPORT", self.url, data=query_body, headers=headers) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"[{self.name}] calendar-query snapshot failed: {response.status}")
                        return events
                    xml_content = await response.text()
                    try:
                        root = ET.fromstring(xml_content)
                        # Use wildcard namespace for calendar-data to cope with server variants
                        for resp in root.findall(".//{DAV:}response"):
                            caldata = resp.find(".//{*}calendar-data")
                            if caldata is not None and caldata.text:
                                try:
                                    cal = Calendar.from_ical(caldata.text)
                                    for comp in cal.walk():
                                        if comp.name == "VEVENT":
                                            model = EventModel.from_icalendar(comp, self.name)
                                            if model and model.uid:
                                                events.append(model)
                                except Exception as e:
                                    logger.error(f"[{self.name}] Failed to parse calendar-data in snapshot: {e}")
                    except ET.ParseError as e:
                        logger.error(f"[{self.name}] Snapshot XML parse error: {e}")
        except Exception as e:
            logger.error(f"[{self.name}] Error performing calendar-query snapshot: {e}")
        return events

    async def _find_deleted_events_fallback(self) -> List[str]:
        """
        Self-heal detection: compute deleted UIDs by diffing DB-stored UIDs
        against a full current snapshot fetched via calendar-query.
        If calendar-query is not supported (e.g., 400), fall back to a sync-collection
        baseline snapshot of UIDs.
        """
        if not self.database:
            return []
        try:
            # First attempt: calendar-query full snapshot (rich VEVENTs)
            snapshot_events = await self._fetch_all_events_calendar_query()
            current_uids: Set[str]
            if snapshot_events:
                current_uids = {e.uid for e in snapshot_events if e and e.uid}
            else:
                # Fallback: baseline sync-collection to gather UIDs only
                try:
                    baseline_uids = await self._fetch_all_uids_sync_collection()
                    current_uids = set(baseline_uids)
                    if current_uids:
                        logger.info(f"[{self.name}] Fallback baseline snapshot used; collected {len(current_uids)} UID(s).")
                except Exception as ee:
                    logger.debug(f"[{self.name}] Baseline UID snapshot failed: {ee}")
                    current_uids = set()

            # Stored UIDs in DB
            stored_rows = await self.database.get_events_by_source(self.name)
            stored_uids: Set[str] = {row.get("caldav_uid") for row in stored_rows if row.get("caldav_uid")}

            # Deleted = present in DB but not in current snapshot
            if not current_uids:
                # If we cannot fetch any snapshot at all, do not assume deletions
                return []

            missing = list(stored_uids - current_uids)
            if missing:
                try:
                    logger.info(f"[{self.name}] Fallback deletion audit: {len(missing)} UID(s) missing on server will be treated as deleted.")
                except Exception:
                    pass
            return missing
        except Exception as e:
            logger.debug(f"[{self.name}] Fallback deletion detection failed: {e}")
            return []

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
    async def _fetch_all_uids_sync_collection(self) -> Set[str]:
        """
        Snapshot of ALL current VEVENT UIDs using sync-collection baseline (empty sync-token).
        Daylite supports sync-token; an empty token requests a full baseline.
        """
        sync_body = """<?xml version="1.0" encoding="utf-8" ?>
        <D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
            <D:sync-token></D:sync-token>
            <D:sync-level>1</D:sync-level>
            <D:prop>
                <D:getetag />
                <C:calendar-data />
            </D:prop>
        </D:sync-collection>"""
        headers = {"Content-Type": "application/xml; charset=utf-8", "Depth": "1"}
        uids: Set[str] = set()
        try:
            async with aiohttp.ClientSession(auth=self._auth) as session:
                async with session.request("REPORT", self.url, data=sync_body, headers=headers) as response:
                    if response.status not in [200, 207]:
                        logger.error(f"[{self.name}] sync-collection baseline failed: {response.status}")
                        return uids
                    xml_content = await response.text()
                    try:
                        root = ET.fromstring(xml_content)
                        for resp in root.findall(".//{DAV:}response"):
                            caldata = resp.find(".//{*}calendar-data")
                            if caldata is not None and caldata.text:
                                try:
                                    cal = Calendar.from_ical(caldata.text)
                                    for comp in cal.walk():
                                        if comp.name == "VEVENT":
                                            try:
                                                model = EventModel.from_icalendar(comp, self.name)
                                                if model and model.uid:
                                                    uids.add(model.uid)
                                            except Exception:
                                                # Fallback: read raw UID from component if model parse fails
                                                raw_uid = str(comp.get("UID") or "")
                                                if raw_uid:
                                                    uids.add(raw_uid)
                                except Exception as e:
                                    logger.error(f"[{self.name}] Failed to parse calendar-data in baseline snapshot: {e}")
                    except ET.ParseError as e:
                        logger.error(f"[{self.name}] Baseline XML parse error: {e}")
        except Exception as e:
            logger.error(f"[{self.name}] Error performing sync-collection baseline snapshot: {e}")
        return uids