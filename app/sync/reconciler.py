"""
Reconciliation module for CalDAV Mirror

Compares the state of the local database with the state of Google Calendar
and generates the necessary operations to make Google Calendar a mirror of
the database.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional, Set
from datetime import datetime, timedelta, timezone
from dateutil import rrule
import pytz

import json
from .google_client import GoogleClient
from database import Database
from .event_model import EventModel

logger = logging.getLogger(__name__)

class Reconciler:
    """
    Orchestrates the reconciliation between the local database and Google
    Calendar for a specific source, with full support for recurring events.
    """

    def __init__(self, google_client: GoogleClient, database: Database):
        self.google = google_client
        self.db = database

    # Diagnostic helper: verify that a recurrence_id maps to a generated instance
    # of the master event's RRULE (with its current anchor and timezone).
    def _rid_valid_against_master(self, master: EventModel, rid: Optional[str]) -> Tuple[bool, str]:
        try:
            tzname = master.timezone or 'UTC'
            try:
                event_tz = pytz.timezone(tzname)
            except Exception:
                event_tz = pytz.UTC

            # Resolve master dtstart
            if master.start_datetime:
                dtstart = master.start_datetime
                if dtstart.tzinfo:
                    dtstart = dtstart.astimezone(event_tz)
                else:
                    dtstart = event_tz.localize(dtstart)
            elif master.start_date:
                try:
                    dtstart = event_tz.localize(datetime.strptime(master.start_date, '%Y-%m-%d'))
                except Exception:
                    return False, f"Invalid master start_date={master.start_date}"
            else:
                return False, "Master has no start time"

            if not master.rrule:
                return False, "Master has no RRULE"

            # Parse RID to localized datetime
            if not rid:
                return False, "No recurrence_id"
            try:
                if len(rid) == 8 and rid.isdigit():
                    rid_local = event_tz.localize(datetime.strptime(rid, '%Y%m%d'))
                else:
                    if rid.endswith('Z'):
                        rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                    else:
                        rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                    rid_local = rid_dt_utc.astimezone(event_tz)
            except Exception as e:
                return False, f"Failed to parse RID '{rid}': {e}"

            # Build rule and test membership (inclusive)
            try:
                rule = rrule.rrulestr(master.rrule, dtstart=dtstart)
            except Exception as e:
                return False, f"Failed to parse RRULE '{master.rrule}': {e}"

            candidate = rule.after(rid_local - timedelta(seconds=1), inc=True)
            is_member = candidate == rid_local
            detail = (
                f"tz={getattr(event_tz, 'zone', str(event_tz))}, "
                f"dtstart={dtstart.isoformat()}, "
                f"rrule={master.rrule}, "
                f"rid_local={rid_local.isoformat()}, "
                f"candidate={candidate.isoformat() if candidate else None}"
            )
            return is_member, detail
        except Exception as e:
            return False, f"RID validation error: {e}"

    # --- Split detection helpers for "This and following events" (R1 → R2) ---
    def _rid_to_local_dt(self, master: EventModel, rid: Optional[str]) -> Optional[datetime]:
        """
        Parse a RECURRENCE-ID string (YYYYMMDD or YYYYMMDDTHHMMSS[Z]) into a timezone-aware
        datetime localized to the master's timezone.
        """
        if not rid:
            return None
        tzname = master.timezone or 'UTC'
        try:
            event_tz = pytz.timezone(tzname)
        except Exception:
            event_tz = pytz.UTC
        try:
            if len(rid) == 8 and rid.isdigit():
                # Date-only
                return event_tz.localize(datetime.strptime(rid, '%Y%m%d'))
            else:
                # Date-time, assume UTC if Z or naive UTC form
                if rid.endswith('Z'):
                    rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                else:
                    rid_dt_utc = datetime.strptime(rid, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                return rid_dt_utc.astimezone(event_tz)
        except Exception:
            return None

    def _extract_rrule_from_recurrence_list(self, recurrence: Optional[List[str]]) -> Optional[str]:
        """Extract the RRULE value (without the leading 'RRULE:') from a Google recurrence array."""
        if not recurrence:
            return None
        for rule in recurrence:
            if isinstance(rule, str) and rule.startswith('RRULE:'):
                return rule[6:]
        return None

    def _canon_rrule_str(self, rrule_str: Optional[str]) -> Optional[str]:
        """
        Canonicalize an RRULE string the same way our hashing does, to avoid churn
        from WKST or INTERVAL=1 differences.
        """
        if not rrule_str:
            return None
        try:
            tmp = EventModel(uid="__canon__", summary="", rrule=rrule_str)
            return tmp.normalized_for_hash().get('rrule')
        except Exception:
            return rrule_str

    def _extract_until_from_rrule(self, rrule_str: Optional[str]) -> Optional[str]:
        """
        Extract the UNTIL component from an RRULE string, if present, returning the raw token value.
        """
        if not rrule_str:
            return None
        try:
            parts = [p for p in rrule_str.split(';') if p]
            for p in parts:
                if p.upper().startswith('UNTIL='):
                    return p.split('=', 1)[1].strip()
        except Exception:
            pass
        return None

    def _build_event_highlights(self, ev: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not ev:
            return None
        try:
            private = ((ev.get('extendedProperties') or {}).get('private')) or {}
            rrule_val = self._extract_rrule_from_recurrence_list(ev.get('recurrence'))
            return {
                "id": ev.get("id"),
                "iCalUID": ev.get("iCalUID"),
                "recurringEventId": ev.get("recurringEventId"),
                "sequence": ev.get("sequence"),
                "status": ev.get("status"),
                "extendedProperties.private": private,
                "rrule": rrule_val,
                "rrule_until": self._extract_until_from_rrule(rrule_val)
            }
        except Exception:
            return {"error": "failed_to_build_highlights"}

    def _log_event_comparison(self, source_name: str, ical_uid: Optional[str], original_event: Optional[Dict[str, Any]], new_event: Dict[str, Any]) -> None:
        """
        Emit a structured, side-by-side comparison between an existing mirrored master and a newly
        discovered event that shares the same iCalUID but is missing our ownership property.
        """
        try:
            obj = {
                "type": "FULL_EVENT_COMPARISON",
                "source": source_name,
                "iCalUID": ical_uid,
                "original_highlights": self._build_event_highlights(original_event),
                "new_highlights": self._build_event_highlights(new_event),
                "original_raw": original_event,
                "new_raw": new_event
            }
            logger.info(json.dumps(obj, default=str))
        except Exception:
            logger.debug(f"[{source_name}] Failed to emit FULL_EVENT_COMPARISON for iCalUID={ical_uid}")

    async def _detect_split_successor(
        self,
        source_name: str,
        uid: str,
        db_series_keys: set,
        google_series_keys: set,
        desired_instances: Dict[Tuple[str, Optional[str]], EventModel],
        google_active_instances: Dict[Tuple[str, Optional[str]], EventModel],
        until_hint: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Detect if a recurring series was split in Google UI ("This and following events").

        Modes:
        - BY_RID: Find earliest missing recurrence from DB's perspective and probe a narrow window.
        - BY_UNTIL: If no missing RIDs exist but Google's master shows an UNTIL truncation, compute the first
                    occurrence strictly after UNTIL using the DB rule and probe there.

        Returns:
            dict with { 'r2_master_id', 'r2_ical_uid', 'split_at' } if detected, else None
        """
        # Precondition: require both DB and Google active master to be present
        db_master = desired_instances.get((uid, None))
        google_master = google_active_instances.get((uid, None))
        if not db_master or not google_master:
            return None
        try:
            logger.info(f"[TRACE][SPLIT][{source_name}] UID={uid} db_master_gid={getattr(db_master, 'google_event_id', None)} google_master_gid={getattr(google_master, 'google_event_id', None)}")
        except Exception:
            logger.debug(f"[TRACE][SPLIT][{source_name}] UID={uid} master gid logging failed")

        detection_mode = None
        t0_local: Optional[datetime] = None
        date_only = False

        # Attempt BY_RID first (structural mismatch implies at least one missing instance)
        missing = [(u, rid) for (u, rid) in db_series_keys if u == uid and rid is not None and (u, rid) not in google_series_keys]
        if missing:
            def rid_sort_key(pair: Tuple[str, str]):
                _, rid = pair
                dt_local = self._rid_to_local_dt(db_master, rid)
                if dt_local:
                    return dt_local.astimezone(timezone.utc)
                return rid  # fallback

            missing_sorted = sorted(missing, key=rid_sort_key)
            rid0 = missing_sorted[0][1]
            logger.info(f"[TRACE][SPLIT][{source_name}] UID={uid} DETECTION_MODE=BY_RID earliest_missing_rid={rid0} missing_count={len(missing_sorted)}")

            # Parse rid0 → local
            t0_local = self._rid_to_local_dt(db_master, rid0)
            if not t0_local:
                return None
            date_only = len(rid0) == 8 and rid0.isdigit()
            detection_mode = "BY_RID"

        # If no missing RID but we have an UNTIL hint indicating a truncation, compute next occurrence after UNTIL
        elif until_hint:
            try:
                # Resolve event timezone and master dtstart
                tzname = db_master.timezone or 'UTC'
                try:
                    event_tz = pytz.timezone(tzname)
                except Exception:
                    event_tz = pytz.UTC

                if db_master.start_datetime:
                    dtstart = db_master.start_datetime
                    if dtstart.tzinfo:
                        dtstart = dtstart.astimezone(event_tz)
                    else:
                        dtstart = event_tz.localize(dtstart)
                elif db_master.start_date:
                    dtstart = event_tz.localize(datetime.strptime(db_master.start_date, '%Y-%m-%d'))
                else:
                    return None

                # Build DB rule
                rule = rrule.rrulestr(db_master.rrule, dtstart=dtstart) if db_master.rrule else None
                if not rule:
                    return None

                # Parse UNTIL hint and convert to event tz
                if len(until_hint) == 8 and until_hint.isdigit():
                    until_local = event_tz.localize(datetime.strptime(until_hint, '%Y%m%d'))
                else:
                    if until_hint.endswith('Z'):
                        until_dt_utc = datetime.strptime(until_hint, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
                    else:
                        until_dt_utc = datetime.strptime(until_hint, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                    until_local = until_dt_utc.astimezone(event_tz)

                # First occurrence strictly after UNTIL boundary
                t0_local = rule.after(until_local, inc=False)
                if not t0_local:
                    return None

                date_only = bool(db_master.start_date and not db_master.start_datetime)
                detection_mode = "BY_UNTIL"
                logger.info(f"[TRACE][SPLIT][{source_name}] UID={uid} DETECTION_MODE=BY_UNTIL until_hint={until_hint} t0_local={t0_local.isoformat()} event_tz={getattr(event_tz,'zone',str(event_tz))}")
            except Exception as e:
                logger.debug(f"[{source_name}] BY_UNTIL computation failed for UID={uid}: {e}")
                return None
        else:
            # No evidence to perform a split probe
            return None

        # Convert probe time to UTC and build the search window
        t0_utc = t0_local.astimezone(timezone.utc)
        if date_only:
            # Use the whole UTC day for date-only instances
            day_start = datetime(t0_utc.year, t0_utc.month, t0_utc.day, 0, 0, 0, tzinfo=timezone.utc)
            day_end = datetime(t0_utc.year, t0_utc.month, t0_utc.day, 23, 59, 59, tzinfo=timezone.utc)
            time_min = day_start.isoformat().replace('+00:00', 'Z')
            time_max = day_end.isoformat().replace('+00:00', 'Z')
        else:
            time_min = (t0_utc - timedelta(minutes=5)).isoformat().replace('+00:00', 'Z')
            time_max = (t0_utc + timedelta(minutes=5)).isoformat().replace('+00:00', 'Z')

        logger.info(f"[TRACE][SPLIT][{source_name}] UID={uid} DETECTION_MODE={detection_mode} date_only={date_only} t0_utc={t0_utc.isoformat()} window=[{time_min}, {time_max}]")

        # Require window/list + get-by-id helpers
        if not hasattr(self.google, 'list_events_window') or not hasattr(self.google, 'get_event_by_id'):
            logger.debug(f"[{source_name}] Split detect skipped for UID={uid}: Google client helper(s) missing.")
            return None

        try:
            items = await self.google.list_events_window(time_min, time_max, single_events=True)
        except Exception as e:
            logger.warning(f"[{source_name}] Window probe failed for UID={uid}: {e}")
            return None

        def _time_matches_start(item: Dict[str, Any]) -> bool:
            start = (item.get('start') or {})
            dt_s = start.get('dateTime')
            d_s = start.get('date')
            if dt_s:
                try:
                    inst = datetime.fromisoformat(dt_s.replace('Z', '+00:00')).astimezone(timezone.utc)
                    return abs((inst - t0_utc).total_seconds()) <= 60
                except Exception:
                    return False
            if d_s:
                try:
                    return d_s == t0_utc.date().isoformat()
                except Exception:
                    return False
            return False

        candidates = []
        for it in items:
            status = (it.get('status') or '').lower()
            if status == 'cancelled':
                continue
            if not it.get('recurringEventId'):
                continue
            private = ((it.get('extendedProperties') or {}).get('private')) or {}
            # Skip our own mirrored artifacts; we want user-created/untracked instances
            if private.get('caldav-mirror-source') == source_name:
                continue
            if _time_matches_start(it):
                candidates.append(it)

        if not candidates:
            return None

        # Canonicalize DB rule once
        db_rr_can = self._canon_rrule_str(db_master.rrule)

        for inst in candidates:
            try:
                master_id = inst.get('recurringEventId')
                if not master_id:
                    continue
                master = await self.google.get_event_by_id(master_id)
                if not master:
                    continue
                m_rr = self._extract_rrule_from_recurrence_list(master.get('recurrence'))
                m_rr_can = self._canon_rrule_str(m_rr)
                logger.info(f"[TRACE][RRULE_COMPARE][{source_name}] UID={uid} db_rr_can={db_rr_can} m_rr_can={m_rr_can} equal={bool(m_rr_can and db_rr_can and m_rr_can == db_rr_can)} master_id={master_id}")
                if m_rr_can and db_rr_can and m_rr_can == db_rr_can:
                    logger.info(f"[DETECT][SPLIT][{source_name}] UID={uid} → Found successor R2 master={master_id} at {t0_utc.isoformat()}")
                    return {
                        'r2_master_id': master.get('id'),
                        'r2_ical_uid': master.get('iCalUID'),
                        'split_at': t0_utc.isoformat()
                    }
            except Exception as e:
                logger.debug(f"[{source_name}] Candidate evaluation failed for UID={uid}: {e}")

        return None

    async def _purge_all_google_artifacts_for_uid(self, source_name: str, uid: str):
        """No-op: sweep disabled in disown-before-delete model."""
        logger.debug(f"[DELETE][RECURRING][{source_name}] sweep disabled for UID={uid}")

    async def reconcile_source(self, source_name: str):
        """
        Performs a full, recurrence-aware reconciliation for a given source.
        """
        logger.info(f"Starting reconciliation for source: {source_name}")

        db_events_raw = await self.db.get_all_events_for_source(source_name)
        logger.debug(f"[{source_name}] Fetched {len(db_events_raw)} raw events from DB.")
        
        google_events_raw, _ = await self.google.list_all_mirrored_events(source_name)
        logger.debug(f"[{source_name}] Fetched {len(google_events_raw)} active events from Google.")

        desired_instances = {
            key: EventModel.from_dict(val['event_data'])
            for key, val in db_events_raw.items()
        }
        for key, event in desired_instances.items():
            logger.debug(f"[KEY GEN] Source Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        # Active events only (non-cancelled) for comparison
        google_active_instances = {
            key: EventModel.from_google_event(val)
            for key, val in google_events_raw.items()
        }
        
        # Tombstones ignored in disown-before-delete model
        google_tombstones = {}
        for key, event in google_active_instances.items():
            logger.debug(f"[KEY GEN] Google Event Key: UID={event.uid}, RecurrenceID={event.recurrence_id}")

        # Detect unauthorized deletions of recurring instances on Google (CANCELLED items)
        # IMPORTANT: Only consider CANCELLED items that reference the CURRENT active master.
        # Stale tombstones from a previous master (different GID) must be ignored to prevent repeat replacements.
        cancelled_map = await self.google.list_cancelled_by_source(source_name)
        # Build active master GID map for quick lookup
        active_master_by_uid: Dict[str, EventModel] = {
            uid: model for (uid, rid), model in google_active_instances.items()
            if rid is None and model and model.google_event_id
        }
        uids_with_cancelled_instances = set()
        for (uid, rid), raw_item in cancelled_map.items():
            active_master = active_master_by_uid.get(uid)
            if not active_master:
                continue
            current_master_gid = active_master.google_event_id
            # CANCELLED master: only count if the cancelled item's ID equals the CURRENT master GID (shouldn't in steady state)
            if rid is None:
                if raw_item.get('id') == current_master_gid:
                    uids_with_cancelled_instances.add(uid)
            else:
                # CANCELLED instance: only count if the tombstone references the CURRENT master via recurringEventId
                if raw_item.get('recurringEventId') == current_master_gid:
                    uids_with_cancelled_instances.add(uid)
        if uids_with_cancelled_instances:
            logger.info(f"[DETECT][CANCELLED_INSTANCES][{source_name}] UIDs with deleted instances on Google (current-master scoped): {sorted(list(uids_with_cancelled_instances))}")

        # --- RID Validation Diagnostics ---
        try:
            masters_by_uid = {uid: model for (uid, rid), model in desired_instances.items() if rid is None}
            for (uid, rid), model in desired_instances.items():
                if rid is None:
                    continue
                master = masters_by_uid.get(uid)
                if not master:
                    logger.debug(f"[RID VALIDATION][SKIP] UID={uid}, RID={rid} → No master in desired set.")
                    continue
                ok, detail = self._rid_valid_against_master(master, rid)
                if ok:
                    logger.debug(f"[RID VALIDATION] UID={uid}, RID={rid} is valid. {detail}")
                else:
                    logger.warning(f"[RID VALIDATION][MISMATCH] UID={uid}, RID={rid} not generated by master. {detail}")
        except Exception as e:
            logger.warning(f"[RID VALIDATION] Unexpected error while validating: {e}")

        db_keys = set(desired_instances.keys())
        # Only consider active Google items for structural keys
        google_keys = set(google_active_instances.keys())

        logger.debug(f"[{source_name}] DB keys ({len(db_keys)}): {db_keys}")
        logger.debug(f"[{source_name}] Google keys ({len(google_keys)}): {google_keys}")

        # --- Series-level Reconciliation ---
        series_to_replace = set()
        
        # Deferred per-instance actions for recurring series (avoid whole-series replacement churn)
        deferred_creates: List[EventModel] = []
        deferred_updates: List[Tuple[str, EventModel]] = []
        deferred_deletes: List[str] = []
        all_uids = {key[0] for key in db_keys | google_keys}

        for uid in all_uids:
            db_series_keys = {k for k in db_keys if k[0] == uid}
            google_series_keys = {k for k in google_keys if k[0] == uid}
            try:
                logger.info(f"[TRACE][SERIES_START][{source_name}] UID={uid} db_series_keys={sorted(list(db_series_keys))} google_series_keys={sorted(list(google_series_keys))}")
            except Exception:
                logger.debug(f"[TRACE][SERIES_START][{source_name}] UID={uid} series keys logging failed")

            # --- iCalUID-level reconciliation diagnostics ---
            original_master_raw = google_events_raw.get((uid, None))
            ical_uid = (original_master_raw or {}).get('iCalUID')

            items_by_ical: List[Dict[str, Any]] = []
            if ical_uid and hasattr(self.google, 'list_events_by_icaluid'):
                try:
                    items_by_ical = await self.google.list_events_by_icaluid(ical_uid, include_cancelled=True)
                except Exception as e:
                    logger.warning(f"[{source_name}] iCalUID probe failed for UID={uid}, iCalUID={ical_uid}: {e}")

            try:
                suspect_masters: List[Dict[str, Any]] = []
                suspect_instances: List[Dict[str, Any]] = []
                total_masters = 0

                original_master_id = (original_master_raw or {}).get("id")
                if not original_master_id:
                    gm = google_active_instances.get((uid, None))
                    if gm and gm.google_event_id:
                        original_master_id = gm.google_event_id

                for it in items_by_ical:
                    status = (it.get('status') or '').lower()
                    private = ((it.get('extendedProperties') or {}).get('private')) or {}
                    is_master = bool(it.get('recurrence')) and not it.get('recurringEventId')

                    if is_master:
                        total_masters += 1
                        if private.get('caldav-mirror-source') != source_name:
                            suspect_masters.append(it)
                    else:
                        reid = it.get('recurringEventId')
                        # Flag instances that point to a different master OR lack our ownership property
                        if (reid and original_master_id and reid != original_master_id) or (private.get('caldav-mirror-source') != source_name):
                            if status != 'cancelled':
                                suspect_instances.append(it)

                logger.info(json.dumps({
                    "type": "RECONCILIATION_START",
                    "source": source_name,
                    "uid": uid,
                    "iCalUID": ical_uid,
                    "found_count": len(items_by_ical),
                    "master_count": total_masters,
                    "suspect_master_count": len(suspect_masters),
                    "suspect_instance_count": len(suspect_instances)
                }))

                for sm in suspect_masters:
                    self._log_event_comparison(source_name, ical_uid, original_master_raw, sm)
                    logger.info(json.dumps({
                        "type": "SPLIT_EVENT_DETECTED",
                        "source": source_name,
                        "iCalUID": ical_uid,
                        "new_id": sm.get("id"),
                        "original_id": (original_master_raw or {}).get("id"),
                        "reason": "Same iCalUID but missing ownership property (master)"
                    }))

                for si in suspect_instances:
                    self._log_event_comparison(source_name, ical_uid, original_master_raw, si)
                    reason = "Different recurringEventId vs active master" if (original_master_id and si.get('recurringEventId') and si.get('recurringEventId') != original_master_id) else "Missing ownership property"
                    logger.info(json.dumps({
                        "type": "SPLIT_EVENT_DETECTED",
                        "source": source_name,
                        "iCalUID": ical_uid,
                        "new_id": si.get("id"),
                        "original_id": (original_master_raw or {}).get("id"),
                        "reason": reason
                    }))

                for sm in suspect_masters:
                    self._log_event_comparison(source_name, ical_uid, original_master_raw, sm)
                    logger.info(json.dumps({
                        "type": "SPLIT_EVENT_DETECTED",
                        "source": source_name,
                        "iCalUID": ical_uid,
                        "new_id": sm.get("id"),
                        "original_id": (original_master_raw or {}).get("id"),
                        "reason": "Same iCalUID but missing ownership property"
                    }))
            except Exception:
                logger.debug(f"[{source_name}] iCalUID diagnostics failed for UID={uid}")
 
            # A series is recurring if it has exceptions or if its master event has an RRULE.
            is_recurring = any(k[1] is not None for k in db_series_keys | google_series_keys)
            if not is_recurring:
                master_model = desired_instances.get((uid, None)) or google_active_instances.get((uid, None))
                if master_model and master_model.rrule:
                    is_recurring = True

            # DIAG: master presence and key counts for this UID
            try:
                master_active_present = (uid, None) in google_active_instances
                logger.debug(f"[SERIES DIAG][{source_name}] UID={uid} master_active={master_active_present}, db_keys_count={len(db_series_keys)}, google_active_keys_count={len(google_series_keys)}")
            except Exception:
                logger.debug(f"[SERIES DIAG][{source_name}] UID={uid} diagnostics failed.")

            # If any CANCELLED instances were detected for this UID, replace the series.
            if uid in uids_with_cancelled_instances and db_series_keys:
                logger.info(f"[{source_name}] CANCELLED instances detected on Google for UID={uid}. Replacing series.")
                logger.warning(f"[TRACE][QUEUE_REPLACE][{source_name}] UID={uid} action=series_replacement reason=cancelled_instances")
                logger.info(json.dumps({"type": "DECISION", "action": "REPLACE_SERIES", "reason": "cancelled_instances", "source": source_name, "uid": uid}))
                series_to_replace.add(uid)
                continue

            # Tombstones are ignored in disown-before-delete model; do not trigger series replacement based on their presence.

            if not is_recurring:
                continue # Skip to next UID, non-recurring events are handled later

            # Check for splits on both structural and content mismatches of the master
            # Ensure truncation hints are always defined, even for structural-only mismatches
            until_g = None
            until_db = None
            is_mismatch = False
            if db_series_keys != google_series_keys:
                logger.info(f"[{source_name}] Series {uid} has a structural mismatch. DB keys: {db_series_keys}, Google keys: {google_series_keys}.")
                is_mismatch = True
            else:
                # Check for content mismatch on the master, which is often the first sign of a split (truncated RRULE)
                master_key = (uid, None)
                if master_key in db_series_keys:
                    db_master = desired_instances[master_key]
                    google_master = google_active_instances.get(master_key)
                    if google_master:
                        # Canonical RRULEs and hashes for master comparison (split signal)
                        try:
                            db_rr_can = self._canon_rrule_str(db_master.rrule)
                            gm_rr_can = self._canon_rrule_str(google_master.rrule)
                        except Exception:
                            db_rr_can = db_master.rrule
                            gm_rr_can = google_master.rrule if google_master else None
                        db_hash = db_master.compute_hash()
                        gm_hash = google_master.compute_hash()
                        logger.info(f"[TRACE][MASTER_COMPARE][{source_name}] UID={uid} db_rr_can={db_rr_can} gm_rr_can={gm_rr_can} db_hash={db_hash} gm_hash={gm_hash} equal={db_hash == gm_hash}")
                        if db_hash != gm_hash:
                            logger.info(f"[{source_name}] Series {uid} has a content mismatch in master instance. Checking for split.")
                            # Initialize truncation hints so they are visible outside the try/except
                            until_g = None
                            until_db = None
                            try:
                                until_g = self._extract_until_from_rrule(google_master.rrule)
                                until_db = self._extract_until_from_rrule(db_master.rrule)
                                if until_g and (not until_db or until_g != until_db):
                                    logger.info(json.dumps({
                                        "type": "STATE_MISMATCH",
                                        "reason": "GOOGLE_TRUNCATION_DETECTED",
                                        "source": source_name,
                                        "uid": uid,
                                        "google_event_id": google_master.google_event_id,
                                        "iCalUID": ical_uid if 'ical_uid' in locals() else None,
                                        "google_rrule": google_master.rrule,
                                        "db_rrule": db_master.rrule,
                                        "google_until": until_g,
                                        "db_until": until_db
                                    }))
                            except Exception:
                                logger.debug(f"[{source_name}] Truncation diagnostics failed for UID={uid}")
                            is_mismatch = True

            if is_mismatch:
                split_info = await self._detect_split_successor(
                    source_name=source_name,
                    uid=uid,
                    db_series_keys=db_series_keys,
                    google_series_keys=google_series_keys,
                    desired_instances=desired_instances,
                    google_active_instances=google_active_instances,
                    until_hint=until_g
                )

                if split_info:
                    # This is an invalid modification. Per philosophy, we must revert it.
                    # 1. Immediately delete the new successor series (R2)
                    r2_id = split_info.get('r2_master_id')
                    if r2_id:
                        logger.info(json.dumps({"type": "SPLIT_EVENT_DETECTED", "source": source_name, "uid": uid, "iCalUID": ical_uid if 'ical_uid' in locals() else None, "new_id": r2_id, "original_id": (google_active_instances.get((uid, None)).google_event_id if (uid, None) in google_active_instances else None)}))
                        logger.info(f"[{source_name}] Split detected for UID={uid}. Deleting invalid successor series R2={r2_id}.")
                        await self.google.delete_event(r2_id)

                    # 2. Add the original series (R1) to the replacement plan. This will
                    #    trigger the standard disown->delete->recreate flow, which will
                    #    clean up the truncated R1 and recreate the series from DB truth.
                    logger.info(f"[{source_name}] Adding original series UID={uid} to replacement plan to revert split.")
                    logger.info(json.dumps({"type": "DECISION", "action": "REPLACE_SERIES", "reason": "split_detected", "source": source_name, "uid": uid}))
                    series_to_replace.add(uid)
                    continue
                else:
                    # If it's a mismatch but not a detectable split, replace the series.
                    logger.info(f"[{source_name}] Mismatch for UID={uid} is not a split. Replacing series.")
                    logger.warning(f"[TRACE][QUEUE_REPLACE][{source_name}] UID={uid} action=series_replacement reason=mismatch_not_split")
                    logger.info(json.dumps({"type": "DECISION", "action": "REPLACE_SERIES", "reason": "mismatch_not_split", "source": source_name, "uid": uid}))
                    series_to_replace.add(uid)
                    continue

            # If the structure is the same, check for content changes on exceptions
            for key in db_series_keys:
                if key[1] is None: continue # Master already checked
                db_model = desired_instances[key]
                google_model = google_active_instances.get(key)

                if not google_model:
                    # This case should be caught by the structural mismatch check, but as a safeguard:
                    logger.warning(f"[{source_name}] Mismatch: DB key {key} not found in Google results for series {uid}. Replacing.")
                    series_to_replace.add(uid)
                    break

                # Ensure desired exceptions carry the master recurringEventId BEFORE hashing
                if db_model.recurrence_id and not db_model.google_recurring_event_id:
                    master_key = (db_model.uid, None)
                    master_gcal_event = google_active_instances.get(master_key)
                    if master_gcal_event and master_gcal_event.google_event_id:
                        db_model.google_recurring_event_id = master_gcal_event.google_event_id

                if db_model.compute_hash() != google_model.compute_hash():
                    logger.info(f"[{source_name}] Series {uid} has a content mismatch in instance {key}. Replacing.")
                    series_to_replace.add(uid)
                    break
        
        # --- Build Plan ---
        to_create: List[EventModel] = []
        to_update: List[Tuple[str, EventModel]] = []
        to_delete: List[str] = []

        # Merge deferred per-instance actions accumulated during series checks
        if deferred_creates:
            to_create.extend(deferred_creates)
        if deferred_updates:
            to_update.extend(deferred_updates)
        if deferred_deletes:
            to_delete.extend(deferred_deletes)

        # Track recurring series to delete individually (not batch)
        recurring_series_to_delete: Dict[str, Dict[str, List[str]]] = {}
        
        # Plan replacement for entire series
        for uid in series_to_replace:
            # Collect all Google event IDs for this UID (masters only; tombstones ignored)
            masters_to_delete: List[str] = []
            
            # Active masters
            for key, event in google_active_instances.items():
                if key[0] == uid and key[1] is None and event.google_event_id:
                    masters_to_delete.append(event.google_event_id)
                    logger.info(f"[DELETE][SERIES][{source_name}] Will delete master for UID={uid}, GID={event.google_event_id}")
            
            # Store for individual deletion (not batch)
            if masters_to_delete:
                recurring_series_to_delete[uid] = {
                    'masters': masters_to_delete
                }
                logger.info(f"[DELETE][SERIES][{source_name}] UID={uid} summary: masters={len(masters_to_delete)}")

            # Add all desired DB events for this UID to the create list (master + exceptions)
            db_events_in_series = [d for k, d in desired_instances.items() if k[0] == uid]
            
            # Log exception creation plan
            db_exceptions = [e for e in db_events_in_series if e.recurrence_id]
            if db_exceptions:
                exc_rids = [e.recurrence_id for e in db_exceptions]
                logger.info(f"[PLAN][EXC_CREATE][{source_name}] UID={uid} will create {len(db_exceptions)} exceptions: RIDs={exc_rids}")
            
            to_create.extend(db_events_in_series)

        # Handle non-recurring events that are not part of a series being replaced
        non_recurring_db_keys = {k for k in db_keys if k[0] not in series_to_replace}
        non_recurring_google_keys = {k for k in google_keys if k[0] not in series_to_replace}

        nr_to_create_keys = non_recurring_db_keys - non_recurring_google_keys
        nr_to_delete_keys = non_recurring_google_keys - non_recurring_db_keys
        nr_to_compare_keys = non_recurring_db_keys.intersection(non_recurring_google_keys)

        for key in nr_to_create_keys:
            to_create.append(desired_instances[key])
        
        for key in nr_to_delete_keys:
            if google_active_instances[key].google_event_id:
                to_delete.append(google_active_instances[key].google_event_id)

        for key in nr_to_compare_keys:
            db_model = desired_instances[key]
            google_model = google_active_instances[key]
            if db_model.compute_hash() != google_model.compute_hash():
                logger.info(
                    f"[{source_name}] Change detected for non-recurring event. "
                    f"UID: {db_model.uid}. Google Event ID: {google_model.google_event_id}."
                )
                db_model.google_event_id = google_model.google_event_id
                to_update.append((db_model.google_event_id, db_model))

        # NOTE: Do not convert creates to updates during series replacement.
        # We intentionally delete the old series and re-create from DB to avoid churn and ensure mirror correctness.

        # --- Ordered execution to avoid invalid exception creations ---
        # Treat all non-exception events (masters and non-recurring) as "primary"
        primary_to_create: List[EventModel] = [e for e in to_create if not e.recurrence_id]
        exceptions_to_create: List[EventModel] = [e for e in to_create if e.recurrence_id]

        primary_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if not e.recurrence_id]
        exceptions_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if e.recurrence_id]

        # De-duplicate deletes to avoid "cannotOperateOnSameResourceMultipleTimesInBatch".
        to_delete_dedup = list(dict.fromkeys(to_delete))

        logger.info(
            f"Reconciliation plan: {len(to_create)} create, {len(to_update)} update, {len(to_delete_dedup)} delete. "
            f"(primary_create={len(primary_to_create)}, exceptions_create={len(exceptions_to_create)}, "
            f"primary_update={len(primary_to_update)}, exceptions_update={len(exceptions_to_update)})"
        )
        if logger.isEnabledFor(logging.DEBUG):
            try:
                plan_creates = [(e.uid, e.recurrence_id, bool(e.is_master_event)) for e in to_create]
                plan_updates = [(gid, e.uid, e.recurrence_id) for (gid, e) in to_update]
                plan_deletes = list(to_delete_dedup)
                logger.debug(f"[PLAN][CREATE] {plan_creates}")
                logger.debug(f"[PLAN][UPDATE] {plan_updates}")
                logger.debug(f"[PLAN][DELETE] {plan_deletes}")
                # Extra diagnostics: for any master scheduled to create, check raw Google presence
                for e in to_create:
                    if e.recurrence_id is None:
                        raw = google_events_raw.get((e.uid, None))
                        raw_status = (raw.get('status') if raw else None)
                        raw_id = (raw.get('id') if raw else None)
                        logger.debug(f"[PLAN][MASTER_CREATE_DIAG][{source_name}] UID={e.uid} raw_present={bool(raw)} raw_status={raw_status} raw_id={raw_id}")
            except Exception:
                logger.debug("[PLAN] Failed to enumerate plan details for debug output.")
        if logger.isEnabledFor(logging.DEBUG):
            try:
                plan_creates = [(e.uid, e.recurrence_id, bool(e.is_master_event)) for e in to_create]
                plan_updates = [(gid, e.uid, e.recurrence_id) for (gid, e) in to_update]
                plan_deletes = list(to_delete_dedup)
                logger.debug(f"[PLAN][CREATE] {plan_creates}")
                logger.debug(f"[PLAN][UPDATE] {plan_updates}")
                logger.debug(f"[PLAN][DELETE] {plan_deletes}")
            except Exception:
                logger.debug("[PLAN] Failed to enumerate plan details for debug output.")

        # Ensure created_map always exists for downstream exception creation
        created_map: Dict[Tuple[str, Optional[str]], str] = {}

        # 0) Delete non-recurring events in batch
        if to_delete_dedup:
            await self.google.batch_delete_events(to_delete_dedup)
        
        # Disown-then-delete recurring series masters (no tombstone handling)
        for uid, google_ids in recurring_series_to_delete.items():
            masters_ids = recurring_series_to_delete[uid].get('masters', [])
            
            # Disown only ACTIVE items for this UID (masters + active exceptions).
            # We intentionally do NOT include CANCELLED tombstones to avoid unnecessary PATCH calls.
            try:
                items_for_uid = await self.google.list_events_for_uid(source_name, uid, include_cancelled=False)
            except Exception as e:
                logger.warning(f"[DISOWN][RECURRING][{source_name}] UID={uid} failed to list items for disown: {e}")
                items_for_uid = []
            
            disowned_count = 0
            for item in items_for_uid:
                # Safety: skip if Google already marks it cancelled (shouldn't appear when include_cancelled=False)
                status = (item.get('status') or '').upper()
                if status == 'CANCELLED':
                    continue
                gid = item.get('id')
                if not gid:
                    continue
                ok = await self.google.disown_event(gid)
                if ok:
                    disowned_count += 1
            logger.info(f"[DISOWN][RECURRING][{source_name}] UID={uid} disowned {disowned_count} active items prior to deletion")
            
            logger.info(f"[DELETE][RECURRING][{source_name}] UID={uid} deleting masters={len(masters_ids)}")
            for google_id in masters_ids:
                logger.debug(f"[DELETE][RECURRING][{source_name}] Deleting master GID={google_id}")
                success = await self.google.delete_event(google_id)
                if not success:
                    logger.warning(f"[DELETE][RECURRING][{source_name}] Failed to delete master GID={google_id}")

        # 1) Update primary (masters and non-recurring) first
        if primary_to_update:
            await self.google.batch_update_events(primary_to_update)

        # 2) Create primary next
        if primary_to_create:
            created_map = await self.google.batch_create_events(primary_to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)

        # 3) Update exceptions (originalStartTime suppression now matches current master)
        if exceptions_to_update:
            await self.google.batch_update_events(exceptions_to_update)

        # 4) Create exceptions after primary are settled
        if exceptions_to_create:
            # After primary events are created, their Google IDs are available in the created_map.
            # We must now set the `google_recurring_event_id` on our exception models before creating them.
            newly_created_masters = {k: v for k, v in created_map.items() if k[1] is None}
            
            valid_exceptions_to_create = []
            for exception_model in exceptions_to_create:
                master_key = (exception_model.uid, None)
                master_id = newly_created_masters.get(master_key)

                if not master_id:
                    # This can happen if the master already existed and was not part of this create batch.
                    # We need to find its ID from the original google_instances map.
                    master_gcal_event = google_active_instances.get(master_key)
                    if master_gcal_event and master_gcal_event.google_event_id:
                         master_id = master_gcal_event.google_event_id

                if master_id:
                    exception_model.google_recurring_event_id = master_id
                    valid_exceptions_to_create.append(exception_model)
                else:
                    logger.error(f"[{source_name}] CRITICAL: Cannot find master Google ID for exception {exception_model.uid}/{exception_model.recurrence_id}. Skipping creation.")

            if valid_exceptions_to_create:
                created_map_exceptions = await self.google.batch_create_events(valid_exceptions_to_create)
                if created_map_exceptions:
                    await self.db.bulk_update_google_ids(source_name, created_map_exceptions)


        logger.info(f"Reconciliation finished for source: {source_name}")


    async def reconcile_source_simple(self, source_name: str):
        """
        Dedicated-calendar reconciliation (simplified, aggressive model).

        Rules:
        - DB is the single source of truth.
        - Create: Any DB event missing on Google is created.
        - Update: Any differing Google event is updated to match DB.
        - Delete:
            * Any Google event without our extendedProperties (manual/user-created) is deleted.
            * Any Google event owned by this source_name but not present in DB is deleted.
          Events owned by other sources (with caldav-mirror-source != source_name) are left untouched.

        This inherently resolves "split recurring series" duplications:
        - The user-created successor series will lack our extendedProperties => deleted on next sync.
        - The truncated original series (if Google modified it) will be updated from DB truth.
        """
        logger.info(f"[SIMPLE] Start reconciliation for source '{source_name}' (dedicated calendar mode)")

        # 1) Load desired state from DB (this source only)
        db_events_raw = await self.db.get_all_events_for_source(source_name)
        desired: Dict[Tuple[str, Optional[str]], EventModel] = {
            key: EventModel.from_dict(val['event_data'])
            for key, val in db_events_raw.items()
        }
        desired_keys = set(desired.keys())
        logger.debug(f"[SIMPLE][{source_name}] Desired keys={len(desired_keys)}")

        # 2) Load all active Google events (across entire calendar)
        all_google_items: List[Dict[str, Any]] = await self.google.list_all_events_active()

        # Partition google events:
        # - same_source_map: events for this source_name keyed by (uid, recurrence_id)
        # - to_delete_ids: IDs lacking our extendedProperties (user-created/unmanaged)
        same_source_map: Dict[Tuple[str, Optional[str]], EventModel] = {}
        to_delete_ids: List[str] = []
        kept_other_sources = 0
        # Track UIDs where we saw malformed owned exceptions so we can reset the whole series
        malformed_owned_uids: Set[str] = set()

        for item in all_google_items:
            status = (item.get('status') or '').upper()
            if status == 'CANCELLED':
                # We requested active-only, but guard anyway.
                continue

            private = ((item.get('extendedProperties') or {}).get('private')) or {}
            item_source = private.get('caldav-mirror-source')
            item_uid = private.get('caldav-mirror-uid')
            is_master = bool(item.get('recurrence')) and not item.get('recurringEventId')
            gid = item.get('id')
            ical = item.get('iCalUID')
            rec_id = item.get('recurringEventId')

            # If it's ours but belongs to a different source, leave it alone.
            if item_source and item_source != source_name:
                kept_other_sources += 1
                try:
                    logger.info(json.dumps({
                        "type": "SIMPLE_CLASSIFY",
                        "category": "other_source",
                        "source": source_name,
                        "item_source": item_source,
                        "id": gid,
                        "iCalUID": ical,
                        "is_master": is_master,
                        "recurringEventId": rec_id
                    }))
                except Exception:
                    pass
                continue

            if item_source == source_name:
                model = EventModel.from_google_event(item)
                # from_google_event returns None for malformed exceptions; skip those defensively
                if not model or not model.uid:
                    if gid:
                        to_delete_ids.append(gid)
                    # Mark the series for reset if the malformed item is one of ours and is an exception
                    if item_uid and not is_master:
                        malformed_owned_uids.add(item_uid)
                    try:
                        logger.info(json.dumps({
                            "type": "SIMPLE_CLASSIFY",
                            "category": "ours_malformed",
                            "source": source_name,
                            "id": gid,
                            "iCalUID": ical,
                            "is_master": is_master,
                            "recurringEventId": rec_id,
                            "private_uid": item_uid
                        }))
                    except Exception:
                        pass
                    continue

                key = (model.uid, model.recurrence_id)
                # Detect duplicates mapped to same (uid, recurrence_id) and delete the extra owned item
                if key in same_source_map:
                    existing = same_source_map[key]
                    gid_dup = gid
                    if gid_dup:
                        to_delete_ids.append(gid_dup)
                    try:
                        logger.info(json.dumps({
                            "type": "DUPLICATE_OWNED_KEY",
                            "source": source_name,
                            "key": [model.uid, model.recurrence_id],
                            "keep_google_id": existing.google_event_id,
                            "drop_google_id": gid_dup
                        }))
                    except Exception:
                        pass
                    continue

                same_source_map[key] = model
                try:
                    logger.info(json.dumps({
                        "type": "SIMPLE_CLASSIFY",
                        "category": "ours_master" if is_master else "ours_exception",
                        "source": source_name,
                        "id": gid,
                        "iCalUID": ical,
                        "recurringEventId": rec_id,
                        "key": [model.uid, model.recurrence_id]
                    }))
                except Exception:
                    pass
            else:
                # No ownership marker at all -> unmanaged -> delete
                if gid:
                    to_delete_ids.append(gid)
                try:
                    logger.info(json.dumps({
                        "type": "SIMPLE_CLASSIFY",
                        "category": "unmanaged",
                        "source": source_name,
                        "id": gid,
                        "iCalUID": ical,
                        "is_master": is_master,
                        "recurringEventId": rec_id
                    }))
                except Exception:
                    pass

        logger.info(f"[SIMPLE][{source_name}] Google totals: same_source={len(same_source_map)}, delete_unmanaged={len(to_delete_ids)}, kept_other_sources={kept_other_sources}")

        # --- Diagnostics: detect duplicate masters per iCalUID and RRULE truncation mismatches ---
        try:
            # Map raw items by id for quick lookups
            raw_by_id = {it.get('id'): it for it in all_google_items if it.get('id')}

            # Group owned masters by iCalUID
            owned_masters_by_ical: Dict[str, List[Dict[str, Any]]] = {}
            for item in all_google_items:
                status = (item.get('status') or '').upper()
                if status == 'CANCELLED':
                    continue
                private = ((item.get('extendedProperties') or {}).get('private')) or {}
                if private.get('caldav-mirror-source') != source_name:
                    continue
                is_master = bool(item.get('recurrence')) and not item.get('recurringEventId')
                if not is_master:
                    continue
                ical_uid = item.get('iCalUID')
                if ical_uid:
                    owned_masters_by_ical.setdefault(ical_uid, []).append(item)

            dup_groups = {ical: items for ical, items in owned_masters_by_ical.items() if len(items) > 1}
            if dup_groups:
                # Summary
                logger.info(json.dumps({
                    "type": "DIAG_DUPLICATE_MASTERS",
                    "source": source_name,
                    "group_count": len(dup_groups),
                    "total_masters": sum(len(v) for v in dup_groups.values()),
                }))
                # Per-group details
                for ical, items in dup_groups.items():
                    highlights = [self._build_event_highlights(it) for it in items]
                    logger.info(json.dumps({
                        "type": "DUPLICATE_MASTERS_GROUP",
                        "source": source_name,
                        "iCalUID": ical,
                        "items": highlights
                    }))
        except Exception as e:
            logger.debug(f"[SIMPLE][{source_name}] duplicate-masters diagnostics failed: {e}")
        # Duplicate masters by our private UID across all events -> unconditional reset
        try:
            masters_by_private_uid: Dict[str, List[Dict[str, Any]]] = {}
            for item in all_google_items:
                status = (item.get('status') or '').upper()
                if status == 'CANCELLED':
                    continue
                private = ((item.get('extendedProperties') or {}).get('private')) or {}
                if private.get('caldav-mirror-source') != source_name:
                    continue
                # Treat as master when it has recurrence and no recurringEventId
                if item.get('recurrence') and not item.get('recurringEventId'):
                    owner_uid = private.get('caldav-mirror-uid')
                    if owner_uid:
                        masters_by_private_uid.setdefault(owner_uid, []).append(item)

            for owner_uid, masters in masters_by_private_uid.items():
                if len(masters) >= 2:
                    # Disown all active items for this UID so trash tombstones won't match our filters
                    disowned3 = 0
                    try:
                        items_for_uid = await self.google.list_events_for_uid(source_name, owner_uid, include_cancelled=False)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for UID={owner_uid}: {e}")
                        items_for_uid = []
                    for it in items_for_uid:
                        status2 = (it.get('status') or '').upper()
                        if status2 == 'CANCELLED':
                            continue
                        gid2 = it.get('id')
                        if not gid2:
                            continue
                        ok = await self.google.disown_event(gid2)
                        if ok:
                            disowned3 += 1

                    # Delete all masters we found for this UID (both/all)
                    master_ids = [it.get('id') for it in masters if it.get('id')]
                    if master_ids:
                        to_delete_ids.extend(master_ids)

                    logger.info(json.dumps({
                        "type": "RESET_SERIES_DUPLICATE_BY_PRIVATE_UID",
                        "source": source_name,
                        "uid": owner_uid,
                        "masters": master_ids,
                        "disowned_count": disowned3,
                        "count": len(masters)
                    }))

                    # Schedule DB re-create for the entire series
                    reset_uids.add(owner_uid)
                    for (k_uid, k_rid), ev in desired.items():
                        if k_uid == owner_uid:
                            create_after_reset[(k_uid, k_rid)] = ev
        except Exception as e:
            logger.debug(f"[SIMPLE][{source_name}] duplicate-by-private-uid reset failed: {e}")

        try:
            # RRULE mismatch diagnostics between DB master and Google master (owned)
            masters_by_uid_db: Dict[str, EventModel] = {uid: model for (uid, rid), model in desired.items() if rid is None}
            masters_by_uid_google: Dict[str, EventModel] = {uid: model for (uid, rid), model in same_source_map.items() if rid is None}

            for uid, g_model in masters_by_uid_google.items():
                db_model = masters_by_uid_db.get(uid)
                if not db_model:
                    continue

                db_rr_can = self._canon_rrule_str(db_model.rrule)
                g_rr_can = self._canon_rrule_str(g_model.rrule)
                if db_rr_can != g_rr_can:
                    raw = raw_by_id.get(g_model.google_event_id)
                    ical = (raw or {}).get('iCalUID')
                    logger.info(json.dumps({
                        "type": "DIAG_RRULE_MISMATCH",
                        "source": source_name,
                        "uid": uid,
                        "iCalUID": ical,
                        "google_event_id": g_model.google_event_id,
                        "db_rrule": db_model.rrule,
                        "google_rrule": g_model.rrule,
                        "db_until": self._extract_until_from_rrule(db_model.rrule),
                        "google_until": self._extract_until_from_rrule(g_model.rrule)
                    }))
        except Exception as e:
            logger.debug(f"[SIMPLE][{source_name}] RRULE mismatch diagnostics failed: {e}")
        google_keys = set(same_source_map.keys())

        # --- Series reset planning (iCalUID-driven) for truncation/split ---
        reset_uids: set = set()
        create_after_reset: Dict[Tuple[str, Optional[str]], EventModel] = {}
        try:
            # Build raw_by_id locally for safety
            raw_by_id = {it.get('id'): it for it in all_google_items if it.get('id')}

            # CANCELLED tombstones (This event only delete/edit) referencing our current master -> reset series
            try:
                cancelled_map = await self.google.list_cancelled_by_source(source_name)
                # Map current active masters by UID
                master_gid_by_uid_tmp: Dict[str, str] = {
                    uid: model.google_event_id
                    for (uid, rid), model in same_source_map.items()
                    if rid is None and model and model.google_event_id
                }
                for (c_uid, c_rid), raw_item in cancelled_map.items():
                    master_gid = master_gid_by_uid_tmp.get(c_uid)
                    if not master_gid:
                        continue
                    is_cancelled_master = (c_rid is None and raw_item.get('id') == master_gid)
                    is_cancelled_instance = (c_rid is not None and raw_item.get('recurringEventId') == master_gid)
                    if not (is_cancelled_master or is_cancelled_instance):
                        continue

                    # Disown all active items so private props don't match new tombstones
                    disowned_c = 0
                    masters_ids_c: List[str] = []
                    try:
                        items_for_uid = await self.google.list_events_for_uid(source_name, c_uid, include_cancelled=False)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for CANCELLED reset UID={c_uid}: {e}")
                        items_for_uid = []
                    for it in items_for_uid:
                        status2 = (it.get('status') or '').upper()
                        if status2 != 'CANCELLED':
                            gid2 = it.get('id')
                            if gid2:
                                ok = await self.google.disown_event(gid2)
                                if ok:
                                    disowned_c += 1
                        if it.get('recurrence') and not it.get('recurringEventId'):
                            if it.get('id'):
                                masters_ids_c.append(it.get('id'))

                    if masters_ids_c:
                        to_delete_ids.extend(masters_ids_c)

                    reset_uids.add(c_uid)
                    for (k_uid, k_rid), ev in desired.items():
                        if k_uid == c_uid:
                            create_after_reset[(k_uid, k_rid)] = ev

                    try:
                        logger.info(json.dumps({
                            "type": "RESET_SERIES_CANCELLED_EXCEPTION",
                            "source": source_name,
                            "uid": c_uid,
                            "rid": c_rid,
                            "disowned_count": disowned_c,
                            "masters": masters_ids_c
                        }))
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"[SIMPLE][{source_name}] cancelled-exception reset detection failed: {e}")

            # Malformed owned exceptions detected during classification → reset series
            try:
                if malformed_owned_uids:
                    for owner_uid in list(malformed_owned_uids):
                        try:
                            items_for_uid = await self.google.list_events_for_uid(source_name, owner_uid, include_cancelled=False)
                        except Exception as e:
                            logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for malformed exception UID={owner_uid}: {e}")
                            items_for_uid = []
                        disowned_m = 0
                        masters_ids_m: List[str] = []
                        for it in items_for_uid:
                            status2 = (it.get('status') or '').upper()
                            if status2 != 'CANCELLED':
                                gid2 = it.get('id')
                                if gid2:
                                    ok = await self.google.disown_event(gid2)
                                    if ok:
                                        disowned_m += 1
                            if it.get('recurrence') and not it.get('recurringEventId'):
                                if it.get('id'):
                                    masters_ids_m.append(it.get('id'))
                        if masters_ids_m:
                            to_delete_ids.extend(masters_ids_m)
                        reset_uids.add(owner_uid)
                        for (k_uid, k_rid), ev in desired.items():
                            if k_uid == owner_uid:
                                create_after_reset[(k_uid, k_rid)] = ev
                        try:
                            logger.info(json.dumps({
                                "type": "RESET_SERIES_MALFORMED_EXCEPTION",
                                "source": source_name,
                                "uid": owner_uid,
                                "disowned_count": disowned_m,
                                "masters": masters_ids_m
                            }))
                        except Exception:
                            pass
            except Exception as e:
                logger.debug(f"[SIMPLE][{source_name}] malformed-owned-exception reset handling failed: {e}")

            # Duplicate masters by iCalUID (any ownership) -> unconditional reset
            masters_by_ical_all: Dict[str, List[Dict[str, Any]]] = {}
            for it in all_google_items:
                status = (it.get('status') or '').upper()
                if status == 'CANCELLED':
                    continue
                if (it.get('recurrence') and not it.get('recurringEventId')):
                    ical = it.get('iCalUID')
                    if ical:
                        masters_by_ical_all.setdefault(ical, []).append(it)
            for ical, masters in masters_by_ical_all.items():
                if len(masters) >= 2:
                    # Find owned item to determine DB uid
                    owner_uid = None
                    for m in masters:
                        private = ((m.get('extendedProperties') or {}).get('private')) or {}
                        if private.get('caldav-mirror-source') == source_name:
                            owner_uid = private.get('caldav-mirror-uid')
                            break
                    if not owner_uid:
                        continue
                    try:
                        items_by_ical = await self.google.list_events_by_icaluid(ical, include_cancelled=True)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_by_icaluid failed for iCalUID={ical}: {e}")
                        items_by_ical = []
                    # Disown our active items
                    disowned = 0
                    for it2 in items_by_ical:
                        status2 = (it2.get('status') or '').upper()
                        if status2 == 'CANCELLED':
                            continue
                        private2 = ((it2.get('extendedProperties') or {}).get('private')) or {}
                        if private2.get('caldav-mirror-source') == source_name and it2.get('id'):
                            ok = await self.google.disown_event(it2.get('id'))
                            if ok:
                                disowned += 1
                    # Delete all masters in group
                    masters_ids = [it2.get('id') for it2 in items_by_ical if it2.get('id') and (it2.get('recurrence') and not it2.get('recurringEventId'))]
                    if masters_ids:
                        to_delete_ids.extend(masters_ids)
                    logger.info(json.dumps({"type": "RESET_SERIES_DUPLICATE_MASTERS", "source": source_name, "uid": owner_uid, "iCalUID": ical, "masters": masters_ids, "disowned_count": disowned, "total_masters": len(masters)}))
                    # Schedule DB recreate
                    reset_uids.add(owner_uid)
                    for (k_uid, k_rid), ev in desired.items():
                        if k_uid == owner_uid:
                            create_after_reset[(k_uid, k_rid)] = ev

            # Duplicate masters by our UID (owned) -> unconditional reset
            try:
                # Check for multiple owned masters within the same UID regardless of iCalUID
                for owner_uid, g_model in masters_by_uid_google.items():
                    try:
                        items_for_uid = await self.google.list_events_for_uid(source_name, owner_uid, include_cancelled=False)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for UID={owner_uid}: {e}")
                        items_for_uid = []
                    master_ids = [it.get('id') for it in items_for_uid if it.get('id') and (it.get('recurrence') and not it.get('recurringEventId'))]
                    if len(master_ids) >= 2:
                        # Disown active items for this UID
                        disowned2 = 0
                        for it in items_for_uid:
                            status2 = (it.get('status') or '').upper()
                            if status2 == 'CANCELLED':
                                continue
                            gid2 = it.get('id')
                            if not gid2:
                                continue
                            ok = await self.google.disown_event(gid2)
                            if ok:
                                disowned2 += 1
                        # Delete all masters we found (both)
                        to_delete_ids.extend(master_ids)
                        logger.info(json.dumps({"type": "RESET_SERIES_DUPLICATE_BY_UID", "source": source_name, "uid": owner_uid, "masters": master_ids, "disowned_count": disowned2}))
                        reset_uids.add(owner_uid)
                        for (k_uid, k_rid), ev in desired.items():
                            if k_uid == owner_uid:
                                create_after_reset[(k_uid, k_rid)] = ev
            except Exception as e:
                logger.warning(f"[SIMPLE][{source_name}] UID-duplicate reset planning failed: {e}")

            # --- Unauthorized single-instance modifications ("This event only") ---
            # Case A: Owned exception exists in Google but not in DB → reset whole series.
            extra_owned_keys = google_keys - desired_keys
            for (ex_uid, ex_rid) in list(extra_owned_keys):
                if ex_rid is None:
                    continue  # masters handled elsewhere
                if (ex_uid, None) not in desired_keys:
                    continue  # not a series we manage
                try:
                    items_for_uid = await self.google.list_events_for_uid(source_name, ex_uid, include_cancelled=False)
                except Exception as e:
                    logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for unauthorized exception UID={ex_uid}: {e}")
                    items_for_uid = []
                # Disown all active items to prevent privateExtendedProperty being carried to tombstones
                disowned_ex = 0
                masters_ids_ex = []
                for it in items_for_uid:
                    status2 = (it.get('status') or '').upper()
                    if status2 != 'CANCELLED':
                        gid2 = it.get('id')
                        if gid2:
                            ok = await self.google.disown_event(gid2)
                            if ok:
                                disowned_ex += 1
                    # Collect masters to delete
                    if it.get('recurrence') and not it.get('recurringEventId'):
                        if it.get('id'):
                            masters_ids_ex.append(it.get('id'))
                if masters_ids_ex:
                    to_delete_ids.extend(masters_ids_ex)
                reset_uids.add(ex_uid)
                for (k_uid, k_rid), ev in desired.items():
                    if k_uid == ex_uid:
                        create_after_reset[(k_uid, k_rid)] = ev
                try:
                    logger.info(json.dumps({
                        "type": "RESET_SERIES_UNAUTHORIZED_EXCEPTION",
                        "source": source_name,
                        "uid": ex_uid,
                        "rid": ex_rid,
                        "disowned_count": disowned_ex,
                        "masters": masters_ids_ex
                    }))
                except Exception:
                    pass

            # Case B: Unmanaged exception (no private props) pointing at our master → reset the owner series.
            try:
                master_gid_by_uid_tmp: Dict[str, str] = {
                    uid: model.google_event_id
                    for (uid, rid), model in same_source_map.items()
                    if rid is None and model and model.google_event_id
                }
                # Scan all items; if an item has recurringEventId matching one of our masters but lacks our ownership, reset.
                for it in all_google_items:
                    status = (it.get('status') or '').upper()
                    if status == 'CANCELLED':
                        continue
                    rec_id = it.get('recurringEventId')
                    private = ((it.get('extendedProperties') or {}).get('private')) or {}
                    item_source = private.get('caldav-mirror-source')
                    if not rec_id:
                        continue
                    # Find which UID this rec_id belongs to
                    owner_uid = None
                    for u, gid in master_gid_by_uid_tmp.items():
                        if gid and rec_id == gid:
                            owner_uid = u
                            break
                    if not owner_uid:
                        continue
                    if item_source == source_name:
                        # It's ours; this case is covered by extra_owned_keys above
                        continue
                    # Unmanaged exception pointing at our master -> reset owner_uid
                    try:
                        items_for_uid = await self.google.list_events_for_uid(source_name, owner_uid, include_cancelled=False)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_for_uid failed for unmanaged exception UID={owner_uid}: {e}")
                        items_for_uid = []
                    disowned_ex2 = 0
                    masters_ids_ex2 = []
                    for it2 in items_for_uid:
                        status2 = (it2.get('status') or '').upper()
                        if status2 != 'CANCELLED':
                            gid2 = it2.get('id')
                            if gid2:
                                ok = await self.google.disown_event(gid2)
                                if ok:
                                    disowned_ex2 += 1
                        if it2.get('recurrence') and not it2.get('recurringEventId'):
                            if it2.get('id'):
                                masters_ids_ex2.append(it2.get('id'))
                    if masters_ids_ex2:
                        to_delete_ids.extend(masters_ids_ex2)
                    reset_uids.add(owner_uid)
                    for (k_uid, k_rid), ev in desired.items():
                        if k_uid == owner_uid:
                            create_after_reset[(k_uid, k_rid)] = ev
                    try:
                        logger.info(json.dumps({
                            "type": "RESET_SERIES_UNMANAGED_EXCEPTION",
                            "source": source_name,
                            "uid": owner_uid,
                            "recurringEventId": rec_id,
                            "disowned_count": disowned_ex2,
                            "masters": masters_ids_ex2
                        }))
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"[SIMPLE][{source_name}] unmanaged-exception reset detection failed: {e}")

            # Recompute masters maps
            masters_by_uid_db = {uid: model for (uid, rid), model in desired.items() if rid is None}
            masters_by_uid_google = {uid: model for (uid, rid), model in same_source_map.items() if rid is None}

            # Canonical RRULE mismatch → candidate for reset (typically Google added UNTIL)
            for uid, g_model in masters_by_uid_google.items():
                db_model = masters_by_uid_db.get(uid)
                if not db_model:
                    continue
                db_rr_can = self._canon_rrule_str(db_model.rrule)
                g_rr_can = self._canon_rrule_str(g_model.rrule)
                if db_rr_can != g_rr_can:
                    # Resolve iCalUID of the owned master
                    raw = raw_by_id.get(g_model.google_event_id)
                    ical = (raw or {}).get('iCalUID')
                    if not ical:
                        continue

                    # Fetch entire group by iCalUID (captures successor masters lacking our private props)
                    try:
                        items_by_ical = await self.google.list_events_by_icaluid(ical, include_cancelled=True)
                    except Exception as e:
                        logger.warning(f"[SIMPLE][{source_name}] list_events_by_icaluid failed for iCalUID={ical}: {e}")
                        items_by_ical = []

                    # Disown our active items for this series to prevent privateExtendedProperty-based matches on tombstones
                    disowned = 0
                    for it in items_by_ical:
                        status = (it.get('status') or '').upper()
                        if status == 'CANCELLED':
                            continue
                        private = ((it.get('extendedProperties') or {}).get('private')) or {}
                        if private.get('caldav-mirror-source') == source_name and it.get('id'):
                            ok = await self.google.disown_event(it.get('id'))
                            if ok:
                                disowned += 1
                    logger.info(json.dumps({"type": "RESET_SERIES_PREP", "source": source_name, "uid": uid, "iCalUID": ical, "disowned_count": disowned}))

                    # Delete ALL masters for this iCalUID (R1 and any R2)
                    masters_ids = [it.get('id') for it in items_by_ical if it.get('id') and (it.get('recurrence') and not it.get('recurringEventId'))]
                    if masters_ids:
                        to_delete_ids.extend(masters_ids)
                        logger.info(json.dumps({"type": "RESET_SERIES_DELETE_MASTERS", "source": source_name, "uid": uid, "iCalUID": ical, "masters": masters_ids}))

                    # Mark UID for reset and queue full re-create from DB after deletion
                    reset_uids.add(uid)
                    for (k_uid, k_rid), ev in desired.items():
                        if k_uid == uid:
                            create_after_reset[(k_uid, k_rid)] = ev
        except Exception as e:
            logger.warning(f"[SIMPLE][{source_name}] reset planning failed: {e}")

        # 3) Compute reconciliation sets for this source
        keys_to_create = desired_keys - google_keys
        keys_to_update = desired_keys & google_keys
        extra_google_keys = google_keys - desired_keys  # ours on Google but not in DB => delete

        # 3a) Queue deletes for "extra" same-source events
        for key in extra_google_keys:
            gid = same_source_map[key].google_event_id
            if gid:
                to_delete_ids.append(gid)

        # 3b) Prepare update list (ensure exception hash parity by populating recurringEventId on desired)
        to_update: List[Tuple[str, EventModel]] = []
        master_gid_by_uid: Dict[str, str] = {
            uid: model.google_event_id
            for (uid, rid), model in same_source_map.items()
            if rid is None and model and model.google_event_id
        }

        for key in keys_to_update:
            db_model = desired[key]
            g_model = same_source_map[key]

            # Ensure DB exception models carry master recurringEventId prior to hashing
            if db_model.recurrence_id and not db_model.google_recurring_event_id:
                master_id = master_gid_by_uid.get(db_model.uid)
                if master_id:
                    db_model.google_recurring_event_id = master_id

            if db_model.compute_hash() != g_model.compute_hash():
                db_model.google_event_id = g_model.google_event_id
                to_update.append((g_model.google_event_id, db_model))

        # Drop updates for series scheduled for reset
        try:
            if reset_uids:
                before = len(to_update)
                to_update = [(gid, e) for (gid, e) in to_update if e.uid not in reset_uids]
                logger.info(json.dumps({"type": "RESET_SERIES_FILTER_UPDATES", "source": source_name, "removed": before - len(to_update), "remaining": len(to_update)}))
        except NameError:
            # reset_uids not defined
            pass

        # 3c) Prepare create lists
        to_create: List[EventModel] = [desired[k] for k in keys_to_create]
        # Add full series to create for any series scheduled for reset, and de-duplicate
        try:
            if create_after_reset:
                merge_map: Dict[Tuple[str, Optional[str]], EventModel] = {(e.uid, e.recurrence_id): e for e in to_create}
                for key, ev in create_after_reset.items():
                    merge_map[key] = ev
                to_create = list(merge_map.values())
                logger.info(json.dumps({"type": "RESET_SERIES_ADD_CREATES", "source": source_name, "added": len(create_after_reset)}))
        except NameError:
            # create_after_reset not defined
            pass

        # Partition by primary vs exceptions
        primary_to_create: List[EventModel] = [e for e in to_create if not e.recurrence_id]
        exceptions_to_create: List[EventModel] = [e for e in to_create if e.recurrence_id]
        primary_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if not e.recurrence_id]
        exceptions_to_update: List[Tuple[str, EventModel]] = [(gid, e) for (gid, e) in to_update if e.recurrence_id]

        # 4) Execute plan (Delete -> Update primary -> Create primary -> Update exceptions -> Create exceptions)

        # 4.1) Deletes (dedup to avoid batch errors)
        if to_delete_ids:
            dedup_delete = list(dict.fromkeys(to_delete_ids))
            logger.info(f"[SIMPLE][{source_name}] Deleting {len(dedup_delete)} Google events (unmanaged + extra).")
            await self.google.batch_delete_events(dedup_delete)

        # 4.2) Update primary
        if primary_to_update:
            logger.info(f"[SIMPLE][{source_name}] Updating {len(primary_to_update)} primary events.")
            await self.google.batch_update_events(primary_to_update)

        # 4.3) Create primary
        created_map: Dict[Tuple[str, Optional[str]], str] = {}
        if primary_to_create:
            logger.info(f"[SIMPLE][{source_name}] Creating {len(primary_to_create)} primary events.")
            created_map = await self.google.batch_create_events(primary_to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)
                # Extend master map with newly created masters
                for (uid, rid), gid in created_map.items():
                    if rid is None:
                        master_gid_by_uid[uid] = gid

        # 4.4) Update exceptions
        if exceptions_to_update:
            logger.info(f"[SIMPLE][{source_name}] Updating {len(exceptions_to_update)} exceptions.")
            await self.google.batch_update_events(exceptions_to_update)

        # 4.5) Create exceptions (after masters exist; set recurringEventId)
        if exceptions_to_create:
            valid_exceptions: List[EventModel] = []
            for ex in exceptions_to_create:
                master_id = ex.google_recurring_event_id or master_gid_by_uid.get(ex.uid)
                if master_id:
                    ex.google_recurring_event_id = master_id
                    valid_exceptions.append(ex)
                else:
                    logger.error(f"[SIMPLE][{source_name}] Cannot resolve master Google ID for exception {ex.uid}/{ex.recurrence_id}. Skipping creation.")
            if valid_exceptions:
                created_ex_map = await self.google.batch_create_events(valid_exceptions)
                if created_ex_map:
                    await self.db.bulk_update_google_ids(source_name, created_ex_map)

        logger.info(f"[SIMPLE] Reconciliation finished for source '{source_name}'")
