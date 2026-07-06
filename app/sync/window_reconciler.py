"""
Windowed, flattened-instance reconciler

Treats Google Calendar as a cache of explicitly projected instances over a rolling window.
Avoids RRULE/EXDATE semantics in Google by creating independent events per occurrence.

Core responsibilities:
- Compute rolling window bounds from ENV (hot-reloaded every cycle)
- Project per-instance occurrences from DB series (RRULE expansion + exceptions)
- Compute deterministic window fingerprint
- Skip via Google syncToken AND fingerprint when possible
- Set arithmetic diff: create missing, delete extras, replace changed
- Hot garbage-collect out-of-window owned instances when window shrinks
- Tag events using extendedProperties.private for ownership and hashing
"""

import os
import json
import hashlib
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta, date, timezone

import pytz
from dateutil import rrule
from dateutil.relativedelta import relativedelta

from database import Database
from .event_model import EventModel  # Source model (from CalDAV)
from .google_client import GoogleClient

logger = logging.getLogger(__name__)

# --------- Helpers ---------

def _rfc3339_z(dt_utc: datetime) -> str:
    dt_utc = dt_utc.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z")

def _parse_yyyy_mm_dd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _safe_event_tz(tzname: Optional[str]) -> pytz.BaseTzInfo:
    if not tzname:
        return pytz.UTC
    # Normalize to string; handle tzical and GMT/UTC fixed-offset forms
    s = str(tzname).strip()
    try:
        # Local import to avoid adding a top-level dependency churn
        import re

        # Unwrap tzical repr like: <tzicalvtz 'GMT-0400'> or <tzicalvtz "GMT-0400">
        m = re.match(r"^<tzicalvtz\s+['\"]([^'\"]+)['\"]>$", s)
        if m:
            s = m.group(1)

        # Match GMT/UTC with HHMM or HH:MM, e.g., GMT-0400, GMT-04:00, UTC+0530, UTC+05:30
        m = re.match(r"^(?:GMT|UTC)\s*([+-])\s*(\d{2}):?(\d{2})$", s, re.IGNORECASE)
        if m:
            sign = 1 if m.group(1) == "+" else -1
            hh = int(m.group(2))
            mm = int(m.group(3))
            offset_min = sign * (hh * 60 + mm)
            return pytz.FixedOffset(offset_min)

        # Also accept hour-only forms like GMT-4 or UTC+9
        m = re.match(r"^(?:GMT|UTC)\s*([+-])\s*(\d{1,2})$", s, re.IGNORECASE)
        if m:
            sign = 1 if m.group(1) == "+" else -1
            hh = int(m.group(2))
            offset_min = sign * (hh * 60)
            return pytz.FixedOffset(offset_min)

        # Fall back to named timezone parsing
        return pytz.timezone(s)
    except Exception:
        logger.warning(f"Unknown timezone '{tzname}', falling back to UTC.")
        return pytz.UTC

def _to_aware_local(dt_or_date: Any, event_tz: pytz.BaseTzInfo) -> datetime:
    if isinstance(dt_or_date, datetime):
        if dt_or_date.tzinfo:
            return dt_or_date.astimezone(event_tz)
        return event_tz.localize(dt_or_date)
    if isinstance(dt_or_date, date):
        return event_tz.localize(datetime(dt_or_date.year, dt_or_date.month, dt_or_date.day))
    raise ValueError("Unsupported temporal type")

def _rid_to_local_dt(master: EventModel, rid: str) -> Optional[datetime]:
    tz = _safe_event_tz(master.timezone)
    try:
        if len(rid) == 8 and rid.isdigit():
            # YYYYMMDD date-only
            return tz.localize(datetime.strptime(rid, "%Y%m%d"))
        # Datetime; accept both with and without trailing Z, interpret as UTC then convert
        if rid.endswith("Z"):
            rid_dt_utc = datetime.strptime(rid, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        else:
            rid_dt_utc = datetime.strptime(rid, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        return rid_dt_utc.astimezone(tz)
    except Exception:
        return None

def _event_duration(master: EventModel) -> Tuple[Optional[timedelta], bool]:
    """Return (duration, is_all_day) using master times."""
    if master.start_date or master.end_date:
        # All-day: end is exclusive; default 1 day if missing
        try:
            start_d = _parse_yyyy_mm_dd(master.start_date) if master.start_date else None
            end_d = _parse_yyyy_mm_dd(master.end_date) if master.end_date else None
            if start_d and end_d:
                return (timedelta(days=(end_d - start_d).days), True)
            # Assume 1 day if ambiguous
            return (timedelta(days=1), True)
        except Exception:
            return (timedelta(days=1), True)
    # Timed
    if master.start_datetime and master.end_datetime:
        return (master.end_datetime - master.start_datetime, False)
    return (None, False)

def _clamp_int(val: int, lo: int, hi: int, name: str) -> int:
    if val < lo:
        logger.warning(f"{name} below min; clamping {val} -> {lo}")
        return lo
    if val > hi:
        logger.warning(f"{name} above max; clamping {val} -> {hi}")
        return hi
    return val

def _norm_text(text: Optional[str]) -> Optional[str]:
    """
    Canonicalize free-text fields to avoid churn between DB and Google:
    - Normalize line endings to LF
    - Convert NBSP variants to regular spaces
    - Trim trailing whitespace on each line
    - Strip leading/trailing whitespace
    - Collapse to None when empty after normalization
    """
    if text is None:
        return None
    s = str(text)
    # Normalize line endings
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # Replace non-breaking spaces with regular spaces
    s = s.replace("\u00A0", " ").replace("\u202F", " ")
    # Trim trailing whitespace on each line
    s = "\n".join(line.rstrip() for line in s.split("\n"))
    # Strip overall
    s = s.strip()
    return s if s else None

# RRULE helpers for parse-time normalization of UNTIL to UTC when DTSTART is tz-aware
def _extract_until_token(rrule_str: Optional[str]) -> Optional[str]:
    if not rrule_str:
        return None
    try:
        parts = [p for p in rrule_str.split(";") if p]
        for p in parts:
            if p.upper().startswith("UNTIL="):
                return p.split("=", 1)[1].strip()
    except Exception:
        return None
    return None

def _normalize_rrule_until_utc(rrule_str: Optional[str], dtstart_local: datetime) -> Tuple[str, Optional[str], Optional[str]]:
    """
    If RRULE has UNTIL without Z while DTSTART is timezone-aware, convert UNTIL to UTC Z form.
    Returns (normalized_rrule, until_raw, until_normalized)
    """
    if not rrule_str:
        return rrule_str, None, None
    until_raw: Optional[str] = None
    until_norm: Optional[str] = None
    try:
        parts = [p for p in rrule_str.split(";") if p]
        new_parts: List[str] = []
        tz = getattr(dtstart_local, "tzinfo", None)
        for p in parts:
            if p.upper().startswith("UNTIL="):
                val = p.split("=", 1)[1].strip()
                until_raw = val
                # If already UTC Z, keep as-is
                if isinstance(val, str) and val.endswith("Z"):
                    until_norm = val
                    new_parts.append(f"UNTIL={val}")
                else:
                    # Convert local or date-only UNTIL to UTC Z
                    try:
                        if isinstance(val, str) and len(val) == 8 and val.isdigit():
                            # YYYYMMDD -> treat as local midnight
                            dt_naive = datetime.strptime(val, "%Y%m%d")
                            if hasattr(tz, "localize"):
                                local_dt = tz.localize(datetime(dt_naive.year, dt_naive.month, dt_naive.day, 0, 0, 0)) if tz else datetime(dt_naive.year, dt_naive.month, dt_naive.day, 0, 0, 0)
                            else:
                                local_dt = datetime(dt_naive.year, dt_naive.month, dt_naive.day, 0, 0, 0, tzinfo=tz) if tz else datetime(dt_naive.year, dt_naive.month, dt_naive.day, 0, 0, 0)
                        else:
                            # Expect YYYYMMDDTHHMMSS (no Z) -> interpret in event tz
                            dt_naive = datetime.strptime(val, "%Y%m%dT%H%M%S")
                            if hasattr(tz, "localize"):
                                local_dt = tz.localize(dt_naive) if tz else dt_naive
                            else:
                                local_dt = dt_naive.replace(tzinfo=tz) if tz else dt_naive
                        until_z = local_dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                        until_norm = until_z
                        new_parts.append(f"UNTIL={until_z}")
                    except Exception:
                        # Fallback to original token unchanged
                        new_parts.append(p)
            else:
                new_parts.append(p)
        return ";".join(new_parts), until_raw, until_norm
    except Exception:
        return rrule_str, None, None
# --------- Projected Event (flattened instance) ---------

@dataclass
class ProjectedEvent:
    uid: str
    source_name: str
    summary: Optional[str]
    description: Optional[str]
    location: Optional[str]
    # Effective instance timing (final, after exceptions)
    is_all_day: bool
    start_date: Optional[str]      # YYYY-MM-DD when all-day
    end_date: Optional[str]        # YYYY-MM-DD (exclusive) when all-day
    start_dt: Optional[datetime]   # aware datetime when timed
    end_dt: Optional[datetime]     # aware datetime when timed
    time_zone: Optional[str]       # when timed, Google timeZone to emit (keep original if possible)

    # Interface parity for GoogleClient batch calls
    recurrence_id: Optional[str] = None
    is_master_event: bool = False
    google_event_id: Optional[str] = None
    transparency: str = "OPAQUE"

    def instance_key(self) -> str:
        """Stable key: uid + final start + final end in UTC or date form."""
        if self.is_all_day:
            s = self.start_date or ""
            e = self.end_date or ""
            return f"{self.uid}#{s}#{e}"
        # Timed -> normalize to Z
        s = _rfc3339_z(self.start_dt.astimezone(timezone.utc)) if self.start_dt else ""
        e = _rfc3339_z(self.end_dt.astimezone(timezone.utc)) if self.end_dt else ""
        return f"{self.uid}#{s}#{e}"

    def to_google_event(self) -> Dict[str, Any]:
        # Normalize text to match Google's stored representation and stabilize hashes
        norm_summary = _norm_text(self.summary) or ""
        norm_description = _norm_text(self.description)
        norm_location = _norm_text(self.location)

        body: Dict[str, Any] = {
            "summary": norm_summary,
        }
        if norm_description is not None:
            body["description"] = norm_description
        if norm_location is not None:
            body["location"] = norm_location

        if self.is_all_day:
            body["start"] = {"date": self.start_date}
            body["end"] = {"date": self.end_date}
        else:
            tzname = self.time_zone or "UTC"
            sdt = self.start_dt.replace(microsecond=0)
            edt = self.end_dt.replace(microsecond=0)
            body["start"] = {
                "dateTime": sdt.isoformat().replace("+00:00", "Z") if sdt.tzinfo == timezone.utc else sdt.isoformat(),
                "timeZone": tzname
            }
            body["end"] = {
                "dateTime": edt.isoformat().replace("+00:00", "Z") if edt.tzinfo == timezone.utc else edt.isoformat(),
                "timeZone": tzname
            }

        body["transparency"] = "transparent" if self.transparency == "TRANSPARENT" else "opaque"


        # No recurrence semantics; each instance is standalone
        # Ownership and integrity tags
        contents_hash = self.compute_hash()
        body["extendedProperties"] = {
            "private": {
                "caldav-mirror-source": self.source_name,
                "caldav-mirror-uid": self.uid,
                "caldav-mirror-instance-key": self.instance_key(),
                "caldav-mirror-hash": contents_hash,
                "caldav-mirror-version": "flat-1"
            }
        }
        return body

    def compute_hash(self) -> str:
        """
        Fingerprint of the canonical projected instance payload.
        Only fields that we push to Google and expect to remain stable.
        Notes:
        - Excludes timeZone to avoid churn when Google omits/normalizes it but preserves absolute times.
        - Normalizes free-text fields to align with Google's stored representation.
        """
        norm_summary = _norm_text(self.summary) or ""
        norm_description = _norm_text(self.description)
        norm_location = _norm_text(self.location)

        if self.is_all_day:
            core = {
                "summary": norm_summary,
                "description": norm_description,
                "location": norm_location,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "visibility": "public",
                "transparency": self.transparency,
            }
        else:
            core = {
                "summary": norm_summary,
                "description": norm_description,
                "location": norm_location,
                "start_dt_z": _rfc3339_z(self.start_dt.astimezone(timezone.utc)) if self.start_dt else None,
                "end_dt_z": _rfc3339_z(self.end_dt.astimezone(timezone.utc)) if self.end_dt else None,
                "visibility": "public",
                "transparency": self.transparency,
            }
        blob = json.dumps(core, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

# --------- Reconciler ---------

class WindowReconciler:
    def __init__(self, google_client: GoogleClient, database: Database):
        self.google = google_client
        self.db = database

    # ENV hot-reloaded each call
    def _compute_window_bounds(self) -> Tuple[datetime, datetime, str, str]:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        past_days = int(os.getenv("PROJECTION_WINDOW_PAST_DAYS", "30") or "30")
        future_months = int(os.getenv("PROJECTION_WINDOW_FUTURE_MONTHS", "18") or "18")

        past_days = _clamp_int(past_days, lo=0, hi=3650, name="PROJECTION_WINDOW_PAST_DAYS")
        future_months = _clamp_int(future_months, lo=0, hi=60, name="PROJECTION_WINDOW_FUTURE_MONTHS")

        start = now - timedelta(days=past_days)
        end = now + relativedelta(months=future_months)

        return start, end, _rfc3339_z(start), _rfc3339_z(end)

    async def reconcile_window(self, source_name: str) -> None:
        """
        Main entry: perform deterministic, idempotent reconciliation for a window.
        """
        ws_dt, we_dt, ws_str, we_str = self._compute_window_bounds()

        # Load desired DB state to project
        db_series_raw = await self.db.get_all_events_for_source(source_name)
        series_by_uid: Dict[str, List[EventModel]] = {}
        for (_uid, _rid), entry in db_series_raw.items():
            ev = EventModel.from_dict(entry["event_data"])
            series_by_uid.setdefault(ev.uid, []).append(ev)

        desired_instances: Dict[str, ProjectedEvent] = self._project_instances(series_by_uid, ws_dt, we_dt, source_name)

        # Compute fingerprint (sorted list of key:hash)
        fingerprint = self._fingerprint(desired_instances)

        # Skip gate: Google sync token + window fingerprint
        prev_proj = await self.db.get_projection_state(source_name)
        prev_ws = (prev_proj or {}).get("window_start_utc")
        prev_we = (prev_proj or {}).get("window_end_utc")
        prev_fp = (prev_proj or {}).get("window_fingerprint")

        google_token = await self.db.get_app_state("google_next_sync_token")
        google_changed = True
        next_token: Optional[str] = None
        if google_token:
            google_changed, next_token = await self.google.has_changes_since(google_token)

        can_skip = (not google_changed) and (prev_ws == ws_str) and (prev_we == we_str) and (prev_fp == fingerprint)

        logger.info(json.dumps({
            "type": "WINDOW_SKIP_GATE",
            "source": source_name,
            "google_changed": google_changed,
            "window_start_utc": ws_str,
            "window_end_utc": we_str,
            "prev_window_start_utc": prev_ws,
            "prev_window_end_utc": prev_we,
            "fingerprint_match": prev_fp == fingerprint,
            "decision_skip": can_skip
        }))

        if can_skip:
            # Update token if provided; no-op otherwise
            if next_token:
                try:
                    await self.db.set_app_state("google_next_sync_token", next_token)
                except Exception:
                    pass
            return

        # List currently owned items in window and classify
        owned_in_window, rogue_ids = await self._scan_owned_in_window(source_name, ws_dt, we_dt)

        # Set arithmetic
        desired_keys = set(desired_instances.keys())
        google_keys = set(owned_in_window.keys())

        to_create_keys = desired_keys - google_keys
        to_delete_ids: List[str] = list(rogue_ids)  # start with rogue owned artifacts (legacy recurrence, malformed)
        to_replace_keys: List[str] = []  # intersection with hash mismatch

        for k in (desired_keys & google_keys):
            desired_hash = desired_instances[k].compute_hash()
            google_hash = owned_in_window[k]["hash"]
            if desired_hash != google_hash:
                # Replace for simplicity
                gid = owned_in_window[k]["id"]
                if gid:
                    to_delete_ids.append(gid)
                to_replace_keys.append(k)

        # Extras present in Google but not desired anymore (e.g., deleted in source) -> delete
        extras_keys = google_keys - desired_keys
        for k in extras_keys:
            gid = owned_in_window.get(k, {}).get("id")
            if gid:
                to_delete_ids.append(gid)
        
        # Create events for both new and replaced
        to_create_all: List[ProjectedEvent] = []
        for k in (list(to_create_keys) + list(to_replace_keys)):
            to_create_all.append(desired_instances[k])

        # Hot garbage collection when window shrinks
        shrank = False
        if prev_ws and prev_we:
            try:
                prev_ws_dt = datetime.fromisoformat(prev_ws.replace("Z", "+00:00"))
                prev_we_dt = datetime.fromisoformat(prev_we.replace("Z", "+00:00"))
                shrank = (ws_dt >= prev_ws_dt and we_dt <= prev_we_dt) and (ws_dt != prev_ws_dt or we_dt != prev_we_dt)
            except Exception:
                shrank = False

        if shrank:
            out_ids = await self._collect_out_of_window_owned_ids(source_name, ws_dt, we_dt)
            if out_ids:
                to_delete_ids.extend(out_ids)

        # Global sweep: delete any owned items (anywhere in calendar) whose instance_key is not in desired set
        # Handles Google-side edits that move items outside the window or otherwise diverge from DB projection.
        extras_global_ids = await self._collect_owned_not_in_desired(source_name, set(desired_instances.keys()))
        if extras_global_ids:
            to_delete_ids.extend(extras_global_ids)

        # Emit plan
        logger.info(json.dumps({
            "type": "PROJECTION_PLAN",
            "source": source_name,
            "desired_keys": len(desired_keys),
            "google_keys": len(google_keys),
            "create_count": len(to_create_all),
            "delete_count": len(to_delete_ids)
        }))

        # Execute plan: delete first (dedup), then create
        dry_run = str(os.getenv("PROJECTION_DRY_RUN", "false")).lower() in ("1", "true", "yes", "on")
        if dry_run:
            logger.info(json.dumps({
                "type": "PROJECTION_DRY_RUN",
                "source": source_name,
                "delete_ids": list(dict.fromkeys([i for i in to_delete_ids if i])),
                "create_count": len(to_create_all)
            }))
        else:
            if to_delete_ids:
                dedup_del = list(dict.fromkeys([i for i in to_delete_ids if i]))
                await self.google.batch_delete_events(dedup_del)
            
            if to_create_all:
                await self.google.batch_create_events(to_create_all)  # IDs not persisted; cache-only by design

        # Persist new projection fingerprint and bounds only when not dry-run
        if not dry_run:
            await self.db.set_projection_state(source_name, ws_str, we_str, fingerprint)

            # Sync token maintenance
            if next_token:
                await self.db.set_app_state("google_next_sync_token", next_token)
            else:
                # If token was invalid or missing, fetch a baseline nextSyncToken
                try:
                    fresh_token = await self.google.fetch_next_sync_token()
                    if fresh_token:
                        await self.db.set_app_state("google_next_sync_token", fresh_token)
                except Exception:
                    pass
        else:
            logger.info(json.dumps({
                "type": "PROJECTION_DRY_RUN_SKIPPED_STATE",
                "source": source_name
            }))

    # ----- Internal methods -----

    def _fingerprint(self, desired_instances: Dict[str, ProjectedEvent]) -> str:
        pairs = sorted([(k, v.compute_hash()) for k, v in desired_instances.items()], key=lambda x: x[0])
        blob = json.dumps(pairs, sort_keys=True)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    async def _scan_owned_in_window(self, source_name: str, ws_dt: datetime, we_dt: datetime) -> Tuple[Dict[str, Dict[str, Any]], Set[str]]:
        """
        Return:
            - map instance_key -> {id, hash}
            - rogue_ids: owned items to delete (e.g., legacy masters, malformed without instance-key, duplicates)
        """
        time_min = _rfc3339_z(ws_dt)
        time_max = _rfc3339_z(we_dt)
        items = await self.google.list_events_window(time_min, time_max, single_events=True)

        owned: Dict[str, Dict[str, Any]] = {}
        rogue_ids: Set[str] = set()

        for it in items:
            status = (it.get("status") or "").upper()
            if status == "CANCELLED":
                continue
            private = ((it.get("extendedProperties") or {}).get("private")) or {}
            gid = it.get("id")

            # Dedicated calendar: any unmanaged event (no ownership marker) is rogue and must be deleted.
            if not private or not private.get("caldav-mirror-source"):
                if gid:
                    rogue_ids.add(gid)
                continue

            # Keep items owned by other sources (multi-source aggregation); only reconcile current source here.
            if private.get("caldav-mirror-source") != source_name:
                continue

            inst_key = private.get("caldav-mirror-instance-key")
            recurrence = it.get("recurrence")
            rec_ev_id = it.get("recurringEventId")

            # Any owned recurrence artifacts (masters or instances) from legacy approach should be removed.
            if recurrence and not rec_ev_id:
                if gid:
                    rogue_ids.add(gid)
                continue

            # Compute observed content hash from the actual Google fields to detect unauthorized edits.
            # We do NOT trust the stored private hash because a user edit would not update it.
            try:
                start = (it.get("start") or {})
                end = (it.get("end") or {})
                if "date" in start and "date" in end:
                    pe = ProjectedEvent(
                        uid=private.get("caldav-mirror-uid"),
                        source_name=source_name,
                        summary=it.get("summary"),
                        description=it.get("description"),
                        location=it.get("location"),
                        is_all_day=True,
                        start_date=start.get("date"),
                        end_date=end.get("date"),
                        start_dt=None,
                        end_dt=None,
                        time_zone=None,
                        transparency=("TRANSPARENT" if str(it.get("transparency", "opaque")).lower() == "transparent" else "OPAQUE"),
                    )
                else:
                    dt_s = start.get("dateTime")
                    dt_e = end.get("dateTime")
                    tzname = start.get("timeZone") or end.get("timeZone") or "UTC"

                    def _parse_dt(s: Optional[str]) -> Optional[datetime]:
                        if not s:
                            return None
                        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                        return dt.replace(microsecond=0)

                    pe = ProjectedEvent(
                        uid=private.get("caldav-mirror-uid"),
                        source_name=source_name,
                        summary=it.get("summary"),
                        description=it.get("description"),
                        location=it.get("location"),
                        is_all_day=False,
                        start_date=None,
                        end_date=None,
                        start_dt=_parse_dt(dt_s),
                        end_dt=_parse_dt(dt_e),
                        time_zone=tzname,
                        transparency=("TRANSPARENT" if str(it.get("transparency", "opaque")).lower() == "transparent" else "OPAQUE"),
                    )
                observed_hash = pe.compute_hash()
            except Exception:
                observed_hash = None

            # Malformed: missing our integrity markers or cannot compute observed hash
            if not inst_key or not observed_hash:
                if gid:
                    rogue_ids.add(gid)
                continue

            # Duplicate detection: more than one owned item with the same instance_key → delete extras
            if inst_key in owned:
                if gid:
                    rogue_ids.add(gid)  # keep the first we saw, purge this duplicate
                continue

            owned[inst_key] = {"id": gid, "hash": observed_hash}

        return owned, rogue_ids

    async def _collect_owned_not_in_desired(self, source_name: str, desired_keys: Set[str]) -> List[str]:
        """
        Delete criteria across entire calendar (active items only):
        - Owned item missing instance_key or hash (malformed) → delete
        - Owned item whose instance_key is not in current desired set → delete
        - Duplicate owned items with same instance_key (keep first; delete the rest)
        """
        items = await self.google.list_all_owned_events_raw(source_name)
        ids_to_delete: List[str] = []
        seen: Set[str] = set()

        for it in items:
            status = (it.get("status") or "").upper()
            if status == "CANCELLED":
                continue
            private = ((it.get("extendedProperties") or {}).get("private")) or {}
            if private.get("caldav-mirror-source") != source_name:
                continue

            gid = it.get("id")
            inst_key = private.get("caldav-mirror-instance-key")
            gh = private.get("caldav-mirror-hash")

            # Malformed: missing integrity markers
            if not inst_key or not gh:
                if gid:
                    ids_to_delete.append(gid)
                continue

            # Duplicate across calendar
            if inst_key in seen:
                if gid:
                    ids_to_delete.append(gid)
                continue
            seen.add(inst_key)

            # Not part of desired projection anymore
            if inst_key not in desired_keys:
                if gid:
                    ids_to_delete.append(gid)

        # Dedup IDs defensively
        return list(dict.fromkeys([i for i in ids_to_delete if i]))

    async def _collect_out_of_window_owned_ids(self, source_name: str, ws_dt: datetime, we_dt: datetime) -> List[str]:
        """
        When window shrinks, delete owned items strictly outside [ws, we].
        """
        items = await self.google.list_all_owned_events_raw(source_name)
        ids: List[str] = []
        for raw in items:
            gid = raw.get("id")
            if not gid:
                continue
            status = (raw.get("status") or "").upper()
            if status == "CANCELLED":
                continue
            private = ((raw.get("extendedProperties") or {}).get("private")) or {}
            if private.get("caldav-mirror-source") != source_name:
                continue

            inst_key = private.get("caldav-mirror-instance-key")
            # If legacy master (no instance-key) -> treat as rogue everywhere
            if not inst_key:
                ids.append(gid)
                continue

            # Determine effective start
            start = (raw.get("start") or {})
            s_dt_str = start.get("dateTime")
            s_date = start.get("date")
            if s_dt_str:
                try:
                    sdt = datetime.fromisoformat(s_dt_str.replace("Z", "+00:00"))
                except Exception:
                    sdt = None
                if sdt:
                    if sdt < ws_dt or sdt >= we_dt:
                        ids.append(gid)
            elif s_date:
                try:
                    base = _parse_yyyy_mm_dd(s_date)
                    sdt = datetime(base.year, base.month, base.day, tzinfo=timezone.utc)
                    if sdt < ws_dt or sdt >= we_dt:
                        ids.append(gid)
                except Exception:
                    # Malformed date -> garbage collect
                    ids.append(gid)
            else:
                # No start -> garbage collect
                ids.append(gid)

        return list(dict.fromkeys(ids))

    def _project_instances(
        self,
        series_by_uid: Dict[str, List[EventModel]],
        ws_dt: datetime,
        we_dt: datetime,
        source_name: str
    ) -> Dict[str, ProjectedEvent]:
        """
        Expand recurring series and include non-recurring instances that fall within the window.
        Returns map of instance_key -> ProjectedEvent
        """
        result: Dict[str, ProjectedEvent] = {}

        for uid, items in series_by_uid.items():
            # Separate master, exceptions, and singletons
            master: Optional[EventModel] = None
            exceptions: Dict[str, EventModel] = {}
            singles: List[EventModel] = []

            for ev in items:
                if ev.recurrence_id:
                    exceptions[ev.recurrence_id] = ev
                elif ev.rrule:
                    master = ev
                else:
                    singles.append(ev)

            # Handle single, non-recurring events
            for ev in singles:
                proj = self._project_single(ev, source_name)
                if not proj:
                    continue
                # Include only if within window by start
                if self._instance_in_window(proj, ws_dt, we_dt):
                    result[proj.instance_key()] = proj

            # Expand recurring series if master present
            if master and master.rrule:
                try:
                    self._project_recurring(master, exceptions, source_name, ws_dt, we_dt, result)
                except Exception as e:
                    logger.warning(f"[{source_name}] Failed to project series UID={uid}: {e}")

        return result

    def _project_single(self, ev: EventModel, source_name: str) -> Optional[ProjectedEvent]:
        # All-day case
        if ev.start_date:
            # Normalize end_date; Google expects exclusive end
            end_date = ev.end_date
            if not end_date:
                try:
                    sd = _parse_yyyy_mm_dd(ev.start_date)
                    end_date = (sd + timedelta(days=1)).strftime("%Y-%m-%d")
                except Exception:
                    end_date = ev.start_date
            return ProjectedEvent(
                uid=ev.uid,
                source_name=source_name,
                summary=ev.summary,
                description=ev.description,
                location=ev.location,
                is_all_day=True,
                start_date=ev.start_date,
                end_date=end_date,
                start_dt=None,
                end_dt=None,
                time_zone=None,
                transparency=ev.transparency,
            )
        # Timed
        if ev.start_datetime and ev.end_datetime:
            tzname = ev.timezone or "UTC"
            start_dt = ev.start_datetime
            end_dt = ev.end_datetime
            # Ensure aware
            tz = _safe_event_tz(tzname)
            start_dt = _to_aware_local(start_dt, tz).replace(microsecond=0)
            end_dt = _to_aware_local(end_dt, tz).replace(microsecond=0)
            return ProjectedEvent(
                uid=ev.uid,
                source_name=source_name,
                summary=ev.summary,
                description=ev.description,
                location=ev.location,
                is_all_day=False,
                start_date=None,
                end_date=None,
                start_dt=start_dt,
                end_dt=end_dt,
                time_zone=tzname,
                transparency=ev.transparency
            )
        return None

    def _instance_in_window(self, proj: ProjectedEvent, ws_dt: datetime, we_dt: datetime) -> bool:
        if proj.is_all_day:
            try:
                sdate = _parse_yyyy_mm_dd(proj.start_date)
                sdt = datetime(sdate.year, sdate.month, sdate.day, tzinfo=timezone.utc)
                return (sdt >= ws_dt) and (sdt < we_dt)
            except Exception:
                return False
        else:
            sdt_utc = proj.start_dt.astimezone(timezone.utc)
            return (sdt_utc >= ws_dt) and (sdt_utc < we_dt)

    def _project_recurring(
        self,
        master: EventModel,
        exceptions: Dict[str, EventModel],
        source_name: str,
        ws_dt: datetime,
        we_dt: datetime,
        out: Dict[str, ProjectedEvent]
    ) -> None:
        tz = _safe_event_tz(master.timezone)
        # Resolve master dtstart
        if master.start_datetime:
            dtstart_local = _to_aware_local(master.start_datetime, tz)
        elif master.start_date:
            sd = _parse_yyyy_mm_dd(master.start_date)
            dtstart_local = tz.localize(datetime(sd.year, sd.month, sd.day))
        else:
            return

        duration, is_all_day_master = _event_duration(master)
        if is_all_day_master and (duration is None or duration.days <= 0):
            duration = timedelta(days=1)

        # Build rruleset with EXDATEs
        rset = rrule.rruleset()
        try:
            # Diagnostics: capture RRULE and UNTIL before normalization
            until_token = _extract_until_token(master.rrule)
            try:
                logger.info(json.dumps({
                    "type": "RRULE_DIAG",
                    "source": source_name,
                    "uid": master.uid,
                    "timezone": getattr(tz, "zone", str(tz)),
                    "dtstart_local": dtstart_local.isoformat(),
                    "rrule_raw": master.rrule,
                    "until_raw": until_token
                }))
            except Exception:
                pass

            rrule_input = master.rrule
            # Normalize UNTIL to UTC Z when DTSTART is tz-aware
            if dtstart_local.tzinfo is not None:
                rrule_norm, until_raw2, until_norm2 = _normalize_rrule_until_utc(master.rrule, dtstart_local)
                rrule_input = rrule_norm
                try:
                    logger.info(json.dumps({
                        "type": "RRULE_NORMALIZE",
                        "source": source_name,
                        "uid": master.uid,
                        "until_raw": until_raw2,
                        "until_norm": until_norm2,
                        "changed": bool(until_norm2 and until_raw2 and until_norm2 != until_raw2)
                    }))
                except Exception:
                    pass

            r = rrule.rrulestr(rrule_input, dtstart=dtstart_local)
            rset.rrule(r)
        except Exception as e:
            logger.warning(f"Failed to parse RRULE for UID={master.uid}: {e}")
            return

        # Add EXDATEs from master
        if master.exdates:
            for ex in master.exdates:
                try:
                    ex_local = _to_aware_local(ex, tz)
                    rset.exdate(ex_local)
                except Exception:
                    continue

        # Compute window bounds in event local tz
        ws_local = ws_dt.astimezone(tz)
        we_local = we_dt.astimezone(tz)

        # Pre-parse exceptions by local RID time for matching
        exc_by_rid_local: Dict[str, Tuple[datetime, EventModel]] = {}
        for rid, ex in exceptions.items():
            rid_local = _rid_to_local_dt(master, rid)
            if rid_local is not None:
                exc_by_rid_local[rid] = (rid_local, ex)

        # Iterate occurrences
        occs = rset.between(ws_local, we_local, inc=True)
        for occ_local in occs:
            # Check exception by RID (original start time)
            matched_exception: Optional[EventModel] = None
            for rid, (rid_local, ex) in exc_by_rid_local.items():
                if rid_local == occ_local:
                    matched_exception = ex
                    break

            # If exception exists and is CANCELLED → skip
            if matched_exception and (matched_exception.status or "").upper() == "CANCELLED":
                continue

            if matched_exception:
                # Effective instance uses exception timing/data
                proj = self._project_single(matched_exception, source_name)
                if not proj:
                    continue
                # Only include if final start in window (exception may move outside)
                if self._instance_in_window(proj, ws_dt, we_dt):
                    out[proj.instance_key()] = proj
            else:
                # Generate from master defaults
                if is_all_day_master:
                    s_date = occ_local.date()
                    # end exclusive
                    e_date = s_date + duration
                    proj = ProjectedEvent(
                        uid=master.uid,
                        source_name=source_name,
                        summary=master.summary,
                        description=master.description,
                        location=master.location,
                        is_all_day=True,
                        start_date=s_date.strftime("%Y-%m-%d"),
                        end_date=e_date.strftime("%Y-%m-%d"),
                        start_dt=None,
                        end_dt=None,
                        time_zone=None,
                        transparency=master.transparency,
                    )
                else:
                    if not duration:
                        continue
                    start_dt = occ_local.replace(microsecond=0)
                    end_dt = (start_dt + duration).replace(microsecond=0)
                    proj = ProjectedEvent(
                        uid=master.uid,
                        source_name=source_name,
                        summary=master.summary,
                        description=master.description,
                        location=master.location,
                        is_all_day=False,
                        start_date=None,
                        end_date=None,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        time_zone=master.timezone or getattr(occ_local.tzinfo, "zone", "UTC"),
                        transparency=master.transparency,
                    )

                if self._instance_in_window(proj, ws_dt, we_dt):
                    out[proj.instance_key()] = proj