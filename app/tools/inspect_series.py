#!/usr/bin/env python3
"""
Inspect DB state for a source + recurring series.

Usage:
  docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/inspect_series.py "Source Name" CALDAV_UID
"""

import os
import sys
import json
import asyncio
from pathlib import Path
from typing import Optional

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Database  # [database.Database](app/database.py:20)
from sync.event_model import EventModel  # [sync.event_model.EventModel](app/sync/event_model.py:19)

async def inspect_series(source_name: str, caldav_uid: str) -> int:
    db_path = os.getenv("DATABASE_PATH", "/app/data/caldav_mirror.db")
    db = Database(db_path)
    await db.initialize()

    print(f"--- Inspecting DB series ---")
    print(f"Source: {source_name}")
    print(f"UID:    {caldav_uid}")
    print(f"DB:     {db_path}")
    print()

    rows = await db.get_all_event_instances_for_uid(source_name, caldav_uid)
    if not rows:
        print("No rows found for this UID in DB.")
        return 1

    print(f"Found {len(rows)} instance(s) in DB for this UID.")
    print()

    # Sort for stable output: master first (recurrence_id None), then by recurrence_id
    def _key(r):
        rid = r.get("event_data", {}).get("recurrence_id")
        return (0 if rid is None else 1, str(rid))

    for i, row in enumerate(sorted(rows, key=_key), start=1):
        event_data = row["event_data"]
        stored_hash = row.get("event_hash")
        google_event_id = row.get("google_event_id")
        recurrence_id = event_data.get("recurrence_id")

        # Re-hydrate to model and re-compute hash using current normalization
        model = EventModel.from_dict(event_data)
        computed_hash = model.compute_hash()
        norm = model.normalized_for_hash()

        print(f"Instance {i}:")
        print(f"  recurrence_id:           {recurrence_id!r}")
        print(f"  is_master_event:         {bool(event_data.get('is_master_event'))}")
        print(f"  google_event_id:         {google_event_id}")
        print(f"  google_recurring_event_id: {event_data.get('google_recurring_event_id')}")
        print(f"  summary:                 {event_data.get('summary')!r}")
        print(f"  start_datetime:          {event_data.get('start_datetime')}")
        print(f"  end_datetime:            {event_data.get('end_datetime')}")
        print(f"  start_date:              {event_data.get('start_date')}")
        print(f"  end_date:                {event_data.get('end_date')}")
        print(f"  timezone:                {event_data.get('timezone')}")
        print(f"  rrule:                   {event_data.get('rrule')}")
        print(f"  exdates_count:           {len(event_data.get('exdates') or [])}")
        print(f"  stored_hash:             {stored_hash}")
        print(f"  computed_hash:           {computed_hash}")
        print(f"  hash_match:              {stored_hash == computed_hash}")
        print(f"  normalized_for_hash:")
        print(json.dumps(norm, indent=2, sort_keys=True, default=str))
        print()

    # Simple summary for masters vs exceptions
    master_count = sum(1 for r in rows if r.get("event_data", {}).get("is_master_event"))
    ex_count = len(rows) - master_count
    print(f"Summary: masters={master_count}, exceptions={ex_count}")

    return 0

def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python3 tools/inspect_series.py \"Source Name\" CALDAV_UID", file=sys.stderr)
        return 2
    source = sys.argv[1]
    uid = sys.argv[2]
    return asyncio.run(inspect_series(source, uid))

if __name__ == "__main__":
    raise SystemExit(main())