#!/usr/bin/env python3
"""
Purge/Disown utility for a single Source + CalDAV UID

Usage examples:
  - List artifacts only:
    docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/purge_uid.py "Source Name" CALDAV_UID

  - Disown then delete + purge tombstones:
    docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/purge_uid.py "Source Name" CALDAV_UID --purge
"""

import asyncio
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Tuple

# Add app/ to import path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from database import Database  # [database.Database](app/database.py:20)
from auth.google_oauth import GoogleOAuth  # [auth.google_oauth.GoogleOAuth](app/auth/google_oauth.py:21)
from sync.google_client import GoogleClient  # [sync.google_client.GoogleClient](app/sync/google_client.py:21)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("tools.purge_uid")


async def list_items(google: GoogleClient, source: str, uid: str) -> list:
    """List Google artifacts (active + cancelled) for a specific source + UID."""
    items = await google.list_events_for_uid(source, uid)
    print(f"\n--- Artifacts for source='{source}' UID='{uid}' ---")
    print(f"Count: {len(items)}\n")
    for i, item in enumerate(items, start=1):
        status = (item.get("status") or "").upper()
        ext = item.get("extendedProperties", {})
        private = (ext or {}).get("private") or {}
        print(f"{i:02d}) id={item.get('id')}")
        print(f"    status={status}")
        print(f"    summary={item.get('summary')!r}")
        print(f"    recurringEventId={item.get('recurringEventId')!r}")
        print(f"    originalStartTime={item.get('originalStartTime')!r}")
        print(f"    privateExtendedProperties={private}")
        print()
    return items


async def disown_active(google: GoogleClient, items: list) -> int:
    """Clear CalDAV Mirror private extendedProperties from active items."""
    count = 0
    for item in items:
        gid = item.get("id")
        if not gid:
            continue
        status = (item.get("status") or "").upper()
        if status == "CANCELLED":
            continue
        ok = await google.disown_event(gid)
        if ok:
            count += 1
    return count


async def delete_active(google: GoogleClient, items: list) -> int:
    """Delete active items (masters or non-recurring) for the UID."""
    count = 0
    for item in items:
        gid = item.get("id")
        if not gid:
            continue
        status = (item.get("status") or "").upper()
        if status == "CANCELLED":
            continue
        ok = await google.delete_event(gid)
        if ok:
            count += 1
    return count


async def purge_cancelled(google: GoogleClient, items: list) -> int:
    """Permanently purge CANCELLED tombstones for the UID."""
    count = 0
    for item in items:
        gid = item.get("id")
        if not gid:
            continue
        status = (item.get("status") or "").upper()
        if status != "CANCELLED":
            continue
        ok = await google.purge_tombstone(gid)
        if ok:
            count += 1
    return count


async def run(source: str, uid: str, purge: bool) -> int:
    # Load env and bootstrap clients
    load_dotenv()
    cfg = {
        "google_client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "google_client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "encryption_key": os.getenv("ENCRYPTION_KEY"),
        "google_calendar_id": os.getenv("GOOGLE_CALENDAR_ID", "primary"),
        "database_path": os.getenv("DATABASE_PATH", "/app/data/caldav_mirror.db"),
    }

    missing = [k for k in ("google_client_id", "google_client_secret", "encryption_key") if not cfg[k]]
    if missing:
        log.error(f"Missing required env vars in .env: {missing}")
        return 2

    db = Database(db_path=cfg["database_path"])
    await db.initialize()

    oauth = GoogleOAuth(
        client_id=cfg["google_client_id"],
        client_secret=cfg["google_client_secret"],
        encryption_key=cfg["encryption_key"],
        database=db,
    )
    google = GoogleClient(oauth_handler=oauth, calendar_id=cfg["google_calendar_id"])

    # 1) List current state
    items = await list_items(google, source, uid)
    if not purge:
        return 0

    # 2) Disown active items so any trash tombstones won't match our filters
    disowned = await disown_active(google, items)
    log.info(f"Disowned {disowned} active item(s)")

    # 3) Delete active items (masters/non-recurring/exceptions)
    deleted = await delete_active(google, items)
    log.info(f"Deleted {deleted} active item(s)")

    # 4) Re-list to catch new CANCELLED tombstones that Google just created from deletion
    items_after_delete = await google.list_events_for_uid(source, uid)
    # 5) Purge all CANCELLED artifacts
    purged = await purge_cancelled(google, items_after_delete)
    log.info(f"Purged {purged} CANCELLED tombstone(s)")

    # 6) Final state check
    final_items = await google.list_events_for_uid(source, uid)
    print("\n--- Final state after purge ---")
    print(f"Count: {len(final_items)}")
    if final_items:
        # Show anything left as a diagnostic
        await list_items(google, source, uid)

    print("\nCompleted. If any items remain, verify that they lack CalDAV Mirror privateExtendedProperties.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="List or purge all Google artifacts for a Source + CalDAV UID")
    parser.add_argument("source_name", help="The 'name' of the source from sources.yml (e.g., iCloud Calendar)")
    parser.add_argument("caldav_uid", help="The CalDAV UID of the event series")
    parser.add_argument("--purge", action="store_true", help="Disown active items, delete them, then purge tombstones")

    args = parser.parse_args()
    return asyncio.run(run(args.source_name, args.caldav_uid, args.purge))


if __name__ == "__main__":
    raise SystemExit(main())