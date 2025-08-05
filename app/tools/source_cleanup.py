#!/usr/bin/env python3
"""
CalDAV Source Cleanup Tool

This script removes all data associated with one or more CalDAV sources
from the local database and the target Google Calendar.
"""

import asyncio
import argparse
import logging
import yaml
from pathlib import Path
import sys
from dotenv import load_dotenv
import os

# Add app directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Database
from auth.google_oauth import GoogleOAuth
from sync.google_client import GoogleClient

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def cleanup_source(source_name: str, db: Database, google_client: GoogleClient):
    """
    Removes all data for a specific source.
    """
    logging.info(f"--- Cleaning up source: {source_name} ---")

    # 1. Get all events for the source from the local database
    events = await db.get_events_by_source(source_name)
    if not events:
        logging.info("No events found in the database for this source. Nothing to do.")
        return

    logging.info(f"Found {len(events)} events to remove from Google Calendar.")

    # 2. Delete events from Google Calendar
    # This could be batched for efficiency, but we'll do it individually for safety and clarity.
    for event in events:
        google_event_id = event.get("google_event_id")
        if google_event_id:
            logging.info(f"Deleting Google event {google_event_id}...")
            await google_client.delete_event(google_event_id)

    # 3. Delete all data for the source from the local database
    logging.info("Deleting all data for this source from the local database...")
    async with aiosqlite.connect(db.db_path) as conn:
        await conn.execute("DELETE FROM events WHERE source_name = ?", (source_name,))
        await conn.execute("DELETE FROM sync_state WHERE source_name = ?", (source_name,))
        await conn.commit()

    logging.info(f"--- Cleanup for '{source_name}' completed successfully! ---")


def main():
    parser = argparse.ArgumentParser(
        description="Clean up all data for a CalDAV source."
    )
    parser.add_argument("source_names", nargs='+', help="The name(s) of the source(s) to clean up from sources.yml.")
    
    args = parser.parse_args()

    # Load .env to get Google credentials
    load_dotenv()
    config = {
        "google_client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "google_client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "encryption_key": os.getenv("ENCRYPTION_KEY"),
        "google_calendar_id": os.getenv("GOOGLE_CALENDAR_ID", "primary"),
        "database_path": os.getenv("DATABASE_PATH", "/app/data/caldav_mirror.db")
    }

    if not all([config["google_client_id"], config["google_client_secret"], config["encryption_key"]]):
        logging.error("Missing required Google credentials in .env file.")
        return

    async def run_cleanup():
        db = Database(db_path=config["database_path"])
        await db.initialize()

        oauth_handler = GoogleOAuth(
            client_id=config["google_client_id"],
            client_secret=config["google_client_secret"],
            encryption_key=config["encryption_key"],
            database=db
        )
        
        google_client = GoogleClient(
            oauth_handler=oauth_handler,
            calendar_id=config["google_calendar_id"]
        )

        for source_name in args.source_names:
            await cleanup_source(source_name, db, google_client)

    asyncio.run(run_cleanup())


if __name__ == "__main__":
    main()