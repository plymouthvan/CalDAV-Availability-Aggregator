#!/usr/bin/env python3
"""
CalDAV Source Tester

This script performs a dry run of the synchronization process for a specific
source to help diagnose configuration or connectivity issues.
"""

import asyncio
import argparse
import logging
import yaml
from pathlib import Path
import sys

# Add app directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from sync.caldav_client import CalDAVClient
from sync.event_model import EventModel

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_source(source_config: dict):
    """
    Tests a CalDAV source by fetching events and validating them.
    """
    name = source_config.get("name", "Unnamed Source")
    logging.info(f"--- Testing Source: {name} ---")

    try:
        client = CalDAVClient(**source_config)

        # 1. Test Connection
        logging.info("Step 1: Testing connection...")
        if not await client.test_connection():
            logging.error("Connection test failed. Please check URL, username, and password.")
            return
        logging.info("✅ Connection successful.")

        # 2. Fetch Events (Dry Run)
        logging.info("Step 2: Fetching events (dry run)...")
        events, deleted_uids, sync_state = await client.sync_events()
        logging.info(f"✅ Fetched {len(events)} new/updated events and {len(deleted_uids)} deletions.")

        # 3. Validate Events
        if events:
            logging.info("Step 3: Validating first 5 events...")
            for i, event in enumerate(events[:5]):
                logging.info(f"  - Event {i+1}:")
                logging.info(f"    UID: {event.uid}")
                logging.info(f"    Summary: {event.summary}")
                logging.info(f"    Start: {event.start_datetime or event.start_date}")
                logging.info(f"    End: {event.end_datetime or event.end_date}")
                if event.rrule:
                    logging.info(f"    Recurrence: {event.rrule}")
            logging.info("✅ Events appear to be parsed correctly.")

        logging.info(f"\n--- Test for '{name}' completed successfully! ---")

    except Exception as e:
        logging.error(f"An unexpected error occurred during testing: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="Test a CalDAV source from your sources.yml file."
    )
    parser.add_argument("source_name", help="The name of the source to test.")
    
    args = parser.parse_args()

    # Load sources.yml
    sources_path = Path("sources.yml")
    if not sources_path.exists():
        logging.error("sources.yml not found.")
        return

    with open(sources_path, 'r') as f:
        sources_data = yaml.safe_load(f)
        if not sources_data:
            logging.error("sources.yml is empty or malformed.")
            return

    # Find the selected source
    source_to_test = None
    for source in sources_data:
        if source.get("name") == args.source_name:
            source_to_test = source
            break

    if not source_to_test:
        logging.error(f"Source '{args.source_name}' not found in sources.yml.")
        return

    asyncio.run(test_source(source_to_test))


if __name__ == "__main__":
    main()