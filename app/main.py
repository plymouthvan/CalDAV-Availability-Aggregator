#!/usr/bin/env python3
"""
CalDAV Mirror - Main Entry Point

A headless background service that aggregates events from one or more CalDAV 
calendars and mirrors them into a single Google Calendar.
"""

import asyncio
import logging
import sys
import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, List

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logger import setup_logger
from database import Database
from sync.caldav_client import CalDAVClient
from sync.google_client import GoogleClient
from auth.google_oauth import GoogleOAuth
from sync.sync_manager import SyncManager


# --- Configuration Loading ---
def load_configuration() -> Dict[str, Any]:
    """Load configuration from .env and sources.yml."""
    # Load .env file
    load_dotenv()

    config = {
        "google_client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "google_client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "encryption_key": os.getenv("ENCRYPTION_KEY"),
        "google_calendar_id": os.getenv("GOOGLE_CALENDAR_ID", "primary"),
        "sources": [],
        "database_path": os.getenv("DATABASE_PATH", "/app/data/caldav_mirror.db"),
        "sync_interval_seconds": int(os.getenv("SYNC_INTERVAL_SECONDS", 300))
    }

    # Validate required environment variables
    if not all([config["google_client_id"], config["google_client_secret"], config["encryption_key"]]):
        raise ValueError("Missing required environment variables in .env file.")

    # Load sources.yml
    sources_path = Path("sources.yml")
    if not sources_path.exists():
        raise FileNotFoundError("sources.yml not found.")

    with open(sources_path, 'r') as f:
        sources_data = yaml.safe_load(f)
        if not sources_data:
            raise ValueError("sources.yml is empty or malformed.")
        config["sources"] = sources_data

    # Validate sources configuration
    for source in config["sources"]:
        if not all(k in source for k in ["name", "url", "username", "password"]):
            raise ValueError(f"Source '{source.get('name', 'Unknown')}' is missing required keys.")

    return config

# --- Main Application ---

async def main():
    """Main entry point for the CalDAV Mirror service."""
    # Quiet default noise; enable targeted DEBUG where we need diagnostics
    logger = setup_logger(level="INFO")
    logger.info("Starting CalDAV Mirror service...")

    # Focus detailed diagnostics on reconciler + google client only
    logging.getLogger("sync.event_model").setLevel(logging.INFO)
    logging.getLogger("sync.reconciler").setLevel(logging.DEBUG)
    logging.getLogger("sync.google_client").setLevel(logging.DEBUG)

    # Silence noisy third-party libs
    logging.getLogger("aiosqlite").setLevel(logging.INFO)
    logging.getLogger("icalendar").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)

    try:
        # 1. Load Configuration
        config = load_configuration()
        logger.info(f"Loaded {len(config['sources'])} CalDAV source(s).")

        # 2. Initialize Database
        db = Database(db_path=config["database_path"])
        await db.initialize()

        # 3. Initialize Google OAuth and Client
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
        
        # Ensure Google authentication is working
        access_token = await oauth_handler.get_access_token()
        if not access_token:
            logger.error("Failed to authenticate with Google. Shutting down.")
            sys.exit(1)
        logger.info("Successfully authenticated with Google.")

        # 3.5 Enforce dedicated empty calendar on first run
        try:
            total_db_events = await db.count_events()
        except Exception:
            total_db_events = 0
        if total_db_events == 0:
            is_empty = await google_client.is_calendar_empty()
            if not is_empty:
                logger.error(
                    "The destination Google Calendar is not empty. This application requires a dedicated, empty "
                    "calendar that it exclusively manages. To proceed, either clear ALL events from the selected "
                    "calendar or provide a different, empty calendar ID in GOOGLE_CALENDAR_ID. Exiting without making changes."
                )
                sys.exit(2)
        # 4. Initialize CalDAV Clients
        caldav_clients = []
        for source_config in config["sources"]:
            provider = source_config.pop("provider", "generic")
            
            # Special handling for iCloud
            if provider == "icloud":
                source_config.pop("sync_method", None)

            client = CalDAVClient(provider, database=db, **source_config)
            
            if not await client.test_connection():
                logger.error(f"Connection test failed for source: {source_config['name']}. Skipping.")
                continue
                
            caldav_clients.append(client)
            logger.info(f"Successfully connected to CalDAV source: {source_config['name']}")

        if not caldav_clients:
            logger.error("No valid CalDAV sources found. Shutting down.")
            sys.exit(1)

        # 5. Initialize Sync Managers
        sync_managers = []
        for client in caldav_clients:
            sync_manager = SyncManager(
                caldav_client=client,
                google_client=google_client,
                database=db
            )
            sync_managers.append(sync_manager)

        # 6. Start Sync Loop
        logger.info("CalDAV Mirror service started successfully. Starting sync loop...")
        while True:
            logger.info("Starting sync cycle...")
            for manager in sync_managers:
                await manager.run_sync()
            
            logger.info("Sync cycle finished. Waiting for next run...")
            await asyncio.sleep(config["sync_interval_seconds"])

    except (ValueError, FileNotFoundError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping service...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())