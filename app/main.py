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
        "sources": []
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
        if not all(k in source for k in ["name", "url", "username", "password", "sync_method"]):
            raise ValueError(f"Source '{source.get('name', 'Unknown')}' is missing required keys.")
        if source["sync_method"] not in ["sync-token", "ctag", "gtag"]:
            raise ValueError(f"Invalid sync_method for source '{source['name']}'.")

    return config

# --- Main Application ---

async def main():
    """Main entry point for the CalDAV Mirror service."""
    logger = setup_logger()
    logger.info("Starting CalDAV Mirror service...")

    try:
        # 1. Load Configuration
        config = load_configuration()
        logger.info(f"Loaded {len(config['sources'])} CalDAV source(s).")

        # 2. Initialize Database
        db = Database()
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

        # 4. Initialize CalDAV Clients
        caldav_clients = []
        for source_config in config["sources"]:
            client = CalDAVClient(database=db, **source_config)
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
            await asyncio.sleep(300)  # Sync every 5 minutes

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