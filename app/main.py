#!/usr/bin/env python3
"""
CalDAV Mirror - Main Entry Point

A headless background service that aggregates events from one or more CalDAV 
calendars and mirrors them into a single Google Calendar.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from utils.logger import setup_logger
from database import Database
from sync.caldav_client import CalDAVClient
from sync.google_client import GoogleClient
from auth.google_oauth import GoogleOAuth


async def main():
    """Main entry point for the CalDAV Mirror service."""
    logger = setup_logger()
    logger.info("Starting CalDAV Mirror service...")
    
    try:
        # Initialize database
        db = Database()
        await db.initialize()
        
        # TODO: Load sources.yml configuration
        # TODO: Initialize CalDAV clients
        # TODO: Initialize Google client
        # TODO: Start sync loop
        
        logger.info("CalDAV Mirror service started successfully")
        
        # Keep the service running
        while True:
            await asyncio.sleep(60)  # Sync every minute for now
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, stopping service...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())