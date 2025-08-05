"""
Clear all events from the database.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add app directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import Database

async def main():
    """Main function to clear events."""
    db_path = os.getenv("DATABASE_PATH", "/app/data/caldav_mirror.db")
    db = Database(db_path)
    
    print("This will delete all event data from the database.")
    print("Auth tokens and sync state will be preserved.")
    confirm = input("Are you sure you want to continue? (y/N): ")
    
    if confirm.lower() == 'y':
        await db.initialize()
        await db.clear_all_events()
        print("All events have been cleared from the database.")
    else:
        print("Operation cancelled.")

if __name__ == "__main__":
    asyncio.run(main())