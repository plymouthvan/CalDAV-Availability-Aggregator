"""
Database module for CalDAV Mirror

Handles SQLite persistence for event tracking, sync state, and authentication tokens.
"""

import sqlite3
import asyncio
import aiosqlite
import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class Database:
    """SQLite database manager for CalDAV Mirror."""
    
    def __init__(self, db_path: str = "data/caldav_mirror.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    async def initialize(self):
        """Initialize database schema."""
        async with aiosqlite.connect(self.db_path) as db:
            await self._create_tables(db)
            await db.commit()
        logger.info(f"Database initialized at {self.db_path}")
    
    async def _create_tables(self, db: aiosqlite.Connection):
        """Create all necessary tables."""
        
        # Events table - stores normalized event data
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                internal_id TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                caldav_uid TEXT NOT NULL,
                recurrence_id TEXT, -- For exceptions, this is the original start time of the instance
                google_event_id TEXT,
                google_recurring_event_id TEXT, -- For exceptions, the master event's Google ID
                event_hash TEXT NOT NULL,
                event_data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_synced TIMESTAMP,
                is_master_event BOOLEAN DEFAULT FALSE, -- Explicitly flag the master event
                UNIQUE(source_name, caldav_uid, recurrence_id)
            )
        """)
        
        # Sync state table - tracks sync tokens/ctags per source
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sync_state (
                source_name TEXT PRIMARY KEY,
                sync_method TEXT NOT NULL,
                sync_token TEXT,
                ctag TEXT,
                gtag TEXT,
                last_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Auth tokens table - encrypted Google OAuth tokens
        await db.execute("""
            CREATE TABLE IF NOT EXISTS auth_tokens (
                service TEXT PRIMARY KEY,
                encrypted_token TEXT NOT NULL,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_name)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_caldav_uid ON events(caldav_uid)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_google_id ON events(google_event_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_hash ON events(event_hash)")
    
    async def store_event(self, source_name: str, caldav_uid: str, recurrence_id: Optional[str],
                          event_data: Dict[str, Any], event_hash: str, is_master_event: bool,
                          google_event_id: Optional[str] = None, google_recurring_event_id: Optional[str] = None) -> str:
        """Store or update an event instance (master or exception) in the database."""
        internal_id = str(uuid.uuid4())

        async with aiosqlite.connect(self.db_path) as db:
            # Correctly handle NULL for recurrence_id in WHERE clauses
            where_clause = "source_name = ? AND caldav_uid = ? AND "
            where_clause += "recurrence_id = ?" if recurrence_id is not None else "recurrence_id IS NULL"
            
            where_params = (source_name, caldav_uid, recurrence_id) if recurrence_id is not None else (source_name, caldav_uid)

            # Try to update existing event first
            update_params = (event_hash, json.dumps(event_data, default=str), google_event_id, google_recurring_event_id, is_master_event, *where_params)
            await db.execute(f"""
                UPDATE events
                SET event_hash = ?, event_data = ?, google_event_id = ?, google_recurring_event_id = ?, is_master_event = ?,
                    updated_at = CURRENT_TIMESTAMP, last_synced = CURRENT_TIMESTAMP
                WHERE {where_clause}
            """, update_params)

            if db.total_changes == 0:
                # Insert new event
                insert_params = (internal_id, source_name, caldav_uid, recurrence_id, event_hash,
                                 json.dumps(event_data, default=str), google_event_id, google_recurring_event_id, is_master_event)
                await db.execute("""
                    INSERT INTO events (internal_id, source_name, caldav_uid, recurrence_id, event_hash,
                                      event_data, google_event_id, google_recurring_event_id, is_master_event, last_synced)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, insert_params)
            else:
                # Get the existing internal_id
                cursor = await db.execute(f"SELECT internal_id FROM events WHERE {where_clause}", where_params)
                row = await cursor.fetchone()
                if row:
                    internal_id = row[0]

            await db.commit()

        return internal_id
    
    async def get_event_instance(self, source_name: str, caldav_uid: str, recurrence_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """Get a specific event instance by its CalDAV UID and recurrence ID."""
        async with aiosqlite.connect(self.db_path) as db:
            where_clause = "source_name = ? AND caldav_uid = ? AND "
            where_clause += "recurrence_id = ?" if recurrence_id is not None else "recurrence_id IS NULL"
            where_params = (source_name, caldav_uid, recurrence_id) if recurrence_id is not None else (source_name, caldav_uid)

            cursor = await db.execute(f"""
                SELECT internal_id, google_event_id, event_hash, event_data, last_synced, recurrence_id
                FROM events
                WHERE {where_clause}
            """, where_params)

            row = await cursor.fetchone()
            if row:
                return {
                    'internal_id': row[0],
                    'google_event_id': row[1],
                    'event_hash': row[2],
                    'event_data': json.loads(row[3]),
                    'last_synced': row[4],
                    'recurrence_id': row[5]
                }
        return None
    
    async def get_events_by_source(self, source_name: str) -> List[Dict[str, Any]]:
        """Get all events for a specific source."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT internal_id, caldav_uid, google_event_id, event_hash, 
                       event_data, last_synced
                FROM events 
                WHERE source_name = ?
                ORDER BY updated_at DESC
            """, (source_name,))
            
            events = []
            async for row in cursor:
                events.append({
                    'internal_id': row[0],
                    'caldav_uid': row[1],
                    'google_event_id': row[2],
                    'event_hash': row[3],
                    'event_data': json.loads(row[4]),
                    'last_synced': row[5]
                })
            
            return events

    async def get_all_events_for_source(self, source_name: str) -> Dict[Tuple[str, Optional[str]], Dict[str, Any]]:
        """
        Get all event instances for a specific source, keyed by (CalDAV UID, recurrence_id).

        Args:
            source_name: The name of the source.

        Returns:
            A dictionary mapping (caldav_uid, recurrence_id) tuples to event data.
        """
        events = {}
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT caldav_uid, recurrence_id, google_event_id, event_hash, event_data
                FROM events
                WHERE source_name = ?
            """, (source_name,))

            async for row in cursor:
                key = (row[0], row[1])
                events[key] = {
                    'google_event_id': row[2],
                    'event_hash': row[3],
                    'event_data': json.loads(row[4])
                }
        return events

    async def get_all_event_instances_for_uid(self, source_name: str, caldav_uid: str) -> List[Dict[str, Any]]:
        """
        Get all event instances (master and exceptions) for a specific CalDAV UID.
        """
        instances = []
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT caldav_uid, recurrence_id, google_event_id, event_hash, event_data
                FROM events
                WHERE source_name = ? AND caldav_uid = ?
            """, (source_name, caldav_uid))

            async for row in cursor:
                instances.append({
                    'caldav_uid': row[0],
                    'recurrence_id': row[1],
                    'google_event_id': row[2],
                    'event_hash': row[3],
                    'event_data': json.loads(row[4])
                })
        return instances

    async def bulk_update_google_ids(self, source_name: str, instance_to_google_id_map: Dict[Tuple[str, Optional[str]], str]):
        """
        Bulk update the google_event_id for a set of event instances.

        Args:
            source_name: The name of the source.
            instance_to_google_id_map: A dictionary mapping (CalDAV UID, recurrence_id) to Google event ID.
        """
        if not instance_to_google_id_map:
            return

        master_updates = []
        exception_updates = []

        for (caldav_uid, recurrence_id), google_id in instance_to_google_id_map.items():
            if recurrence_id is None:
                master_updates.append((google_id, source_name, caldav_uid))
            else:
                exception_updates.append((google_id, source_name, caldav_uid, recurrence_id))

        async with aiosqlite.connect(self.db_path) as db:
            if master_updates:
                await db.executemany("""
                    UPDATE events
                    SET google_event_id = ?
                    WHERE source_name = ? AND caldav_uid = ? AND recurrence_id IS NULL
                """, master_updates)

            if exception_updates:
                await db.executemany("""
                    UPDATE events
                    SET google_event_id = ?
                    WHERE source_name = ? AND caldav_uid = ? AND recurrence_id = ?
                """, exception_updates)

            await db.commit()
            logger.info(f"Bulk updated {db.total_changes} Google event IDs for source {source_name}.")
    
    async def delete_event(self, source_name: str, caldav_uid: str, recurrence_id: Optional[str]) -> Optional[str]:
        """Delete an event instance and return its Google event ID if it exists."""
        async with aiosqlite.connect(self.db_path) as db:
            where_clause = "source_name = ? AND caldav_uid = ? AND "
            where_clause += "recurrence_id = ?" if recurrence_id is not None else "recurrence_id IS NULL"
            where_params = (source_name, caldav_uid, recurrence_id) if recurrence_id is not None else (source_name, caldav_uid)

            # Get Google event ID before deletion
            cursor = await db.execute(f"""
                SELECT google_event_id FROM events
                WHERE {where_clause}
            """, where_params)
    async def delete_event_series(self, source_name: str, caldav_uid: str):
        """Delete all event instances for a given series UID."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                DELETE FROM events
                WHERE source_name = ? AND caldav_uid = ?
            """, (source_name, caldav_uid))
            await db.commit()
            if db.total_changes > 0:
                logger.info(f"Deleted {db.total_changes} instances for event series {caldav_uid} from source {source_name}.")
    
    async def clear_all_events(self):
        """Delete all events from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM events")
            await db.commit()
            logger.info("All events have been cleared from the database.")
    
    async def update_sync_state(self, source_name: str, sync_method: str,
                                 sync_token: Optional[str] = None,
                                 ctag: Optional[str] = None,
                               gtag: Optional[str] = None):
        """Update sync state for a source."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO sync_state 
                (source_name, sync_method, sync_token, ctag, gtag, last_sync)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (source_name, sync_method, sync_token, ctag, gtag))
            
            await db.commit()
    
    async def get_sync_state(self, source_name: str) -> Optional[Dict[str, Any]]:
        """Get sync state for a source."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT sync_method, sync_token, ctag, gtag, last_sync
                FROM sync_state 
                WHERE source_name = ?
            """, (source_name,))
            
            row = await cursor.fetchone()
            if row:
                return {
                    'sync_method': row[0],
                    'sync_token': row[1],
                    'ctag': row[2],
                    'gtag': row[3],
                    'last_sync': row[4]
                }
        return None
    
    async def store_auth_token(self, service: str, encrypted_token: str, 
                              expires_at: Optional[datetime] = None):
        """Store encrypted authentication token."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO auth_tokens 
                (service, encrypted_token, expires_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (service, encrypted_token, expires_at))
            
            await db.commit()
    
    async def get_auth_token(self, service: str) -> Optional[str]:
        """Get encrypted authentication token."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT encrypted_token FROM auth_tokens 
                WHERE service = ?
            """, (service,))
            
            row = await cursor.fetchone()
            return row[0] if row else None

    async def count_events(self) -> int:
        """Return the total number of event instances stored across all sources."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM events")
            row = await cursor.fetchone()
            return int(row[0]) if row and row[0] is not None else 0