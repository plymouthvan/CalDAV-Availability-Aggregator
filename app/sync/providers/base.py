"""
Base CalDAV client for CalDAV Mirror
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple, Optional

from ..event_model import EventModel

class BaseCalDAVClient(ABC):
    """Abstract base class for CalDAV clients."""

    def __init__(self, name: str, url: str, username: str, password: str, database=None):
        self.name = name
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.database = database

    @abstractmethod
    async def sync_events(self) -> Tuple[List[EventModel], List[str], Optional[Dict[str, Any]]]:
        """
        Sync events using the provider-specific method.
        
        Returns:
            Tuple of (new/updated events, deleted event UIDs, new_sync_state)
        """
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        """Test connection to the CalDAV server."""
        pass