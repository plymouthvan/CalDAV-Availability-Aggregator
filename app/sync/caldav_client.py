"""
CalDAV Client Factory
"""

from .providers.base import BaseCalDAVClient
from .providers.generic import GenericCalDAVClient
from .providers.icloud import iCloudCalDAVClient
from .providers.daylite import DayliteCalDAVClient

def CalDAVClient(provider: str, **kwargs) -> BaseCalDAVClient:
    """
    Factory function to create a CalDAV client for a specific provider.
    """
    if provider == "icloud":
        return iCloudCalDAVClient(**kwargs)
    elif provider == "daylite":
        return DayliteCalDAVClient(**kwargs)
    else:
        return GenericCalDAVClient(**kwargs)