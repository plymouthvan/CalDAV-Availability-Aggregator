"""
Event model for CalDAV Mirror

Provides normalized event representation and hashing for deduplication.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class EventModel:
    """Normalized event representation."""
    
    # Core identifiers
    uid: str                    # CalDAV UID
    summary: str               # Event title
    description: Optional[str] = None
    
    # Time information
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    start_date: Optional[str] = None  # For all-day events (YYYY-MM-DD)
    end_date: Optional[str] = None    # For all-day events (YYYY-MM-DD)
    timezone: Optional[str] = None
    
    # Location and organizer
    location: Optional[str] = None
    organizer: Optional[str] = None
    organizer_email: Optional[str] = None
    
    # Event properties
    status: str = "CONFIRMED"  # CONFIRMED, TENTATIVE, CANCELLED
    transparency: str = "OPAQUE"  # OPAQUE, TRANSPARENT
    
    # Recurrence
    rrule: Optional[str] = None
    recurrence_id: Optional[str] = None
    is_master_event: bool = False
    google_recurring_event_id: Optional[str] = None # For exceptions, the master event's Google ID
    google_event_id: Optional[str] = None # The event's own Google ID

    # Attendees
    attendees: List[Dict[str, str]] = None
    
    # Metadata
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    sequence: int = 0
    source_name: Optional[str] = None
    
    # Categories and classification
    categories: List[str] = None
    classification: str = "PUBLIC"  # PUBLIC, PRIVATE, CONFIDENTIAL
    
    def __post_init__(self):
        """Initialize default values for mutable fields."""
        if self.attendees is None:
            self.attendees = []
        else:
            self.attendees = sorted(self.attendees, key=lambda x: x['email'])
            
        if self.categories is None:
            self.categories = []
        else:
            self.categories = sorted(self.categories)
    
    @classmethod
    def from_icalendar(cls, ical_event, source_name: str) -> 'EventModel':
        """
        Create EventModel from an icalendar Event component.
        
        Args:
            ical_event: icalendar Event component
            source_name: The name of the source this event belongs to.
            
        Returns:
            EventModel instance
        """
        logger.debug(f"--- Parsing iCalendar Event from {source_name} ---")
        logger.debug(f"iCal Raw UID: {ical_event.get('UID')}")
        logger.debug(f"iCal Raw SUMMARY: {ical_event.get('SUMMARY')}")
        logger.debug(f"iCal Raw DTSTART: {ical_event.get('DTSTART').dt if ical_event.get('DTSTART') else 'N/A'}")
        logger.debug(f"iCal Raw RRULE: {ical_event.get('RRULE')}")
        logger.debug(f"iCal Raw RECURRENCE-ID: {ical_event.get('RECURRENCE-ID')}")
        
        try:
            # Extract basic properties
            uid = str(ical_event.get('UID', ''))
            summary = str(ical_event.get('SUMMARY', ''))
            description = str(ical_event.get('DESCRIPTION', '')) if ical_event.get('DESCRIPTION') else None
            
            # Handle datetime/date fields
            start_datetime = None
            end_datetime = None
            start_date = None
            end_date = None
            event_timezone = None
            
            dtstart = ical_event.get('DTSTART')
            dtend = ical_event.get('DTEND')
            
            if dtstart:
                if hasattr(dtstart.dt, 'date'):
                    # It's a datetime
                    start_datetime = dtstart.dt
                    if hasattr(dtstart.dt, 'tzinfo') and dtstart.dt.tzinfo:
                        event_timezone = str(dtstart.dt.tzinfo)
                else:
                    # It's a date (all-day event)
                    start_date = dtstart.dt.strftime('%Y-%m-%d')
            
            if dtend:
                if hasattr(dtend.dt, 'date'):
                    # It's a datetime
                    end_datetime = dtend.dt
                else:
                    # It's a date (all-day event)
                    end_date = dtend.dt.strftime('%Y-%m-%d')
            
            # Location and organizer
            location = str(ical_event.get('LOCATION', '')) if ical_event.get('LOCATION') else None
            
            organizer = None
            organizer_email = None
            if ical_event.get('ORGANIZER'):
                organizer_prop = ical_event.get('ORGANIZER')
                organizer_email = str(organizer_prop).replace('mailto:', '') if str(organizer_prop).startswith('mailto:') else str(organizer_prop)
                organizer = organizer_prop.params.get('CN', organizer_email) if hasattr(organizer_prop, 'params') else organizer_email
            
            # Status and transparency
            status = str(ical_event.get('STATUS', 'CONFIRMED')).upper()
            transparency = str(ical_event.get('TRANSP', 'OPAQUE')).upper()
            
            # Recurrence
            rrule = ical_event.get('RRULE').to_ical().decode('utf-8') if ical_event.get('RRULE') else None
            
            recurrence_id_val = ical_event.get('RECURRENCE-ID')
            recurrence_id = None
            if recurrence_id_val:
                dt = recurrence_id_val.dt
                if isinstance(dt, datetime):
                    if dt.tzinfo:
                        recurrence_id = dt.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
                    else:
                        recurrence_id = dt.strftime('%Y%m%dT%H%M%S') # Naive, assume UTC
                else: # date object
                    recurrence_id = dt.strftime('%Y%m%d')

            logger.debug(f"[CalDAV PARSE] Raw RECURRENCE-ID: {recurrence_id_val.dt if recurrence_id_val else 'None'} -> Parsed recurrence_id: {recurrence_id}")
            is_master_event = rrule is not None

            # Attendees
            attendees = []
            for attendee in ical_event.get('ATTENDEE', []):
                if not isinstance(attendee, list):
                    attendee = [attendee]
                
                for att in attendee:
                    att_email = str(att).replace('mailto:', '') if str(att).startswith('mailto:') else str(att)
                    att_name = att.params.get('CN', att_email) if hasattr(att, 'params') else att_email
                    att_status = att.params.get('PARTSTAT', 'NEEDS-ACTION') if hasattr(att, 'params') else 'NEEDS-ACTION'
                    
                    attendees.append({
                        'email': att_email,
                        'name': att_name,
                        'status': att_status
                    })
            
            # Metadata
            created = ical_event.get('CREATED').dt if ical_event.get('CREATED') else None
            last_modified = ical_event.get('LAST-MODIFIED').dt if ical_event.get('LAST-MODIFIED') else None
            sequence = int(ical_event.get('SEQUENCE', 0))
            
            # Categories
            categories = []
            if ical_event.get('CATEGORIES'):
                cats = ical_event.get('CATEGORIES')
                if isinstance(cats, list):
                    categories = [str(cat) for cat in cats]
                else:
                    categories = [str(cats)]
            
            classification = str(ical_event.get('CLASS', 'PUBLIC')).upper()
            
            return cls(
                uid=uid,
                summary=summary,
                description=description,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                start_date=start_date,
                end_date=end_date,
                timezone=event_timezone,
                location=location,
                organizer=organizer,
                organizer_email=organizer_email,
                status=status,
                transparency=transparency,
                rrule=rrule,
                recurrence_id=recurrence_id,
                is_master_event=is_master_event,
                attendees=attendees,
                created=created,
                last_modified=last_modified,
                sequence=sequence,
                categories=categories,
                classification=classification,
                source_name=source_name
            )
            
        except Exception as e:
            logger.error(f"Failed to parse iCalendar event: {e}")
            # Return minimal event to avoid complete failure
            return cls(
                uid=str(ical_event.get('UID', 'unknown')),
                summary=str(ical_event.get('SUMMARY', 'Untitled Event')),
                source_name=source_name
            )
    
    def to_google_event(self) -> Dict[str, Any]:
        """
        Convert to Google Calendar API event format.
        
        Returns:
            Dictionary in Google Calendar API format
        """
        google_event = {
            'summary': self.summary,
            'status': self._map_status_to_google(),
            'transparency': self._map_transparency_to_google(),
        }
        
        # Description
        if self.description:
            google_event['description'] = self.description
        
        # Location
        if self.location:
            google_event['location'] = self.location
        
        # Organizer
        if self.organizer_email:
            google_event['organizer'] = {
                'email': self.organizer_email,
                'displayName': self.organizer or self.organizer_email
            }
        
        # Time handling
        if self.start_date and self.end_date:
            # All-day event
            google_event['start'] = {'date': self.start_date}
            google_event['end'] = {'date': self.end_date}
        elif self.start_datetime and self.end_datetime:
            # Timed event
            google_event['start'] = {
                'dateTime': self.start_datetime.isoformat(),
                'timeZone': self.timezone or 'UTC'
            }
            google_event['end'] = {
                'dateTime': self.end_datetime.isoformat(),
                'timeZone': self.timezone or 'UTC'
            }
        
        # Recurrence
        if self.google_recurring_event_id:
            # This is an exception, link it to the master event in Google
            google_event['recurringEventId'] = self.google_recurring_event_id
        elif self.rrule:
            # This is a master event with a recurrence rule
            google_event['recurrence'] = [f'RRULE:{self.rrule}']

        # Attendees
        if self.attendees:
            google_event['attendees'] = [
                {
                    'email': att['email'],
                    'displayName': att['name'],
                    'responseStatus': self._map_attendee_status_to_google(att['status'])
                }
                for att in self.attendees
            ]
        
        # Visibility
        if self.classification == 'PRIVATE':
            google_event['visibility'] = 'private'
        elif self.classification == 'CONFIDENTIAL':
            google_event['visibility'] = 'confidential'
        else:
            google_event['visibility'] = 'public'

        # Add extended properties for reconciliation
        if self.source_name:
            google_event['extendedProperties'] = {
                'private': {
                    'caldav-mirror-source': self.source_name,
                    'caldav-mirror-uid': self.uid,
                    'caldav-mirror-hash': self.compute_hash()
                }
            }
        
        logger.debug(f"[Google PUSH] UID={self.uid}, RecurrenceID={self.recurrence_id} → Payload RecurringID={google_event.get('recurringEventId')}, OriginalStartTime={google_event.get('originalStartTime')}")
        return google_event
    
    def _map_status_to_google(self) -> str:
        """Map CalDAV status to Google Calendar status."""
        status_map = {
            'CONFIRMED': 'confirmed',
            'TENTATIVE': 'tentative',
            'CANCELLED': 'cancelled'
        }
        return status_map.get(self.status, 'confirmed')
    
    def _map_transparency_to_google(self) -> str:
        """Map CalDAV transparency to Google Calendar transparency."""
        return 'transparent' if self.transparency == 'TRANSPARENT' else 'opaque'
    
    def _map_attendee_status_to_google(self, caldav_status: str) -> str:
        """Map CalDAV attendee status to Google Calendar status."""
        status_map = {
            'ACCEPTED': 'accepted',
            'DECLINED': 'declined',
            'TENTATIVE': 'tentative',
            'NEEDS-ACTION': 'needsAction'
        }
        return status_map.get(caldav_status, 'needsAction')
    
    def compute_hash(self) -> str:
        """
        Compute a hash of the event for deduplication.
        
        Returns:
            SHA-256 hash of normalized event data
        """
        # Create a normalized representation for hashing
        hash_data = {
            'uid': self.uid,
            'summary': self.summary,
            'description': self.description,
            'start_datetime': self.start_datetime.isoformat() if self.start_datetime else None,
            'end_datetime': self.end_datetime.isoformat() if self.end_datetime else None,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'timezone': self.timezone,
            'location': self.location,

            'rrule': self.rrule,
            'recurrence_id': self.recurrence_id,
            'is_master_event': self.is_master_event,
            'google_recurring_event_id': self.google_recurring_event_id,
            'categories': sorted(self.categories) if self.categories else [],
            'classification': self.classification,
            'source_name': self.source_name
        }
        
        # Convert to JSON string with sorted keys for consistent hashing
        json_str = json.dumps(hash_data, sort_keys=True, default=str)
        
        # Return SHA-256 hash
        return hashlib.sha256(json_str.encode('utf-8')).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventModel':
        """Create EventModel from dictionary."""
        # Handle datetime fields
        if data.get('start_datetime') and isinstance(data['start_datetime'], str):
            data['start_datetime'] = datetime.fromisoformat(data['start_datetime'])
        if data.get('end_datetime') and isinstance(data['end_datetime'], str):
            data['end_datetime'] = datetime.fromisoformat(data['end_datetime'])
        if data.get('created') and isinstance(data['created'], str):
            data['created'] = datetime.fromisoformat(data['created'])
        if data.get('last_modified') and isinstance(data['last_modified'], str):
            data['last_modified'] = datetime.fromisoformat(data['last_modified'])
        
        return cls(**data)

    @classmethod
    def from_google_event(cls, google_event: Dict[str, Any]) -> 'EventModel':
        """
        Create EventModel from a Google Calendar API event dictionary.
        
        Args:
            google_event: Dictionary from Google Calendar API
            
        Returns:
            EventModel instance
        """
        logger.debug("--- Parsing Google Calendar Event ---")
        logger.debug(f"Google Raw Event ID: {google_event.get('id')}")
        logger.debug(f"Google Raw Summary: {google_event.get('summary')}")
        logger.debug(f"Google Raw Start: {google_event.get('start')}")
        logger.debug(f"Google Raw Recurrence: {google_event.get('recurrence')}")
        logger.debug(f"Google Raw recurringEventId: {google_event.get('recurringEventId')}")
        logger.debug(f"Google Raw originalStartTime: {google_event.get('originalStartTime')}")
        
        private_props = google_event.get('extendedProperties', {}).get('private', {})
        
        # Extract start and end times
        start = google_event.get('start', {})
        end = google_event.get('end', {})
        
        start_datetime = start.get('dateTime')
        end_datetime = end.get('dateTime')
        start_date = start.get('date')
        end_date = end.get('date')
        
        if start_datetime and isinstance(start_datetime, str):
            start_datetime = datetime.fromisoformat(start_datetime.replace('Z', '+00:00'))
        if end_datetime and isinstance(end_datetime, str):
            end_datetime = datetime.fromisoformat(end_datetime.replace('Z', '+00:00'))

        # Extract recurrence rule
        rrule = None
        if 'recurrence' in google_event:
            for rule in google_event['recurrence']:
                if rule.startswith('RRULE:'):
                    rrule = rule.replace('RRULE:', '')
                    break
        
        google_recurring_event_id = google_event.get('recurringEventId')
        recurrence_id = None
        if google_recurring_event_id:
            original_start = google_event.get('originalStartTime', {})
            if 'dateTime' in original_start:
                dt = datetime.fromisoformat(original_start['dateTime'].replace('Z', '+00:00'))
                recurrence_id = dt.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            elif 'date' in original_start:
                recurrence_id = original_start['date'].replace('-', '')

            logger.debug(f"[Google PARSE] Raw originalStartTime: {google_event.get('originalStartTime')} -> Parsed recurrence_id: {recurrence_id}")

        is_master_event = 'recurrence' in google_event and not google_recurring_event_id

        event_model = cls(
            uid=private_props.get('caldav-mirror-uid'),
            source_name=private_props.get('caldav-mirror-source'),
            summary=google_event.get('summary'),
            description=google_event.get('description'),
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            start_date=start_date,
            end_date=end_date,
            timezone=start.get('timeZone'),
            location=google_event.get('location'),
            status=google_event.get('status', 'CONFIRMED').upper(),
            transparency=google_event.get('transparency', 'OPAQUE').upper(),
            organizer_email=None,
            attendees=[],
            rrule=rrule,
            recurrence_id=recurrence_id,
            is_master_event=is_master_event,
            google_recurring_event_id=google_event.get('recurringEventId'),
            google_event_id=google_event.get('id'),
            sequence=google_event.get('sequence', 0),
            classification='PUBLIC' if google_event.get('visibility', 'public') == 'public' else google_event.get('visibility', 'PUBLIC').upper()
        )
        logger.debug(f"[Google PARSE] Event ID: {google_event.get('id')} → UID={event_model.uid}, RecurrenceID={event_model.recurrence_id}, recurringEventId={google_event.get('recurringEventId')}, originalStartTime={google_event.get('originalStartTime')}")
        return event_model