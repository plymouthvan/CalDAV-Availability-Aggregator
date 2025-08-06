"""
Reconciliation module for CalDAV Mirror

Compares the state of the local database with the state of Google Calendar
and generates the necessary operations to make Google Calendar a mirror of
the database.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timedelta, timezone
from icalendar import vRecur

from .google_client import GoogleClient
from database import Database
from .event_model import EventModel

logger = logging.getLogger(__name__)

class Reconciler:
    """
    Orchestrates the reconciliation between the local database and Google
    Calendar for a specific source, with full support for recurring events.
    """

    def __init__(self, google_client: GoogleClient, database: Database):
        self.google = google_client
        self.db = database

    async def reconcile_source(self, source_name: str):
        """
        Performs a full, recurrence-aware reconciliation for a given source.
        """
        logger.info(f"Starting reconciliation for source: {source_name}")

        db_instances = await self.db.get_all_events_for_source(source_name)
        google_master_events_raw = await self.google.list_mirrored_master_events(source_name)
        
        google_instances = {}
        for uid, g_master_event in google_master_events_raw.items():
            master_model = EventModel.from_google_event(g_master_event)
            google_instances[(uid, None)] = master_model

            if g_master_event.get('recurrence'):
                instances_raw = await self.google.get_event_instances(g_master_event['id'])
                for g_instance in instances_raw:
                    if g_instance.get('status') == 'cancelled':
                        continue
                    
                    instance_model = EventModel.from_google_event(g_instance)
                    if instance_model.recurrence_id:
                        key = (uid, instance_model.recurrence_id)
                        google_instances[key] = instance_model

        # Expand recurring events from the database into a full instance list
        desired_instances = self._expand_db_events(db_instances)

        db_keys = set(desired_instances.keys())
        google_keys = set(google_instances.keys())

        to_create: List[EventModel] = []
        to_update: List[Tuple[str, EventModel]] = []
        to_delete: List[str] = []

        # Events to create
        for key in db_keys - google_keys:
            model = desired_instances[key]
            if model.recurrence_id:
                master_key = (model.uid, None)
                master_gcal_event = google_instances.get(master_key)
                if master_gcal_event and master_gcal_event.google_event_id:
                    model.google_recurring_event_id = master_gcal_event.google_event_id
                else:
                    logger.warning(f"Cannot create exception {key} because its master is not in Google yet.")
                    continue
            to_create.append(model)

        # Events to delete
        for key in google_keys - db_keys:
            google_model = google_instances[key]
            if google_model.google_event_id:
                to_delete.append(google_model.google_event_id)

        # Events to update
        for key in db_keys.intersection(google_keys):
            db_model = desired_instances[key]
            google_model = google_instances[key]
            
            if db_model.compute_hash() != google_model.compute_hash():
                db_model.google_event_id = google_model.google_event_id
                if db_model.recurrence_id:
                    master_key = (db_model.uid, None)
                    master_gcal_event = google_instances.get(master_key)
                    if master_gcal_event:
                        db_model.google_recurring_event_id = master_gcal_event.google_event_id
                to_update.append((db_model.google_event_id, db_model))

        logger.info(f"Reconciliation plan: {len(to_create)} create, {len(to_update)} update, {len(to_delete)} delete.")

        if to_create:
            created_map = await self.google.batch_create_events(to_create)
            if created_map:
                await self.db.bulk_update_google_ids(source_name, created_map)
        
        if to_update:
            await self.google.batch_update_events(to_update)
        
        if to_delete:
            await self.google.batch_delete_events(to_delete)

        logger.info(f"Reconciliation finished for source: {source_name}")

    def _expand_db_events(self, db_events: Dict[Tuple[str, Optional[str]], Dict[str, Any]]) -> Dict[Tuple[str, Optional[str]], EventModel]:
        """
        Expands recurring events from the database into a full instance list.
        """
        expanded_events = {}
        db_master_events = {k[0]: EventModel.from_dict(v['event_data']) for k, v in db_events.items() if v['event_data'].get('is_master_event')}
        db_exceptions = {k: EventModel.from_dict(v['event_data']) for k, v in db_events.items() if not v['event_data'].get('is_master_event')}

        for uid, master_event in db_master_events.items():
            if not master_event.rrule:
                expanded_events[(uid, None)] = master_event
                continue

            try:
                rule = vRecur.from_string(f"RRULE:{master_event.rrule}")
                start_dt = master_event.start_datetime
                duration = master_event.end_datetime - start_dt
                
                now = datetime.now(start_dt.tzinfo)
                after = now - timedelta(days=1) # Include today
                before = now + timedelta(days=730) # 2 years into the future
                
                for instance_start in rule.iterset(dtstart=start_dt, after=after, before=before):
                    rid = instance_start.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
                    key = (uid, rid)
                    
                    if key not in db_exceptions:
                        instance_model = EventModel.from_dict(master_event.to_dict())
                        instance_model.start_datetime = instance_start
                        instance_model.end_datetime = instance_start + duration
                        instance_model.recurrence_id = rid
                        instance_model.is_master_event = False
                        instance_model.rrule = None
                        expanded_events[key] = instance_model

            except Exception as e:
                logger.error(f"Failed to expand RRULE for event {uid}: {e}")

        # Add exceptions to the final list, overriding any expanded instances
        for key, ex_model in db_exceptions.items():
            expanded_events[key] = ex_model
            
        return expanded_events
