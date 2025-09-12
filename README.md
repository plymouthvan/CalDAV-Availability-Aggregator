


# CalDAV Mirror

CalDAV Mirror is a headless background service that aggregates events from one or more CalDAV calendars and mirrors them into a single Google Calendar. It is designed to be deployed via Docker, configured declaratively, and run continuously with minimal human interaction — ideal for self-hosters, power users, and future-you.

## 🔧 What It Does

- Connects to multiple CalDAV calendars
- Fetches all events using efficient sync strategies (Sync-Token, CTag, or GTag)
- Stores event data in its own local database (SQLite)
- Pushes those events to a designated Google Calendar
- Ensures the Google Calendar always matches the internal state — if someone edits a mirrored event on Google, the mirror will quietly revert it

## 💡 Philosophy

- Dedicated destination calendar required (empty and exclusively managed by this service)
- The internal database is the single source of truth; Google Calendar is a mirror
- CalDAV sources are authoritative; edits on Google are reverted or deleted
- One-way sync (CalDAV → Google)
- No UI. No surprises. Just logs and YAML.

## 📦 Installation

1. Clone this repo next to your deployment folder:
    ```bash
    git clone https://github.com/your-username/caldav-mirror.git
    ```

2. Create a folder like this:
    ```
    /your-deployment-folder/
    ├── docker-compose.yml
    ├── .env
    └── sources.yml
    ```

3. Configure `.env`:
    ```dotenv
    # Get these from the Google Cloud Console
    GOOGLE_CLIENT_ID=your-client-id.apps.googleusercontent.com
    GOOGLE_CLIENT_SECRET=your-client-secret

    # A 32-byte random string for encrypting tokens.
    # Generate one with: openssl rand -hex 32
    ENCRYPTION_KEY=your-super-secret-32-byte-encryption-key

    # ID of the Google Calendar to sync to.
    # IMPORTANT: Create a dedicated, empty calendar and use its ID here. Do NOT use your "Primary" calendar.
    # The service will refuse to start on first run if the calendar is not empty.
    GOOGLE_CALENDAR_ID=your-dedicated-calendar-id@group.calendar.google.com

    # Path to the database file inside the container.
    # It's recommended to leave this as the default.
    DATABASE_PATH=/app/data/caldav_mirror.db

    # Optional: How often to sync, in seconds. Defaults to 300 (5 minutes).
    SYNC_INTERVAL_SECONDS=300
    ```

4. Configure `sources.yml`. See below.

5. Start the service:
    ```bash
    docker compose up -d
    ```

6. Authenticate with Google via device code (prompted in logs).

## 🔐 Authentication

This tool uses Google’s OAuth 2.0 Device Flow to authorize access to the destination calendar. You’ll be prompted once on first run.

No credentials are stored in Google — tokens are kept in the SQLite DB, encrypted using a key from `.env`.

## 🗂️ `sources.yml` Format

You must explicitly declare each CalDAV source and how it should be synced. Auto-detection is **not** supported.

```yaml
- name: "Work Calendar"
  url: "https://cal.example.com/user/calendars/work/"
  username: "your-username"
  password: "your-password"
  provider: "generic"  # or "icloud"
  sync_method: "sync-token"   # or "ctag"
```

Startup will fail if this file is missing or malformed.

## 🧠 Sync Behavior

1. Pull new/updated/deleted events from CalDAV sources using the configured method
2. Normalize and hash event data
3. Persist desired state in the local DB
4. Reconcile Google Calendar to match DB exactly:
   - Create any DB event missing on Google
   - Update any differing Google event to match DB
   - Delete any Google event that is not present in the DB (including user-created or split recurring series)
5. Changes made directly on the destination calendar are removed on the next sync

## ⚠️ Dedicated Calendar Requirement & Data Loss Warning

This application now requires a dedicated, empty Google Calendar that it exclusively manages.

- On first run (when the local DB has no events), startup will fail if the selected Google Calendar contains any events.
- Do NOT point this tool at your primary calendar.
- Anything manually created on the destination Google Calendar (including events created via Google UI, mobile apps, or third-party integrations) will be automatically deleted on the next sync cycle unless it originates from your CalDAV sources and is present in the local database.
- If a user splits a recurring event on Google using “This and following events,” Google will create a new series. That new series will not exist in the database and will be deleted on the next sync.

To proceed safely:
- Create a brand-new Google Calendar (Settings → Add calendar → Create new calendar).
- Use its calendar ID in GOOGLE_CALENDAR_ID.
- Keep that calendar exclusively managed by CalDAV Mirror.

## 🗃️ Database

SQLite is used for persistence. All synced events are stored with:
- Internal event ID
- CalDAV UID
- Hash of normalized data
- Last sync timestamps
- Google event ID

## 🚫 Not Included

- No web UI
- No config API
- No calendar browsing
- No two-way sync

## 🛠️ Tools

This project includes several command-line tools to help you manage and diagnose your sync configuration.

### `capabilities.py`

This tool checks all sources in your `sources.yml` file to determine the best available synchronization method.

**Usage:**
```bash
docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/capabilities.py
```

### `test_source.py`

This tool performs a "dry run" of the sync process for a single source. It will connect to the CalDAV server, fetch events, and parse them, but it will **not** make any changes to your Google Calendar or the local database. This is useful for debugging connection or parsing issues.

**Usage:**
```bash
docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/test_source.py "Your Source Name"
```

### `source_cleanup.py`

This tool will remove all data associated with a specific source, including all of its events from Google Calendar and the local database. This is useful if you want to remove a source and its synced data permanently.

**Usage:**
```bash
docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/source_cleanup.py "Your Source Name"
```

### `clear_db.py`

This tool will remove all event data from the local database. This is useful for forcing a complete re-sync from all sources, as it makes the application believe it has never seen any of the events before. It does **not** delete anything from Google Calendar and it preserves your authentication tokens and sync states.

**Usage:**
```bash
docker compose -f docker/docker-compose.yml run --rm --entrypoint python3 caldav-mirror tools/clear_db.py
```

## 💬 License

MIT. Use it, fork it, build something weird with it.
## 🧭 Windowed Projection Architecture (Flattened Instances)

This service now treats Google Calendar as a cache of explicitly projected instances over a rolling time window. Instead of mirroring recurrence semantics (RRULE/EXDATE) on Google, each occurrence is created as a standalone event. This eliminates “this and future” splits, EXDATE reconciliation, and other edge cases. Reconciliation becomes pure set arithmetic.

Key properties:
- Deterministic and idempotent: re-running a cycle with no changes produces zero operations.
- Instance-level hashing: each projected instance carries a fingerprint to detect changes reliably.
- Ownership tagging: events are tagged in extendedProperties.private so we only touch what we own.

What goes to Google per event instance:
- extendedProperties.private
  - caldav-mirror-source: source name
  - caldav-mirror-uid: CalDAV UID
  - caldav-mirror-instance-key: stable instance key (uid + effective start/end)
  - caldav-mirror-hash: projected content hash
  - caldav-mirror-version: “flat-1”

No recurringEventId, no recurrence arrays are used for new events in this model.

## 🪟 Rolling Window Configuration (ENV, hot-reloaded)

These environment variables control how far back and forward we project instances. They are read every sync cycle (no restart required):

- PROJECTION_WINDOW_PAST_DAYS (default: 30)
- PROJECTION_WINDOW_FUTURE_MONTHS (default: 18)
- PROJECTION_DRY_RUN (default: false)

Safety clamps:
- PROJECTION_WINDOW_PAST_DAYS is clamped to [0, 3650]
- PROJECTION_WINDOW_FUTURE_MONTHS is clamped to [0, 60]

Effects:
- Widening the window adds newly in-range historical/future instances
- Shrinking the window triggers garbage collection of out-of-window owned instances

## ⚙️ Skip Gating and Fingerprinting

To minimize API calls, each cycle can be skipped when both conditions are met:
- Google reports no changes since last syncToken (incremental delta)
- The local desired window fingerprint hasn’t changed for this source

If the syncToken expires (410), we automatically compute a new baseline token and continue.

## 🔎 Observability (Structured Logs)

During projection, several structured log objects are emitted to aid debugging:
- type: WINDOW_SKIP_GATE — skip decision and input conditions
- type: PROJECTION_PLAN — counts of desired/owned keys, creates, deletes
- type: PROJECTION_DRY_RUN — dry-run plan (no mutations)
- type: PROJECTION_DRY_RUN_SKIPPED_STATE — indicates state persistence is skipped in dry-run

You can set PROJECTION_DRY_RUN=true to preview actions safely.

## 🧪 Behavioral Summary (Set Arithmetic)

Within the [window_start, window_end) UTC:
- Create: instances that should exist (from DB) but do not in Google
- Delete: owned Google events that should not exist (rogue or out-of-window)
- Replace: owned Google instances whose projected hash differs (delete + create)

Running the same inputs twice yields zero operations on the second run.

## 🚚 Migration Plan (from recurrence-mirroring)

1) Prepare a dedicated calendar (empty, exclusive) as before.
2) Enable dry-run:
   - Set PROJECTION_DRY_RUN=true
   - Let the service run at least one full cycle
   - Inspect logs for PROJECTION_PLAN and PROJECTION_DRY_RUN entries (creates/deletes)
3) Cutover:
   - Set PROJECTION_DRY_RUN=false
   - Allow one or more cycles to complete
   - Legacy recurring masters we own will be deleted and replaced with flattened instances
4) Adjust window as needed:
   - Widen to backfill history/future
   - Shrink to prune old, out-of-window instances (automatic GC)
5) Steady state:
   - Sync gating via syncToken + fingerprint should skip most cycles when nothing changes

## 🧾 Additional .env Settings

In addition to the existing .env keys (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, ENCRYPTION_KEY, GOOGLE_CALENDAR_ID, DATABASE_PATH, SYNC_INTERVAL_SECONDS), add the following optional tuning parameters:

# Rolling window sizing
PROJECTION_WINDOW_PAST_DAYS=30
PROJECTION_WINDOW_FUTURE_MONTHS=18

# Preview mode (no mutations to Google, no state persisted)
PROJECTION_DRY_RUN=false

## ❗ Notes and Guarantees

- Determinism: projection is derived entirely from your CalDAV data and the configured window.
- Idempotency: identical inputs produce identical outputs; second run with no changes results in zero operations.
- Ownership: only events carrying our private ownership marker are considered for delete/replace; anything else is treated as unmanaged and removed in dedicated-calendar mode.
- Hot-Reload: changing PROJECTION_* env vars takes effect on the next cycle without restart.
