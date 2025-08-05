


# CalDAV Mirror

CalDAV Mirror is a headless background service that aggregates events from one or more CalDAV calendars and mirrors them into a single Google Calendar. It is designed to be deployed via Docker, configured declaratively, and run continuously with minimal human interaction — ideal for self-hosters, power users, and future-you.

## 🔧 What It Does

- Connects to multiple CalDAV calendars
- Fetches all events using efficient sync strategies (Sync-Token, CTag, or GTag)
- Stores event data in its own local database (SQLite)
- Pushes those events to a designated Google Calendar
- Ensures the Google Calendar always matches the internal state — if someone edits a mirrored event on Google, the mirror will quietly revert it

## 💡 Philosophy

- Google Calendar is a mirror, **not** a source of truth
- CalDAV sources are authoritative
- The local database is the system’s memory and decision-maker
- Syncing is one-way (CalDAV → Google)
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

    # ID of the Google Calendar to sync to. Defaults to "primary".
    # Find this in your Google Calendar settings.
    GOOGLE_CALENDAR_ID=primary

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
  sync_method: "sync-token"   # or "ctag" or "gtag"
```

Startup will fail if this file is missing or malformed.

## 🧠 Sync Behavior

1. Pull new/updated/deleted events using chosen sync method
2. Normalize and hash event data
3. Store or update events in local DB with internal IDs
4. Push new/changed events to Google Calendar
5. Revert any changes to mirrored events on Google
6. Leave unrelated Google events untouched

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

## 💬 License

MIT. Use it, fork it, build something weird with it.