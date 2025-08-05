


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
    ```
    GOOGLE_CLIENT_ID=your-client-id.apps.googleusercontent.com
    GOOGLE_CLIENT_SECRET=your-client-secret
    GOOGLE_REDIRECT_URI=urn:ietf:wg:oauth:2.0:oob
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

## 🧪 Validating a CalDAV Source

Run the provided `capabilities.py` (or similar script) to determine what sync methods are supported for your CalDAV server. Add the result manually to `sources.yml`.

## 💬 License

MIT. Use it, fork it, build something weird with it.