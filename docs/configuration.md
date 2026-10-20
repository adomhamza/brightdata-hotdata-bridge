# Configuration

The library and the `bdh` CLI read the same settings, in this order of precedence:

1. Keyword overrides passed to `get_settings(...)` (library only).
2. Environment variables.
3. A `.env` file in the working directory, or the file given with `bdh --env-file`.

Empty values count as unset. Credentials are held as secrets and never appear in logs or
error messages.

## Required

| Variable | Meaning |
| --- | --- |
| `BRIGHTDATA_API_KEY` | Bright Data API key. |
| `HOTDATA_API_KEY` | Hotdata API key. |
| `HOTDATA_WORKSPACE_ID` | Hotdata workspace that holds the target database. |

They're checked before a collection is triggered, so a paid Bright Data job never starts when
its results couldn't be published. Commands that don't need them, such as `bdh scrapers`, work
without them.

## Hotdata target

| Variable | Default | Meaning |
| --- | --- | --- |
| `HOTDATA_DATABASE_ID` | looked up | Database to publish into. |
| `HOTDATA_DATABASE_NAME` | looked up | Choose the database by name instead of id. |
| `HOTDATA_SCHEMA` | the database's default schema, then `public` | Schema that tables are created in. |
| `HOTDATA_HOST` | Hotdata's production API | Only for non-production Hotdata environments. |

### How the target database is chosen

Before anything is triggered, the bridge picks the database in this order:

1. `HOTDATA_DATABASE_ID`, if set.
2. `HOTDATA_DATABASE_NAME`, if set.
3. When resuming a run, the database that run already used.
4. The workspace's only database.

If the workspace has several databases and none of the above applies, the run stops with exit
code 2 and lists them. `bdh databases` shows what's available and which one would be used.

Every run records its database, so `bdh replay` always publishes to the same place even if the
settings have changed since.

## Timing

| Variable | Default | Meaning |
| --- | --- | --- |
| `BDH_POLL_INTERVAL_SECONDS` | `10` | Seconds between Bright Data progress checks. |
| `BDH_POLL_TIMEOUT_SECONDS` | `3600` | How long to wait for a collection before stopping (exit code 6, resumable). |
| `BDH_LOAD_TIMEOUT_SECONDS` | `1800` | How long to wait for a Hotdata load job. |
| `BDH_HTTP_TIMEOUT_SECONDS` | `60` | Timeout for a single HTTP request. |

Large discovery jobs can take longer than an hour. Raise `BDH_POLL_TIMEOUT_SECONDS`, or let the
run time out and resume it later; no data is lost.

## Local state and the scraper catalog

| Variable | Default | Meaning |
| --- | --- | --- |
| `BDH_STATE_DIR` | `.bdh` | Run records, downloaded snapshots and the catalog cache. |
| `BDH_CATALOG_URL` | `https://docs.brightdata.com/scrapers-full.json` | Where the scraper catalog is downloaded from. |
| `BDH_CATALOG_TTL_HOURS` | `24` | How long the cached catalog is used before it's refreshed. |
| `BDH_CATALOG_FILE` | not set | Use this local catalog file instead of downloading one. |

Replay depends on the state directory surviving between runs. On CI runners and in
containers, point `BDH_STATE_DIR` at a persistent volume or cache it between jobs; otherwise a
failed run can't be replayed and has to be pushed again from its snapshot id.

## From Python

```python
from brightdata_hotdata_bridge import get_settings, push_snapshot_to_hotdata

settings = get_settings(hotdata_schema="analytics", poll_timeout_seconds=7200)
result = push_snapshot_to_hotdata(
    snapshot_id="sd_m1a2b3c4d5e6f7g8h", table_name="products", settings=settings
)
```

Overrides use the field names, which are the variable names in lower case without the `BDH_`
prefix (for example `state_dir`, `catalog_ttl_hours`).
