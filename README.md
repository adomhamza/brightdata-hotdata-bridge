# brightdata-hotdata-bridge

Collect web data with [Bright Data](https://brightdata.com) scrapers and publish it into
[Hotdata](https://www.hotdata.dev) tables, from Python or the `bdh` command line.

Give it a scraper's dataset id and a CSV of URLs (or keywords, usernames, and so on).
It triggers the collection, waits for it, validates the records, and loads them into a
Hotdata table.

```text
inputs.csv ─► Bright Data trigger ─► poll until ready ─► download NDJSON
           ─► validate against the scraper schema ─► Hotdata upload ─► table load
```

## Features

- **Any Bright Data scraper.** Dataset ids and their inputs are checked against Bright
  Data's published [scraper catalog](https://docs.brightdata.com/scrapers-full.json)
  (1,200+ scrapers, cached locally), so a bad CSV is rejected before a paid collection
  starts.
- **Collect and discover.** Supports `collect_by_url` and every `discover_by_*` method
  (keyword, category, user name, and more).
- **Validated before writing.** Records are type-checked against the scraper's output
  schema. A mismatch pauses ingestion and writes a report. Records Bright Data flags
  as errors are set aside rather than loaded.
- **Large snapshots.** The snapshot is streamed to disk and uploaded straight to Hotdata
  object storage, using multipart for big files. Loads run as background jobs.
- **Resumable and replayable.** Every stage is saved to a local run record. A timeout,
  a failed write or a fixed schema problem is resumed with `bdh replay`, and a run is
  never loaded twice.
- **Polite to the APIs.** Honours `Retry-After` with exponential backoff, and only
  retries non-idempotent requests when they provably never reached the server.

## Install

```bash
pip install brightdata-hotdata-bridge        # library + CLI
pipx install brightdata-hotdata-bridge       # CLI only, isolated environment
```

Requires Python 3.10+.

## Configure

Copy `.env.example` to `.env` (or export the variables):

```bash
BRIGHTDATA_API_KEY=...
HOTDATA_API_KEY=...
HOTDATA_WORKSPACE_ID=...
```

The target database is looked up from the workspace before anything is triggered. If the
workspace has exactly one database it is used automatically; if it has several, set
`HOTDATA_DATABASE_ID` or `HOTDATA_DATABASE_NAME` (run `bdh databases` to list them). Each
run records the database it used, so `bdh replay` always publishes to the same place.

Optional settings (schema, polling interval and timeout, state directory, catalog
source) are documented in [`.env.example`](.env.example). The library and the CLI read
the same configuration.

## CLI

Find a scraper and the CSV columns it needs:

```bash
bdh scrapers list --search amazon
bdh scrapers show gd_l7q7dkf244hwjntr0
```

Collect and publish in one step:

```bash
# urls.csv has a header row:  url
bdh push --dataset-id gd_l7q7dkf244hwjntr0 --input-file urls.csv --table amazon_products

# A single required input can be passed inline
bdh push -d gd_l7q7dkf244hwjntr0 -q "https://www.amazon.com/dp/B0CRMZHDG8" -t amazon_products

# Discovery methods take their own inputs (keywords.csv header: keyword)
bdh push -d gd_l7q7dkf244hwjntr0 -m discover_by_keyword -i keywords.csv \
  --limit-per-input 50 -t amazon_search --mode append
```

Publish a snapshot that already exists, or start a collection and finish it later:

```bash
bdh push --snapshot-id sd_m1a2b3c4d5e6f7g8h --table amazon_products
bdh trigger -d gd_l7q7dkf244hwjntr0 -i urls.csv -t amazon_products   # prints snapshot id
bdh replay sd_m1a2b3c4d5e6f7g8h
```

Operate:

```bash
bdh status sd_m1a2b3c4d5e6f7g8h      # Bright Data status + local stage + last error
bdh runs                              # all local runs
bdh databases                         # Hotdata databases, and which one push will use
bdh replay sd_m1a2b3c4d5e6f7g8h       # resume after a timeout or failed write
```

Add `--json` to `push`, `replay`, `status`, `runs` and `trigger` for machine-readable
output, and `--log-json` for JSON logs.

### Load modes

| Mode      | Effect                                                    |
| --------- | --------------------------------------------------------- |
| `replace` | The snapshot becomes the table's contents (default).      |
| `append`  | Rows are added to the existing table.                     |
| `upsert`  | Rows matching `--key` columns are replaced, others added. |
| `update`  | Rows matching `--key` columns are replaced.               |
| `delete`  | Rows matching `--key` columns are removed.                |

> **`replace` is the default and discards every existing row.** Pushing to a table that
> already holds data without `--mode` leaves only the new snapshot's rows. To keep earlier
> data, use `--mode append`, or `--mode upsert --key <column>` to keep one up-to-date row
> per record (for example `--key id` for LinkedIn profiles).

The first load into a new table is always `replace`, as Hotdata requires. If you ask
for another mode on a table that does not exist yet, `replace` is used automatically.
Every load is atomic: readers see the old contents or the new ones, never a mix, and a
failed load leaves the table unchanged.

### Validation options

- `--require-field name` (repeatable): the field must be present and non-null.
- `--strict-fields`: fail on fields that are not in the scraper's output schema
  (by default they are allowed and listed as schema drift in the report).
- `--stringify-nested`: store nested objects and arrays as JSON text.

### Exit codes

| Code | Meaning                                    |
| ---- | ------------------------------------------ |
| 0    | Success                                    |
| 2    | Configuration or usage error               |
| 3    | Unknown dataset / method, or invalid input |
| 4    | Bright Data API error                      |
| 5    | Collection failed or was canceled          |
| 6    | Snapshot not ready in time (resumable)     |
| 7    | Schema mismatch or no publishable records  |
| 8    | Hotdata write failed (replayable)          |
| 9    | Local run record missing or unusable       |

## Library

```python
from brightdata_hotdata_bridge import push_snapshot_to_hotdata

result = push_snapshot_to_hotdata(
    dataset_id="gd_l7q7dkf244hwjntr0",
    input_file="urls.csv",
    table_name="amazon_products",
)
print(f"{result.rows_published} rows published; table now has {result.table_row_count}")
```

Async code (FastAPI, notebooks, orchestrators) should use the async API:

```python
from brightdata_hotdata_bridge import (
    PushRequest,
    TableTarget,
    arun_pipeline,
    build_collection_request,
    get_settings,
)

settings = get_settings()
collection = await build_collection_request(
    dataset_id="gd_l7q7dkf244hwjntr0",
    queries=["https://www.amazon.com/dp/B0CRMZHDG8"],
    settings=settings,
)
result = await arun_pipeline(
    PushRequest(collection=collection, target=TableTarget(table="amazon_products")),
    settings,
)
```

Every error derives from `BridgeError`. Catch `HotdataWriteError` to schedule a
`replay_run(snapshot_id)`, or `SchemaMismatchError` to inspect `exc.report_path`.

## How failures are handled

| Situation                         | Behaviour                                                                 |
| --------------------------------- | ------------------------------------------------------------------------- |
| Collection `failed` / `canceled`  | Logged with the snapshot id; nothing is written to Hotdata.               |
| Snapshot not ready before timeout | Run is saved; `bdh push --snapshot-id` or `bdh replay` resumes polling.   |
| Schema or type mismatch           | Ingestion pauses; `report.json` lists every issue; nothing is written.    |
| Hotdata upload or load fails      | Logged with Hotdata's trace id; `bdh replay` retries from that stage.     |
| Bright Data / Hotdata rate limits | `Retry-After` honoured, exponential backoff (2, 4, 8, 16, 32 s).          |

Run state lives in `.bdh/runs/<snapshot_id>/`: `run.json` (stage, target, upload id,
last error), the raw and validated NDJSON, the rejected records, and the validation
report.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && ruff format --check .
mypy
pytest --cov
```

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributors sign the
[Contributor License Agreement](CLA.md) on their first pull request. Licensed under
[Apache-2.0](LICENSE).
