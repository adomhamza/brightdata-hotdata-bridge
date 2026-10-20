# Architecture

## Pipeline

```text
inputs (CSV / values)
   │  validated against the scraper's input schema (catalog)
   ▼
Bright Data  POST /datasets/v3/trigger ──► snapshot id ──► run record created
   │  poll GET /datasets/v3/progress/{id} until ready (or failed / timeout)
   ▼
download GET /datasets/v3/snapshot/{id} ──► raw.ndjson (streamed to disk)
   │  validate each record against the scraper's output schema
   ▼
clean.ndjson + rejected.ndjson + report.json
   │
   ▼
Hotdata  upload file to object storage ──► upload id
   │  load into <database>.<schema>.<table> as a background job
   ▼
table row count ──► run record marked loaded
```

Completion is detected by polling only. A webhook would need a public HTTPS endpoint, which a
library and a CLI can't provide by themselves.

The Hotdata target database is resolved **before** the collection is triggered. A run that
couldn't be published never starts a paid Bright Data job.

## Stages and the run record

Each run has a record at `<BDH_STATE_DIR>/runs/<snapshot_id>/run.json`, saved after every stage:

| Stage | Completed when |
| --- | --- |
| `triggered` | Bright Data accepted the job and returned a snapshot id |
| `ready` | Bright Data reported the snapshot as ready |
| `downloaded` | The snapshot is on local disk |
| `validated` | Records passed validation; the clean file exists |
| `uploaded` | Hotdata accepted the file; the upload id is recorded |
| `loaded` | The table load finished |

A failure saves the stage that failed and the error, and leaves the stage at the last one
completed. Replay (`bdh replay`, `replay_run`) resumes from the next stage, so:

- a timeout keeps polling the same snapshot, without triggering again;
- a failed load reuses the upload, without downloading or uploading again;
- a loaded run is reported as already loaded instead of loading twice;
- changing the target table or database starts again from validation, with a fresh upload.

## Modules

| Module | Responsibility |
| --- | --- |
| `catalog.py` | Downloads and caches Bright Data's scraper catalog; looks up scrapers and methods. |
| `inputs.py` | Reads CSV files and values into typed collection inputs. |
| `brightdata.py` | Async Bright Data client: trigger, progress polling, streamed download. |
| `validation.py` | Line-by-line validation; splits clean and rejected records; writes the report. |
| `hotdata_client.py` | Resolves the database; uploads the file; runs the load job. |
| `state.py` | Run records and their file paths. |
| `pipeline.py` | Orchestrates the stages; public library entry points. |
| `cli.py` | The `bdh` CLI, a thin layer over `pipeline.py`. |
| `config.py` | Settings from overrides, the environment and `.env`. |
| `errors.py` | `BridgeError` hierarchy; each subclass has its CLI exit code. |
| `models.py` | Pydantic request, result and catalog models. |
| `logging_setup.py` | Text or JSON logs on stderr. |

The library and the CLI share one code path: every CLI command calls the same functions a
Python user would.

## Reliability

- **Retries.** Both clients honour `Retry-After` (capped at 120 s) and otherwise back off for
  2, 4, 8, 16 and 32 seconds. Hotdata calls are retried on 409, 429 and 5xx responses and on
  connection errors.
- **No duplicate collections.** A Bright Data trigger is only retried when it provably never
  reached the server (a connection failure before sending), so a retry can't start a second
  paid job.
- **Large files.** Snapshots are streamed to disk and uploaded with multipart uploads, so memory
  use doesn't grow with snapshot size. Input CSVs are read into memory.
- **Crash safety.** The run record and the downloaded snapshot are written to a temporary file
  and renamed, so a crash never leaves them half-written. The stage only advances once a step
  has finished, so an interrupted validation simply runs again. Hotdata loads are atomic on the
  server side.

## Security

- Credentials come only from the environment, `.env` or a secrets manager, and are held as
  secrets so they're never logged.
- Snapshot ids are validated before they're used in file paths, so they can't escape the state
  directory.
- The repository has secret scanning and push protection enabled; see
  [SECURITY.md](../SECURITY.md).
