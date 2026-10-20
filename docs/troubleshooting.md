# Troubleshooting

Start with the snapshot's status. It shows Bright Data's view, the local stage, and the last
error:

```bash
bdh status sd_m1a2b3c4d5e6f7g8h
bdh runs                          # every local run, most recent first
```

Re-run with `--log-level DEBUG` to see each request. Remove credentials before sharing any
output.

## By exit code

| Code | What happened | What to do |
| --- | --- | --- |
| 2 | Configuration or usage error | Read the message; it names the missing variable or bad option. See [Configuration](#configuration). |
| 3 | Unknown dataset or method, or invalid inputs | Check the id with `bdh scrapers show <id>`; fix the CSV rows listed in the error. Nothing was triggered. |
| 4 | Bright Data API error | Check the key and your Bright Data account balance. The HTTP status is in the message. |
| 5 | Collection failed or was canceled | Nothing was written. Check the job in the Bright Data dashboard, fix the inputs, and push again. |
| 6 | Snapshot not ready in time | Nothing is lost. Resume with `bdh replay <snapshot_id>`, or raise `BDH_POLL_TIMEOUT_SECONDS`. |
| 7 | Schema mismatch, or no publishable records | Read the report path in the error. See [Schema mismatch](#schema-mismatch). |
| 8 | Hotdata upload or load failed | Fix the cause (see [Hotdata writes](#hotdata-writes)), then `bdh replay <snapshot_id>`. |
| 9 | Run record missing or unusable | The state directory was moved or deleted. Push again with `--snapshot-id`. |

## Configuration

**"Missing configuration: …"** The named variables aren't set. Check that `.env` is in the
directory you run `bdh` from, or pass `--env-file`.

**"N Hotdata databases found"** The workspace has several databases. Run `bdh databases` and set
`HOTDATA_DATABASE_ID` or `HOTDATA_DATABASE_NAME`.

**A value in `.env` seems to be ignored.** An exported environment variable with the same name
takes precedence over `.env`. Check with `env | grep -E 'BRIGHTDATA|HOTDATA|BDH'`.

## Schema mismatch

The run stops before uploading and writes
`.bdh/runs/<snapshot_id>/report.json`, which lists each problem with its line number, the
field, and what was wrong with it.

- **A few bad records:** decide whether they matter. To publish anyway, re-push without the
  options that caused the failure (for example without `--strict-fields`).
- **A field changed type for every record:** the scraper's output has changed. Use
  `--stringify-nested` if it's a nested field, or open an issue with the report.
- **"No publishable records":** every record was an error from Bright Data. Check
  `rejected.ndjson` for the error codes, usually dead or blocked URLs.

The downloaded snapshot is kept, so re-pushing doesn't pay for a new collection.

## Hotdata writes

The error includes the stage (`database lookup`, `upload` or `load`) and Hotdata's trace id.
Quote the trace id if you contact Hotdata support.

- **Rate limits and conflicts** (HTTP 409, 429, 5xx) are retried automatically with backoff
  before the run fails.
- **Schema or permission errors** need fixing first, for example creating the schema or using a
  key with write access.
- **Replay** resumes from the failed stage. A finished upload is reused rather than uploaded
  again, and a run that already loaded is never loaded twice.

## Unexpected results

**The table only has the latest rows.** The default mode is `replace`. Use `--mode append` or
`--mode upsert --key <column>` to keep earlier data.

**A URL was split into two inputs.** `-q` splits on commas. Put the inputs in a CSV file.

**The Hotdata console shows column names and types but no rows.** That view is the schema. Query
the table to see data, for example with
[`examples/query_hotdata.py`](../examples/query_hotdata.py).

**Replay fails on a CI runner.** The state directory didn't survive between jobs. Persist
`BDH_STATE_DIR`, or push again with `--snapshot-id`.
