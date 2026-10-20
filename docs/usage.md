# Usage

## Choosing a scraper

Every Bright Data scraper has a dataset id (`gd_...`) and one or more collection methods.
`collect_by_url` takes URLs; `discover_by_*` methods take keywords, categories, user names and
so on.

```bash
bdh scrapers list --search amazon          # find the dataset id
bdh scrapers show gd_l7q7dkf244hwjntr0     # methods and the CSV columns each one needs
bdh scrapers list --refresh                # re-download the catalog
```

Inputs are checked against the scraper's input schema before anything is triggered, so a bad
CSV costs nothing.

## Inputs

**A CSV file** whose header row names the inputs. Use this for several columns or many rows:

```text
url,zipcode
https://www.amazon.com/dp/B0CRMZHDG8,10001
https://www.amazon.com/dp/B09V3KXJPB,94107
```

```bash
bdh push -d gd_l7q7dkf244hwjntr0 -i urls.csv -t amazon_products
```

**Inline values** with `-q`, when the method has a single required input:

```bash
bdh push -d gd_l7q7dkf244hwjntr0 -q "https://www.amazon.com/dp/B0CRMZHDG8" -t amazon_products
```

`-q` splits on commas, so a URL that contains a comma is cut in two. Use a CSV file for those.

All inputs go to Bright Data as **one** job and become one snapshot, however many rows there
are (up to Bright Data's 1 GB input limit).

**Discovery methods** take their own inputs and usually a per-input limit:

```bash
bdh push -d gd_l7q7dkf244hwjntr0 -m discover_by_keyword -i keywords.csv \
  --limit-per-input 50 -t amazon_search
```

## Collect now or later

`bdh push` triggers, waits, validates and publishes in one step. To return immediately and
finish later:

```bash
bdh trigger -d gd_l7q7dkf244hwjntr0 -i urls.csv -t amazon_products   # prints the snapshot id
bdh status sd_m1a2b3c4d5e6f7g8h                                      # check on it
bdh replay sd_m1a2b3c4d5e6f7g8h                                      # wait, then publish
```

To publish a snapshot that already exists, for example one started from the Bright Data
dashboard:

```bash
bdh push --snapshot-id sd_m1a2b3c4d5e6f7g8h --table amazon_products
```

## Load modes

| Mode | Effect | Needs `--key` |
| --- | --- | --- |
| `replace` (default) | The snapshot becomes the table's whole contents. | No |
| `append` | Rows are added to the table. | No |
| `upsert` | Rows matching the key replace existing ones; others are added. | Yes |
| `update` | Rows matching the key replace existing ones; others are ignored. | Yes |
| `delete` | Rows matching the key are removed. | Yes |

**`replace` discards every existing row.** To build up history, use `append`. To keep one
current row per record, use `upsert` with a stable key, for example:

```bash
bdh push -s sd_m1a2b3c4d5e6f7g8h -t linkedin --mode upsert --key id
```

The key is a matching rule, not a uniqueness constraint: Hotdata won't stop duplicates that
arrive through `append`.

The first load into a new table is always `replace`, as Hotdata requires. If you ask for
another mode and the table doesn't exist yet, `replace` is used automatically. Loads are
atomic: readers see either the old contents or the new, and a failed load changes nothing.

## Validation

Every record is checked against the scraper's output schema before anything is uploaded:

- Records Bright Data marked as errors (dead pages, blocked URLs) are set aside in
  `rejected.ndjson` and counted as "rows rejected", not loaded.
- A value of the wrong type pauses the run (exit code 7) and writes `report.json`. Nothing is
  uploaded.
- Fields that aren't in the schema are allowed and listed as schema drift in the report.

Options:

```bash
--require-field name       # the field must be present and non-null (repeatable)
--strict-fields            # treat fields missing from the schema as a mismatch
--stringify-nested         # store nested objects and arrays as JSON text
```

Nested objects and arrays are otherwise stored as structured columns. Query them with
`company.name` and `unnest(experience)`; see [`examples/how_to_query.md`](../examples/how_to_query.md).

## Output and logging

- `--json` on `push`, `replay`, `status`, `runs`, `trigger` and `databases` prints
  machine-readable output to stdout.
- Logs go to stderr. `--log-level DEBUG` shows every request; `--log-json` emits JSON lines for
  log pipelines.
- Exit codes are listed in the [README](../README.md#exit-codes); scripts and schedulers can
  branch on them.

## From Python

The blocking call mirrors `bdh push`:

```python
from brightdata_hotdata_bridge import push_snapshot_to_hotdata

result = push_snapshot_to_hotdata(
    queries=["https://example.com/product/1"],
    dataset_id="gd_l7q7dkf244hwjntr0",
    table_name="products",
)
print(f"Loaded {result.row_count} rows")
```

In async code (FastAPI, notebooks, orchestrators), use `build_collection_request` and
`arun_pipeline`; the [README](../README.md#library) has an example. Every failure raises a
subclass of `BridgeError`. [`examples/push_with_library.py`](../examples/push_with_library.py)
shows how to handle each one.
