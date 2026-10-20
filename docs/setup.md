# Setup

## 1. Install

Python 3.10 or newer is required.

```bash
pip install brightdata-hotdata-bridge     # library and the bdh CLI
pipx install brightdata-hotdata-bridge    # CLI only, in its own environment
```

Check it worked:

```bash
bdh --version
```

## 2. Get the credentials

You need three values. Keep them out of source control.

| Setting | Where to find it |
| --- | --- |
| `BRIGHTDATA_API_KEY` | Bright Data control panel, **Account settings → API keys**. The key needs access to the Web Scraper API. |
| `HOTDATA_API_KEY` | Hotdata dashboard, under your workspace's API tokens. |
| `HOTDATA_WORKSPACE_ID` | Hotdata dashboard, in the workspace settings. |

You don't need a database id: the bridge looks it up from the workspace (see
[Configuration](configuration.md#how-the-target-database-is-chosen)).

## 3. Configure

Create a `.env` file in the directory you'll run `bdh` from:

```bash
BRIGHTDATA_API_KEY=...
HOTDATA_API_KEY=...
HOTDATA_WORKSPACE_ID=...
```

Or export the same names as environment variables. Environment variables win over `.env`.
For CI and servers, inject them from your secrets manager instead of writing a file.

`.env` is git-ignored in this repository. If you keep it in another project, make sure that
project ignores it too.

## 4. Check the connection

```bash
bdh databases
```

This lists the databases in your Hotdata workspace and says which one `push` will use. If it
prints an error, see [Troubleshooting](troubleshooting.md#configuration).

## 5. First run

Pick a scraper and check which CSV columns it needs:

```bash
bdh scrapers list --search linkedin
bdh scrapers show gd_l1viktl72bvl7bjuj0
```

Then collect one record and publish it:

```bash
bdh push -d gd_l1viktl72bvl7bjuj0 -q "https://www.linkedin.com/in/some-profile" -t linkedin
```

Start with one or two inputs: every collection is billed by Bright Data.
