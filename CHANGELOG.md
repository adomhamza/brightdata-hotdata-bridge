# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Individual Contributor License Agreement (`CLA.md`), signed with a GitHub login through
  CLA assistant and checked on every pull request.

## [0.1.0]

### Added

- End-to-end pipeline: trigger a Bright Data collection, poll until ready, stream the
  snapshot, validate it, upload it to Hotdata object storage and load it into a table.
- Support for any scraper in Bright Data's catalog, with CSV inputs validated against
  the selected collection method (`collect_by_url` and `discover_by_*`).
- Output validation against the scraper's output schema, with error records set aside
  and a JSON report on mismatch.
- Resumable run records, `bdh replay` for failed writes, and `replace`, `append`,
  `upsert`, `update` and `delete` load modes.
- Target database looked up from the Hotdata workspace when `HOTDATA_DATABASE_ID` is not
  set (the only database, or one chosen by `HOTDATA_DATABASE_NAME`), recorded per run so
  replays publish to the same database. The schema defaults to the database's own.
- `bdh` CLI: `push`, `trigger`, `status`, `replay`, `runs`, `databases`,
  `scrapers list|show`.
