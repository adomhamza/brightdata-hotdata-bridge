# Contributing

## Set up

```bash
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
pre-commit install   # optional: pip install pre-commit first
cp .env.example .env # only needed for live runs; never commit .env
```

## Checks

Every change must leave all of these clean. CI runs the same commands.

```bash
ruff check .
ruff format --check .
mypy
pytest --cov
```

- Tests never call Bright Data or Hotdata. Bright Data is mocked with `respx`; the Hotdata SDK is
  replaced with fakes (see `tests/test_hotdata_client.py` and `tests/test_pipeline.py`).
- Coverage must stay at or above the `fail_under` value in `pyproject.toml`.
- Warnings are treated as errors in tests.

## Conventions

- Public functions and classes need Google-style docstrings (`Args`, `Returns`, `Raises`).
- Raise a `BridgeError` subclass from `errors.py` for expected failures; each one maps to a CLI
  exit code.
- New behaviour is available from both the library and the `bdh` CLI.
- Add a line to `CHANGELOG.md` under "Unreleased".

## Security

- Never commit credentials, API keys, tokens or `.env` files. Configuration comes from the
  environment or a secrets manager.
- Report vulnerabilities privately to the maintainers rather than in a public issue.

## Releasing

1. Update `__version__` in `src/brightdata_hotdata_bridge/__about__.py` and `CHANGELOG.md`.
2. Tag `vX.Y.Z` and push the tag. The release workflow builds, publishes to TestPyPI, then PyPI
   using Trusted Publishing.
