"""Settings shared by the library and the CLI.

Values come from keyword overrides, then environment variables, then a ``.env`` file in
the working directory. Credentials are held as :class:`pydantic.SecretStr` so they never
appear in logs, reprs or tracebacks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar

from pydantic import AliasChoices, Field, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from brightdata_hotdata_bridge.errors import ConfigurationError

T = TypeVar("T")

DEFAULT_CATALOG_URL = "https://docs.brightdata.com/scrapers-full.json"


def _env(name: str) -> AliasChoices:
    return AliasChoices(name, name.lower())


class Settings(BaseSettings):
    """Runtime configuration for Bright Data, Hotdata and the local pipeline state."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
        populate_by_name=True,
        frozen=True,
    )

    brightdata_api_key: SecretStr | None = Field(
        default=None, validation_alias=_env("BRIGHTDATA_API_KEY")
    )
    brightdata_base_url: str = Field(
        default="https://api.brightdata.com", validation_alias=_env("BRIGHTDATA_BASE_URL")
    )

    hotdata_api_key: SecretStr | None = Field(
        default=None, validation_alias=_env("HOTDATA_API_KEY")
    )
    hotdata_workspace_id: str | None = Field(
        default=None, min_length=1, validation_alias=_env("HOTDATA_WORKSPACE_ID")
    )
    hotdata_database_id: str | None = Field(
        default=None, min_length=1, validation_alias=_env("HOTDATA_DATABASE_ID")
    )
    hotdata_database_name: str | None = Field(
        default=None, min_length=1, validation_alias=_env("HOTDATA_DATABASE_NAME")
    )
    hotdata_schema: str | None = Field(
        default=None, min_length=1, validation_alias=_env("HOTDATA_SCHEMA")
    )
    hotdata_host: str | None = Field(default=None, validation_alias=_env("HOTDATA_HOST"))

    poll_interval_seconds: float = Field(
        default=10.0, gt=0, validation_alias=_env("BDH_POLL_INTERVAL_SECONDS")
    )
    poll_timeout_seconds: float = Field(
        default=3600.0, gt=0, validation_alias=_env("BDH_POLL_TIMEOUT_SECONDS")
    )
    load_timeout_seconds: float = Field(
        default=1800.0, gt=0, validation_alias=_env("BDH_LOAD_TIMEOUT_SECONDS")
    )
    http_timeout_seconds: float = Field(
        default=60.0, gt=0, validation_alias=_env("BDH_HTTP_TIMEOUT_SECONDS")
    )

    state_dir: Path = Field(default=Path(".bdh"), validation_alias=_env("BDH_STATE_DIR"))
    catalog_url: str = Field(default=DEFAULT_CATALOG_URL, validation_alias=_env("BDH_CATALOG_URL"))
    catalog_file: Path | None = Field(default=None, validation_alias=_env("BDH_CATALOG_FILE"))
    catalog_ttl_hours: float = Field(
        default=24.0, ge=0, validation_alias=_env("BDH_CATALOG_TTL_HOURS")
    )


def get_settings(*, env_file: Path | None = None, **overrides: Any) -> Settings:
    """Build validated settings from overrides, the environment and a ``.env`` file.

    Args:
        env_file: Alternative ``.env`` path. Defaults to ``.env`` in the working directory.
        **overrides: Field values that take precedence over the environment, keyed by
            field name (for example ``hotdata_schema="analytics"``).

    Returns:
        The validated, immutable settings.

    Raises:
        ConfigurationError: A required value is missing or a value is invalid. The message
            names the environment variables to set.

    Example:
        >>> settings = get_settings(poll_interval_seconds=5)  # doctest: +SKIP
    """
    init_kwargs: dict[str, Any] = dict(overrides)
    if env_file is not None:
        init_kwargs["_env_file"] = env_file
    try:
        return Settings(**init_kwargs)
    except ValidationError as exc:
        problems = [_describe_problem(error) for error in exc.errors()]
        raise ConfigurationError(
            "Invalid configuration. Set these in the environment or .env: " + "; ".join(problems)
        ) from exc


def require_setting(value: T | None, env_name: str) -> T:
    """Return a setting that the current operation cannot run without.

    Credentials are optional at load time so that commands which do not need them
    (such as browsing the scraper catalog) still work; this enforces them at use.

    Args:
        value: The setting's value.
        env_name: Environment variable that provides it, named in the error.

    Returns:
        The value, unchanged.

    Raises:
        ConfigurationError: The value is not set.
    """
    if value is None:
        raise ConfigurationError(f"{env_name} is required; set it in the environment or .env")
    return value


def require_publish_settings(settings: Settings) -> None:
    """Check that every credential needed to collect and publish is present.

    Called before a collection is triggered, so a paid Bright Data job never starts when
    the results could not be written anywhere. The target database is not required here:
    it is looked up from the workspace when ``HOTDATA_DATABASE_ID`` is not set.

    Args:
        settings: Settings to check.

    Raises:
        ConfigurationError: Names every missing variable.
    """
    required = {
        "BRIGHTDATA_API_KEY": settings.brightdata_api_key,
        "HOTDATA_API_KEY": settings.hotdata_api_key,
        "HOTDATA_WORKSPACE_ID": settings.hotdata_workspace_id,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ConfigurationError(
            f"Missing configuration: {', '.join(missing)}. Set them in the environment or .env"
        )


def _describe_problem(error: Any) -> str:
    location = ".".join(str(part) for part in error.get("loc", ())) or "settings"
    name = _env_name(location)
    if error.get("type") == "missing":
        return f"{name} is required"
    return f"{name}: {error.get('msg', 'invalid value')}"


def _env_name(location: str) -> str:
    field = Settings.model_fields.get(location.lower())
    alias = field.validation_alias if field else None
    if isinstance(alias, AliasChoices) and isinstance(alias.choices[0], str):
        return alias.choices[0]
    return location.upper()
