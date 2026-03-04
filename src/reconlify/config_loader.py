"""YAML config loading and Pydantic validation."""

from __future__ import annotations

import traceback
from pathlib import Path

import yaml
from pydantic import TypeAdapter, ValidationError

from reconlify.models import ReconConfig, TabularConfig, TextConfig

_adapter = TypeAdapter(ReconConfig)


class ConfigLoadError(Exception):
    """Raised when config loading or validation fails.

    Carries all data needed to build an error report consistently.
    """

    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: str,
        raw_yaml: str = "",
        config_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        self.raw_yaml = raw_yaml
        self.config_type = config_type

    def __str__(self) -> str:
        return self.message


def load_config_with_raw(path: Path) -> tuple[TabularConfig | TextConfig, str, str | None]:
    """Read a YAML config file, validate it, and return the config with metadata.

    Returns (cfg, raw_yaml, config_type).

    Raises ``ConfigLoadError`` on any failure (file I/O, YAML parsing, or
    Pydantic validation), carrying the error code, message, details,
    raw_yaml, and best-effort config_type needed for error reporting.
    """
    # A) Read file
    try:
        raw_yaml = path.read_text()
    except FileNotFoundError:
        raise ConfigLoadError(
            code="CONFIG_VALIDATION_ERROR",
            message=f"Config file not found: {path}",
            details=f"FileNotFoundError: {path}",
            raw_yaml="",
            config_type=None,
        ) from None
    except OSError as exc:
        raise ConfigLoadError(
            code="RUNTIME_ERROR",
            message=f"Failed to read config file: {exc}",
            details=traceback.format_exc(),
            raw_yaml="",
            config_type=None,
        ) from None

    # B) Parse YAML
    try:
        raw_dict = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise ConfigLoadError(
            code="CONFIG_VALIDATION_ERROR",
            message="Config validation failed",
            details=str(exc),
            raw_yaml=raw_yaml,
            config_type=None,
        ) from None

    if not isinstance(raw_dict, dict):
        raise ConfigLoadError(
            code="CONFIG_VALIDATION_ERROR",
            message="Config validation failed",
            details="Config file must contain a YAML mapping",
            raw_yaml=raw_yaml,
            config_type=None,
        )

    # C) Validate with Pydantic
    config_type = raw_dict.get("type")

    try:
        cfg = _adapter.validate_python(raw_dict)
    except (ValidationError, ValueError) as exc:
        raise ConfigLoadError(
            code="CONFIG_VALIDATION_ERROR",
            message="Config validation failed",
            details=str(exc),
            raw_yaml=raw_yaml,
            config_type=config_type,
        ) from None

    # D) Success
    return cfg, raw_yaml, config_type


def load_config(path: Path) -> TabularConfig | TextConfig:
    """Read a YAML file and return a validated ReconConfig.

    Raises ``ConfigLoadError`` on any failure.
    """
    cfg, _raw_yaml, _config_type = load_config_with_raw(path)
    return cfg
