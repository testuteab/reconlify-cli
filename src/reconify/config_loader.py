"""YAML config loading and Pydantic validation."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import TypeAdapter, ValidationError

from reconify.models import ReconConfig, TabularConfig, TextConfig

_adapter = TypeAdapter(ReconConfig)


def load_config(path: Path) -> TabularConfig | TextConfig:
    """Read a YAML file and return a validated ReconConfig.

    Raises ``ValidationError`` on schema violations.
    """
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValidationError.from_exception_data(
            title="ReconConfig",
            line_errors=[],
        )
    return _adapter.validate_python(raw)
