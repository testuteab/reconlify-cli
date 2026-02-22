"""YAML config loading and Pydantic validation."""

from pathlib import Path

import yaml
from pydantic import BaseModel


class ReconConfig(BaseModel):
    """Minimal reconciliation config schema."""

    type: str


def load_config(path: Path) -> ReconConfig:
    """Read a YAML file and return a validated ReconConfig."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return ReconConfig.model_validate(raw)
