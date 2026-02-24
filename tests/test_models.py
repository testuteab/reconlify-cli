"""Unit tests for Pydantic config models."""

import pytest
from pydantic import TypeAdapter, ValidationError

from reconify.models import ReconConfig, TabularConfig, TextConfig

adapter = TypeAdapter(ReconConfig)


# ---------------------------------------------------------------------------
# Tabular - valid
# ---------------------------------------------------------------------------


def test_tabular_minimal() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["id"],
        }
    )
    assert isinstance(cfg, TabularConfig)
    assert cfg.compare.trim_whitespace is True
    assert cfg.compare.case_insensitive is False
    assert cfg.filters.exclude_keys == []
    assert cfg.csv.delimiter == ","
    assert cfg.sampling.sample_limit == 200


def test_tabular_full() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["id", "region"],
            "compare": {
                "exclude_columns": ["updated_at"],
                "trim_whitespace": True,
                "case_insensitive": True,
            },
            "filters": {
                "exclude_keys": [{"id": "1", "region": "US"}],
            },
            "csv": {"delimiter": "|"},
            "sampling": {"sample_limit": 100},
        }
    )
    assert cfg.compare.case_insensitive is True
    assert len(cfg.filters.exclude_keys) == 1
    assert cfg.csv.delimiter == "|"


# ---------------------------------------------------------------------------
# Tabular - invalid
# ---------------------------------------------------------------------------


def test_tabular_empty_keys_rejected() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": [],
            }
        )


def test_tabular_missing_keys_rejected() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
            }
        )


def test_tabular_invalid_exclude_keys_rejected() -> None:
    """exclude_keys entries must contain exactly all key columns."""
    with pytest.raises(ValidationError, match="exclude_keys"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id", "region"],
                "filters": {
                    "exclude_keys": [{"id": "1"}],  # missing "region"
                },
            }
        )


def test_tabular_exclude_keys_extra_column_rejected() -> None:
    """exclude_keys entries must not have extra columns beyond keys."""
    with pytest.raises(ValidationError, match="exclude_keys"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "filters": {
                    "exclude_keys": [{"id": "1", "extra": "bad"}],
                },
            }
        )


# ---------------------------------------------------------------------------
# Text - valid
# ---------------------------------------------------------------------------


def test_text_minimal() -> None:
    cfg = adapter.validate_python(
        {
            "type": "text",
            "source": "a.txt",
            "target": "b.txt",
        }
    )
    assert isinstance(cfg, TextConfig)
    assert cfg.mode.value == "line_by_line"
    assert cfg.normalize.normalize_newlines is True
    assert cfg.drop_lines_regex == []


def test_text_full() -> None:
    cfg = adapter.validate_python(
        {
            "type": "text",
            "source": "a.txt",
            "target": "b.txt",
            "mode": "unordered_lines",
            "normalize": {
                "ignore_blank_lines": True,
                "trim_lines": True,
                "collapse_whitespace": True,
            },
            "drop_lines_regex": ["^#"],
            "replace_regex": [{"pattern": "\\d+", "replace": "N"}],
        }
    )
    assert cfg.mode.value == "unordered_lines"
    assert cfg.normalize.ignore_blank_lines is True
    assert len(cfg.replace_regex) == 1


# ---------------------------------------------------------------------------
# Text - invalid
# ---------------------------------------------------------------------------


def test_text_invalid_mode() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "text",
                "source": "a.txt",
                "target": "b.txt",
                "mode": "bad_mode",
            }
        )


# ---------------------------------------------------------------------------
# Discriminator
# ---------------------------------------------------------------------------


def test_unknown_type_rejected() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "unknown",
                "source": "a",
                "target": "b",
            }
        )
