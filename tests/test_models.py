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
            "key": ["id"],
        }
    )
    assert isinstance(cfg, TabularConfig)
    assert cfg.ignore_columns == []
    assert cfg.tolerance == {}
    assert cfg.normalization == {}


def test_tabular_full() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "key": ["id", "region"],
            "ignore_columns": ["updated_at"],
            "tolerance": {"amount": 0.01},
            "string_rules": {"name": ["trim", "case_insensitive"]},
            "normalization": {
                "full_name": [
                    {"op": "concat", "args": ["first_name", " ", "last_name"]},
                    {"op": "trim"},
                ],
            },
        }
    )
    assert len(cfg.normalization["full_name"]) == 2


# ---------------------------------------------------------------------------
# Tabular - invalid
# ---------------------------------------------------------------------------


def test_tabular_empty_key_rejected() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "key": [],
            }
        )


def test_tabular_missing_key_rejected() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
            }
        )


def test_tabular_norm_self_ref_rejected() -> None:
    """Generated columns must not be referenced in other normalization args."""
    with pytest.raises(ValidationError, match="references generated column"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "key": ["id"],
                "normalization": {
                    "col_a": [{"op": "upper"}],
                    "col_b": [{"op": "concat", "args": ["col_a", "x"]}],
                },
            }
        )


def test_tabular_invalid_string_rule() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "key": ["id"],
                "string_rules": {"name": ["nope"]},
            }
        )


def test_tabular_invalid_norm_op() -> None:
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "key": ["id"],
                "normalization": {"x": [{"op": "bad_op"}]},
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
