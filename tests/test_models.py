"""Unit tests for Pydantic config models."""

import pytest
from pydantic import TypeAdapter, ValidationError

from reconlify.models import ReconConfig, TabularConfig, TextConfig

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
# Tabular - row_filters validation
# ---------------------------------------------------------------------------


def test_row_filter_equals_valid() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["id"],
            "filters": {
                "row_filters": {
                    "mode": "exclude",
                    "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
                },
            },
        }
    )
    assert len(cfg.filters.row_filters.rules) == 1


def test_row_filter_in_valid() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["id"],
            "filters": {
                "row_filters": {
                    "rules": [{"column": "region", "op": "in", "values": ["US", "EU"]}],
                },
            },
        }
    )
    assert cfg.filters.row_filters.rules[0].values == ["US", "EU"]


def test_row_filter_regex_missing_pattern_rejected() -> None:
    with pytest.raises(ValidationError, match="pattern"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "filters": {
                    "row_filters": {
                        "rules": [{"column": "name", "op": "regex"}],
                    },
                },
            }
        )


def test_row_filter_in_missing_values_rejected() -> None:
    with pytest.raises(ValidationError, match="values"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "filters": {
                    "row_filters": {
                        "rules": [{"column": "region", "op": "in"}],
                    },
                },
            }
        )


def test_row_filter_is_null_with_value_rejected() -> None:
    with pytest.raises(ValidationError, match="must not have"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "filters": {
                    "row_filters": {
                        "rules": [{"column": "x", "op": "is_null", "value": "foo"}],
                    },
                },
            }
        )


def test_row_filter_equals_missing_value_rejected() -> None:
    with pytest.raises(ValidationError, match="value"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "filters": {
                    "row_filters": {
                        "rules": [{"column": "x", "op": "equals"}],
                    },
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


# ---------------------------------------------------------------------------
# Tabular - column_mapping validation
# ---------------------------------------------------------------------------


def test_column_mapping_valid() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["trade_id"],
            "column_mapping": {"trade_id": "id", "amount": "total_amount"},
        }
    )
    assert cfg.column_mapping == {"trade_id": "id", "amount": "total_amount"}


def test_column_mapping_empty_key_rejected() -> None:
    with pytest.raises(ValidationError, match="must not be empty"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "column_mapping": {"": "target_col"},
            }
        )


def test_column_mapping_empty_value_rejected() -> None:
    with pytest.raises(ValidationError, match="must not be empty"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "column_mapping": {"amount": ""},
            }
        )


def test_column_mapping_duplicate_target_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate target"):
        adapter.validate_python(
            {
                "type": "tabular",
                "source": "a.csv",
                "target": "b.csv",
                "keys": ["id"],
                "column_mapping": {"amount": "value", "price": "value"},
            }
        )


def test_column_mapping_defaults_empty() -> None:
    cfg = adapter.validate_python(
        {
            "type": "tabular",
            "source": "a.csv",
            "target": "b.csv",
            "keys": ["id"],
        }
    )
    assert cfg.column_mapping == {}
