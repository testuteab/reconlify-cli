"""Unit tests for the tabular (CSV) comparison engine."""

from __future__ import annotations

import os
import tempfile

import pytest

from reconify.models import TabularConfig
from reconify.tabular_engine import compare_tabular


def _write_csv(content: str) -> str:
    """Write CSV content to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Config validation (Pydantic-level)
# ---------------------------------------------------------------------------


def test_keys_required():
    with pytest.raises(ValueError):
        TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=[])


def test_exclude_keys_must_match_key_columns():
    with pytest.raises(Exception, match="exclude_keys"):
        TabularConfig(
            type="tabular",
            source="a.csv",
            target="b.csv",
            keys=["id"],
            filters={"exclude_keys": [{"wrong_col": "1"}]},
        )


def test_exclude_keys_valid():
    cfg = TabularConfig(
        type="tabular",
        source="a.csv",
        target="b.csv",
        keys=["id"],
        filters={"exclude_keys": [{"id": "1"}]},
    )
    assert len(cfg.filters.exclude_keys) == 1


# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------


def test_exact_match():
    src = _write_csv("id,name,value\n1,Alice,100\n2,Bob,200\n")
    tgt = _write_csv("id,name,value\n1,Alice,100\n2,Bob,200\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["summary"]["missing_in_target"] == 0
    assert result["summary"]["missing_in_source"] == 0
    assert result["summary"]["rows_with_mismatches"] == 0


# ---------------------------------------------------------------------------
# Missing rows
# ---------------------------------------------------------------------------


def test_missing_in_target():
    src = _write_csv("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bob\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["missing_in_target"] == 1
    assert result["summary"]["missing_in_source"] == 0


def test_missing_in_source():
    src = _write_csv("id,name\n1,Alice\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bob\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["missing_in_source"] == 1


# ---------------------------------------------------------------------------
# Value mismatches
# ---------------------------------------------------------------------------


def test_value_mismatch():
    src = _write_csv("id,name,value\n1,Alice,100\n2,Bob,200\n")
    tgt = _write_csv("id,name,value\n1,Alice,999\n2,Bob,200\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1
    assert result["summary"]["mismatched_cells"] == 1


def test_multiple_column_mismatches():
    src = _write_csv("id,name,value\n1,Alice,100\n")
    tgt = _write_csv("id,name,value\n1,Bob,200\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1
    assert result["summary"]["mismatched_cells"] == 2


# ---------------------------------------------------------------------------
# Line numbers
# ---------------------------------------------------------------------------


def test_line_numbers_in_samples():
    src = _write_csv("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bobby\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1

    # Missing in target: row 3 (line 4 including header)
    missing_tgt = result["samples"]["missing_in_target"]
    assert len(missing_tgt) == 1
    assert missing_tgt[0]["line_number_source"] == 3

    # Value mismatch: row 2
    mismatches = result["samples"]["value_mismatches"]
    assert len(mismatches) == 1
    assert mismatches[0]["line_number_source"] == 2
    assert mismatches[0]["line_number_target"] == 2


# ---------------------------------------------------------------------------
# Duplicate keys
# ---------------------------------------------------------------------------


def test_duplicate_keys_error():
    src = _write_csv("id,name\n1,Alice\n1,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bob\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert result["error"]["code"] == "DUPLICATE_KEYS"


def test_duplicate_keys_in_target():
    src = _write_csv("id,name\n1,Alice\n2,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n1,Bob\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert result["error"]["code"] == "DUPLICATE_KEYS"


# ---------------------------------------------------------------------------
# Exclude keys filter
# ---------------------------------------------------------------------------


def test_exclude_keys_suppresses_mismatch():
    src = _write_csv("id,name\n1,Alice\n2,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bobby\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={"exclude_keys": [{"id": "2"}]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["details"]["filters_applied"]["source_excluded_rows"] == 1
    assert result["details"]["filters_applied"]["target_excluded_rows"] == 1


def test_exclude_keys_before_duplicate_validation():
    """Excluding a duplicate key should prevent DUPLICATE_KEYS error."""
    src = _write_csv("id,name\n1,Alice\n1,Bob\n2,Charlie\n")
    tgt = _write_csv("id,name\n2,Charlie\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={"exclude_keys": [{"id": "1"}]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["details"]["filters_applied"]["source_excluded_rows"] == 2


def test_exclude_keys_samples():
    src = _write_csv("id,name\n1,Alice\n2,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bob\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={"exclude_keys": [{"id": "2"}]},
    )
    result, _exit_code = compare_tabular(cfg)
    excluded = result["samples"]["excluded"]
    assert len(excluded) >= 1
    assert all(e["reason"] == "exclude_keys" for e in excluded)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def test_trim_whitespace():
    src = _write_csv("id,name\n1, Alice \n")
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"trim_whitespace": True},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_case_insensitive():
    src = _write_csv("id,name\n1,ALICE\n")
    tgt = _write_csv("id,name\n1,alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"case_insensitive": True},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_normalize_nulls():
    src = _write_csv("id,name\n1,NULL\n")
    tgt = _write_csv("id,name\n1,\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"normalize_nulls": ["", "NULL"]},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


# ---------------------------------------------------------------------------
# Column filtering
# ---------------------------------------------------------------------------


def test_include_columns():
    src = _write_csv("id,name,value\n1,Alice,100\n")
    tgt = _write_csv("id,name,value\n1,Alice,999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"include_columns": ["name"]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["details"]["compared_columns"] == ["name"]


def test_exclude_columns():
    src = _write_csv("id,name,value\n1,Alice,100\n")
    tgt = _write_csv("id,name,value\n1,Alice,999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"exclude_columns": ["value"]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert "value" not in result["details"]["compared_columns"]


# ---------------------------------------------------------------------------
# Composite keys
# ---------------------------------------------------------------------------


def test_composite_key():
    src = _write_csv("id,region,name\n1,US,Alice\n1,EU,Bob\n")
    tgt = _write_csv("id,region,name\n1,US,Alice\n1,EU,Bobby\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id", "region"],
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


# ---------------------------------------------------------------------------
# Deterministic ordering
# ---------------------------------------------------------------------------


def test_samples_ordered_by_keys():
    src = _write_csv("id,name\n3,C\n1,A\n2,B\n")
    tgt = _write_csv("id,name\n3,CX\n1,AX\n2,BX\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, _exit_code = compare_tabular(cfg)
    mismatches = result["samples"]["value_mismatches"]
    key_order = [m["key"]["id"] for m in mismatches]
    assert key_order == sorted(key_order)


# ---------------------------------------------------------------------------
# Report structure
# ---------------------------------------------------------------------------


def test_report_structure():
    src = _write_csv("id,name\n1,Alice\n")
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, _exit_code = compare_tabular(cfg)

    assert "summary" in result
    assert "details" in result
    assert "samples" in result
    assert result["details"]["format"] == "csv"
    assert result["details"]["keys"] == ["id"]
    assert "compared_columns" in result["details"]
    assert "filters_applied" in result["details"]
    assert result["summary"]["comparison_time_seconds"] > 0


# ---------------------------------------------------------------------------
# File not found
# ---------------------------------------------------------------------------


def test_missing_source_file():
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(type="tabular", source="/nonexistent.csv", target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert result["error"]["code"] == "RUNTIME_ERROR"


# ---------------------------------------------------------------------------
# Column stats
# ---------------------------------------------------------------------------


def test_column_stats():
    src = _write_csv("id,name,value\n1,Alice,100\n2,Bob,200\n")
    tgt = _write_csv("id,name,value\n1,Alice,999\n2,Bobby,200\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        output={"include_column_stats": True},
    )
    result, _exit_code = compare_tabular(cfg)
    assert "column_stats" in result
    assert result["column_stats"]["value"]["mismatched_count"] == 1
    assert result["column_stats"]["name"]["mismatched_count"] == 1
