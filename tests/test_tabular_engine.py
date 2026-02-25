"""Unit tests for the tabular (CSV) comparison engine."""

from __future__ import annotations

import os
import tempfile

import pytest

from reconify.models import RowFilterRule, TabularConfig
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
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows"] == 1
    assert fa["target_excluded_rows"] == 1
    assert fa["source_excluded_rows_exclude_keys"] == 1
    assert fa["target_excluded_rows_exclude_keys"] == 1
    # read_rows = raw, source_rows = post-filter
    assert result["details"]["read_rows_source"] == 2
    assert result["summary"]["source_rows"] == 1
    expected = result["details"]["read_rows_source"] - fa["source_excluded_rows"]
    assert expected == result["summary"]["source_rows"]


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
    fa = result["details"]["filters_applied"]
    assert "row_filters" not in fa
    assert fa["source_excluded_rows_row_filters"] == 0
    assert fa["target_excluded_rows_row_filters"] == 0
    assert result["summary"]["comparison_time_seconds"] > 0
    # read_rows fields present and equal summary rows (no filtering)
    assert result["details"]["read_rows_source"] == 1
    assert result["details"]["read_rows_target"] == 1
    assert result["details"]["read_rows_source"] == result["summary"]["source_rows"]


# ---------------------------------------------------------------------------
# File not found
# ---------------------------------------------------------------------------


def test_missing_source_file():
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(type="tabular", source="/nonexistent.csv", target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert result["error"]["code"] == "RUNTIME_ERROR"
    assert result["details"]["read_rows_source"] == 0
    assert result["details"]["read_rows_target"] == 0


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
    assert "column_stats" in result["details"]
    assert result["details"]["column_stats"]["value"]["mismatched_count"] == 1
    assert result["details"]["column_stats"]["name"]["mismatched_count"] == 1


def test_column_stats_disabled():
    """column_stats is present as {} when include_column_stats is false."""
    src = _write_csv("id,name\n1,Alice\n2,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bobby\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        output={"include_column_stats": False},
    )
    result, _exit_code = compare_tabular(cfg)
    assert "column_stats" in result["details"]
    assert result["details"]["column_stats"] == {}


def test_column_stats_in_error_result():
    """column_stats is present as {} in error result details."""
    cfg = TabularConfig(type="tabular", source="/nonexistent.csv", target="/also.csv", keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert "column_stats" in result["details"]
    assert result["details"]["column_stats"] == {}


# ---------------------------------------------------------------------------
# Projection optimization
# ---------------------------------------------------------------------------


def test_projection_drops_extra_columns():
    """Wide CSV with extra source-only columns produces identical comparison results."""
    src = _write_csv(
        "id,name,value,extra_a,extra_b,extra_c\n"
        "1,Alice,100,x,y,z\n"
        "2,Bob,200,x,y,z\n"
        "3,Charlie,300,x,y,z\n"
    )
    tgt = _write_csv("id,name,value\n1,Alice,100\n2,Bob,999\n")
    cfg = TabularConfig(type="tabular", source=src, target=tgt, keys=["id"])
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    # compared_columns should only contain common non-key columns
    assert sorted(result["details"]["compared_columns"]) == ["name", "value"]
    assert result["summary"]["missing_in_target"] == 1
    assert result["summary"]["rows_with_mismatches"] == 1
    # Source-only columns (extra_a/b/c) should NOT appear in missing sample rows
    missing = result["samples"]["missing_in_target"]
    assert len(missing) == 1
    assert "extra_a" not in missing[0].get("row", {})


# ---------------------------------------------------------------------------
# Row filters - exclude mode
# ---------------------------------------------------------------------------


def test_row_filters_exclude_both():
    """Exclude rows where status=CANCELLED from both sides."""
    src = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,200\n3,ACTIVE,300\n")
    tgt = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,999\n3,ACTIVE,300\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    fa = result["details"]["filters_applied"]
    assert fa["row_filters"] == {"count": 1, "apply_to": "both", "mode": "exclude"}
    assert fa["source_excluded_rows_row_filters"] == 1
    assert fa["target_excluded_rows_row_filters"] == 1
    # read_rows = 3, excluded = 1, source_rows = 2
    assert result["details"]["read_rows_source"] == 3
    assert result["summary"]["source_rows"] == 2
    expected = result["details"]["read_rows_source"] - fa["source_excluded_rows"]
    assert expected == result["summary"]["source_rows"]


def test_row_filters_exclude_with_samples():
    """Excluded row samples should have reason='row_filters'."""
    src = _write_csv("id,status\n1,ACTIVE\n2,CANCELLED\n")
    tgt = _write_csv("id,status\n1,ACTIVE\n2,CANCELLED\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    excluded = result["samples"]["excluded"]
    rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
    assert len(rf_excluded) >= 1
    assert all(e["key"]["id"] == "2" for e in rf_excluded)


# ---------------------------------------------------------------------------
# Row filters - include mode
# ---------------------------------------------------------------------------


def test_row_filters_include_mode():
    """Include mode keeps only rows matching the rules."""
    src = _write_csv("id,region,value\n1,US,100\n2,EU,200\n3,US,300\n")
    tgt = _write_csv("id,region,value\n1,US,100\n2,EU,999\n3,US,300\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "include",
                "rules": [{"column": "region", "op": "equals", "value": "US"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    # Only US rows remain; EU row with mismatch is excluded
    assert exit_code == 0
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 1
    assert fa["target_excluded_rows_row_filters"] == 1
    assert fa["row_filters"]["mode"] == "include"


# ---------------------------------------------------------------------------
# Row filters - apply_to variants
# ---------------------------------------------------------------------------


def test_row_filters_apply_to_source_only():
    """apply_to=source filters only source; target keeps all rows."""
    src = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,200\n")
    tgt = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "apply_to": "source",
                "mode": "exclude",
                "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    # Source has id=2 removed; target still has it -> missing_in_source=1
    assert exit_code == 1
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 1
    assert fa["target_excluded_rows_row_filters"] == 0
    assert result["summary"]["missing_in_source"] == 1


def test_row_filters_apply_to_target_only():
    """apply_to=target filters only target."""
    src = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,200\n")
    tgt = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "apply_to": "target",
                "mode": "exclude",
                "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    # Target has id=2 removed; source still has it -> missing_in_target=1
    assert exit_code == 1
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 0
    assert fa["target_excluded_rows_row_filters"] == 1
    assert result["summary"]["missing_in_target"] == 1


# ---------------------------------------------------------------------------
# Row filters - operator tests
# ---------------------------------------------------------------------------


def test_row_filters_op_not_equals():
    src = _write_csv("id,status\n1,ACTIVE\n2,CANCELLED\n")
    tgt = _write_csv("id,status\n1,ACTIVE\n2,CANCELLED\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "status", "op": "not_equals", "value": "ACTIVE"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 1


def test_row_filters_op_in():
    src = _write_csv("id,region\n1,US\n2,EU\n3,APAC\n")
    tgt = _write_csv("id,region\n1,US\n2,EU\n3,APAC\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "include",
                "rules": [{"column": "region", "op": "in", "values": ["US", "EU"]}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    fa = result["details"]["filters_applied"]
    # APAC row excluded from both
    assert fa["source_excluded_rows_row_filters"] == 1
    assert fa["target_excluded_rows_row_filters"] == 1


def test_row_filters_op_contains():
    src = _write_csv("id,name\n1,Alice Smith\n2,Bob Jones\n")
    tgt = _write_csv("id,name\n1,Alice Smith\n2,Bob Jones\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "name", "op": "contains", "value": "Jones"}],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 1


def test_row_filters_op_regex():
    src = _write_csv("id,code\n1,ABC-123\n2,XYZ-456\n3,ABC-789\n")
    tgt = _write_csv("id,code\n1,ABC-123\n2,XYZ-456\n3,ABC-789\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "include",
                "rules": [{"column": "code", "op": "regex", "pattern": "^ABC"}],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    fa = result["details"]["filters_applied"]
    # XYZ-456 excluded
    assert fa["source_excluded_rows_row_filters"] == 1


def test_row_filters_op_is_null():
    # DuckDB read_csv_auto with all_varchar=true treats empty fields as NULL
    src = _write_csv("id,note\n1,hello\n2,\n")
    tgt = _write_csv("id,note\n1,hello\n2,\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "note", "op": "is_null"}],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    fa = result["details"]["filters_applied"]
    # Empty CSV field is NULL in DuckDB, so is_null matches row 2
    assert fa["source_excluded_rows_row_filters"] == 1


def test_row_filters_op_not_null():
    src = _write_csv("id,note\n1,hello\n2,world\n")
    tgt = _write_csv("id,note\n1,hello\n2,world\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "include",
                "rules": [{"column": "note", "op": "not_null"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    # All rows have non-null note, so nothing excluded
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 0


# ---------------------------------------------------------------------------
# Row filters - case_insensitive/trim per-rule
# ---------------------------------------------------------------------------


def test_row_filters_case_insensitive_per_rule():
    """Per-rule case_insensitive should work independently of compare config."""
    src = _write_csv("id,status\n1,Active\n2,CANCELLED\n")
    tgt = _write_csv("id,status\n1,Active\n2,CANCELLED\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"case_insensitive": False},
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [
                    {
                        "column": "status",
                        "op": "equals",
                        "value": "cancelled",
                        "case_insensitive": True,
                    }
                ],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    fa = result["details"]["filters_applied"]
    assert fa["source_excluded_rows_row_filters"] == 1


# ---------------------------------------------------------------------------
# Row filters - missing column error
# ---------------------------------------------------------------------------


def test_row_filters_missing_column():
    src = _write_csv("id,name\n1,Alice\n")
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "rules": [{"column": "nonexistent", "op": "equals", "value": "x"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 2
    assert result["error"]["code"] == "INVALID_ROW_FILTERS"


# ---------------------------------------------------------------------------
# Row filters - combined with exclude_keys
# ---------------------------------------------------------------------------


def test_row_filters_combined_with_exclude_keys():
    """Both exclude_keys and row_filters should work together."""
    src = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,200\n3,ACTIVE,300\n")
    tgt = _write_csv("id,status,value\n1,ACTIVE,100\n2,CANCELLED,999\n3,ACTIVE,999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "exclude_keys": [{"id": "3"}],
            "row_filters": {
                "mode": "exclude",
                "rules": [{"column": "status", "op": "equals", "value": "CANCELLED"}],
            },
        },
    )
    result, exit_code = compare_tabular(cfg)
    # id=3 excluded by exclude_keys, id=2 excluded by row_filters
    # Only id=1 remains, which matches -> exit 0
    assert exit_code == 0
    fa = result["details"]["filters_applied"]
    assert fa["exclude_keys_count"] == 1
    assert fa["source_excluded_rows_exclude_keys"] == 1
    assert fa["source_excluded_rows_row_filters"] == 1
    assert fa["source_excluded_rows"] == 2  # total from both
    # read_rows = 3, excluded = 2 (1 EK + 1 RF), source_rows = 1
    assert result["details"]["read_rows_source"] == 3
    assert result["summary"]["source_rows"] == 1
    expected = result["details"]["read_rows_source"] - fa["source_excluded_rows"]
    assert expected == result["summary"]["source_rows"]


# ---------------------------------------------------------------------------
# Row filters - no rules = no-op
# ---------------------------------------------------------------------------


def test_row_filters_empty_rules_noop():
    """Empty rules list should not filter anything."""
    src = _write_csv("id,name\n1,Alice\n2,Bob\n")
    tgt = _write_csv("id,name\n1,Alice\n2,Bobby\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {"rules": []},
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1
    assert "row_filters" not in result["details"]["filters_applied"]


# ---------------------------------------------------------------------------
# Row filters - multiple rules (AND)
# ---------------------------------------------------------------------------


def test_row_filters_multiple_rules_and():
    """Multiple rules are combined with AND."""
    src = _write_csv("id,status,region\n1,ACTIVE,US\n2,CANCELLED,US\n3,CANCELLED,EU\n")
    tgt = _write_csv("id,status,region\n1,ACTIVE,US\n2,CANCELLED,US\n3,CANCELLED,EU\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        filters={
            "row_filters": {
                "mode": "exclude",
                "rules": [
                    {"column": "status", "op": "equals", "value": "CANCELLED"},
                    {"column": "region", "op": "equals", "value": "US"},
                ],
            },
        },
    )
    result, _exit_code = compare_tabular(cfg)
    fa = result["details"]["filters_applied"]
    # Only id=2 matches BOTH (CANCELLED AND US)
    assert fa["source_excluded_rows_row_filters"] == 1


# ---------------------------------------------------------------------------
# Row filters - SQL builder determinism
# ---------------------------------------------------------------------------


def test_row_filter_builder_deterministic():
    """SQL builder should produce consistent output."""
    from reconify.tabular_engine import _build_row_filter_predicate

    rules = [
        RowFilterRule(column="status", op="equals", value="CANCELLED"),
        RowFilterRule(column="region", op="in", values=["US", "EU"]),
    ]
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])

    pred1, params1 = _build_row_filter_predicate(rules, cfg)
    pred2, params2 = _build_row_filter_predicate(rules, cfg)

    assert pred1 == pred2
    assert params1 == params2
    assert "?" in pred1
    assert len(params1) == 3  # "CANCELLED", "US", "EU"


def test_row_filter_builder_uses_param_binding():
    """Values should be passed as params, not interpolated into SQL."""
    from reconify.tabular_engine import _build_row_filter_predicate

    rules = [
        RowFilterRule(column="name", op="equals", value="O'Brien"),
    ]
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])

    pred, params = _build_row_filter_predicate(rules, cfg)
    # The value should NOT appear in the SQL string
    assert "O'Brien" not in pred
    assert "O'Brien" in params[0]  # lowered or not based on case_insensitive


# ---------------------------------------------------------------------------
# ignore_columns
# ---------------------------------------------------------------------------


def test_ignore_columns():
    """ignore_columns should exclude columns from comparison."""
    src = _write_csv("id,name,value,notes\n1,Alice,100,src note\n")
    tgt = _write_csv("id,name,value,notes\n1,Alice,100,tgt note\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        ignore_columns=["notes"],
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert "notes" not in result["details"]["compared_columns"]
    assert "name" in result["details"]["compared_columns"]
    assert "value" in result["details"]["compared_columns"]


def test_ignore_columns_without_match():
    """ignore_columns with non-existent column should not error."""
    src = _write_csv("id,name\n1,Alice\n")
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        ignore_columns=["nonexistent"],
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert "name" in result["details"]["compared_columns"]


# ---------------------------------------------------------------------------
# Tolerance
# ---------------------------------------------------------------------------


def test_tolerance_within_range():
    """Values within tolerance should not cause mismatches."""
    src = _write_csv("id,amount\n1,100.001\n2,200.005\n")
    tgt = _write_csv("id,amount\n1,100.009\n2,200.000\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        tolerance={"amount": 0.01},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["summary"]["rows_with_mismatches"] == 0


def test_tolerance_exceeds_range():
    """Values outside tolerance should cause mismatches."""
    src = _write_csv("id,amount\n1,100.0\n2,200.0\n")
    tgt = _write_csv("id,amount\n1,100.5\n2,200.0\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        tolerance={"amount": 0.1},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


def test_tolerance_non_numeric_fallback():
    """Non-numeric values fall back to string comparison under tolerance."""
    src = _write_csv("id,amount\n1,abc\n")
    tgt = _write_csv("id,amount\n1,abc\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        tolerance={"amount": 0.01},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_tolerance_non_numeric_mismatch():
    """Non-numeric different values should mismatch even with tolerance."""
    src = _write_csv("id,amount\n1,abc\n")
    tgt = _write_csv("id,amount\n1,xyz\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        tolerance={"amount": 0.01},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


def test_tolerance_negative_value_rejected():
    """Negative tolerance values should be rejected by the model validator."""
    with pytest.raises(ValueError, match="tolerance"):
        TabularConfig(
            type="tabular",
            source="a.csv",
            target="b.csv",
            keys=["id"],
            tolerance={"col": -0.1},
        )


# ---------------------------------------------------------------------------
# String rules
# ---------------------------------------------------------------------------


def test_string_rules_trim():
    """Per-column trim rule trims values before comparison."""
    src = _write_csv("id,name\n1, Alice \n")
    tgt = _write_csv("id,name\n1,Alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"trim_whitespace": False},
        string_rules={"name": ["trim"]},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_string_rules_case_insensitive():
    """Per-column case_insensitive rule lowercases values before comparison."""
    src = _write_csv("id,name\n1,ALICE\n")
    tgt = _write_csv("id,name\n1,alice\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        compare={"case_insensitive": False},
        string_rules={"name": ["case_insensitive"]},
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_string_rules_contains():
    """Per-column contains rule checks bidirectional substring containment."""
    src = _write_csv("id,desc\n1,Hello World\n2,Python\n")
    tgt = _write_csv("id,desc\n1,World\n2,Python programming\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        string_rules={"desc": ["contains"]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["summary"]["rows_with_mismatches"] == 0


def test_string_rules_contains_no_match():
    """Contains rule should detect non-containment as mismatch."""
    src = _write_csv("id,desc\n1,Hello\n")
    tgt = _write_csv("id,desc\n1,World\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        string_rules={"desc": ["contains"]},
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


def test_string_rules_regex_extract():
    """regex_extract rule extracts groups before comparison."""
    src = _write_csv("id,code\n1,ABC-123\n2,DEF-456\n")
    tgt = _write_csv("id,code\n1,XYZ-123\n2,QRS-456\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        string_rules={
            "code": [{"regex_extract": {"pattern": "(\\d+)", "group": 1}}],
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert result["summary"]["rows_with_mismatches"] == 0


def test_string_rules_regex_extract_mismatch():
    """regex_extract should detect different extracted values."""
    src = _write_csv("id,code\n1,ABC-123\n")
    tgt = _write_csv("id,code\n1,XYZ-999\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        string_rules={
            "code": [{"regex_extract": {"pattern": "(\\d+)", "group": 1}}],
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


# ---------------------------------------------------------------------------
# Normalization virtual columns
# ---------------------------------------------------------------------------


def test_normalization_concat():
    """Normalization concat creates a virtual column matching target."""
    src = _write_csv("id,first,last,email\n1,Alice,Smith,a@t.com\n")
    tgt = _write_csv("id,full_name,email\n1,Alice Smith,a@t.com\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        normalization={
            "full_name": [{"op": "concat", "args": ["first", " ", "last"]}],
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 0
    assert "full_name" in result["details"]["compared_columns"]
    assert "email" in result["details"]["compared_columns"]


def test_normalization_concat_mismatch():
    """Normalization virtual column mismatch should be detected."""
    src = _write_csv("id,first,last\n1,Alice,Smith\n")
    tgt = _write_csv("id,full_name\n1,Bob Jones\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        normalization={
            "full_name": [{"op": "concat", "args": ["first", " ", "last"]}],
        },
    )
    result, exit_code = compare_tabular(cfg)
    assert exit_code == 1
    assert result["summary"]["rows_with_mismatches"] == 1


def test_normalization_upper():
    """Normalization upper should uppercase the column."""
    src = _write_csv("id,name\n1,alice\n")
    tgt = _write_csv("id,name_upper\n1,ALICE\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        normalization={
            "name_upper": [{"op": "upper", "args": ["name"]}],
        },
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_normalization_add():
    """Normalization add should sum two numeric columns."""
    src = _write_csv("id,a,b\n1,10,20\n")
    tgt = _write_csv("id,total\n1,30.0\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        normalization={
            "total": [{"op": "add", "args": ["a", "b"]}],
        },
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_normalization_pipeline():
    """Multi-step normalization pipeline should chain operations."""
    src = _write_csv("id,first,last\n1,alice,smith\n")
    tgt = _write_csv("id,full_name\n1,ALICE SMITH\n")
    cfg = TabularConfig(
        type="tabular",
        source=src,
        target=tgt,
        keys=["id"],
        normalization={
            "full_name": [
                {"op": "concat", "args": ["first", " ", "last"]},
                {"op": "upper"},
            ],
        },
    )
    _result, exit_code = compare_tabular(cfg)
    assert exit_code == 0


def test_normalization_cross_ref_rejected():
    """Normalization columns cannot reference other generated columns."""
    with pytest.raises(ValueError, match="generated column"):
        TabularConfig(
            type="tabular",
            source="a.csv",
            target="b.csv",
            keys=["id"],
            normalization={
                "a": [{"op": "upper", "args": ["name"]}],
                "b": [{"op": "upper", "args": ["a"]}],
            },
        )


def test_normalization_empty_pipeline_rejected():
    """Empty normalization pipeline should be rejected."""
    with pytest.raises(ValueError, match="pipeline must not be empty"):
        TabularConfig(
            type="tabular",
            source="a.csv",
            target="b.csv",
            keys=["id"],
            normalization={"col": []},
        )


# ---------------------------------------------------------------------------
# Normalization pipeline SQL builder
# ---------------------------------------------------------------------------


def test_norm_step_map():
    from reconify.models import NormStep
    from reconify.tabular_engine import _build_norm_step_sql

    step = NormStep(op="map", args=["status", "A", "Active", "I", "Inactive"])
    sql = _build_norm_step_sql(step, None, {"status"})
    assert "CASE" in sql
    assert "WHEN" in sql
    assert "Active" in sql


def test_norm_step_substr():
    from reconify.models import NormStep
    from reconify.tabular_engine import _build_norm_step_sql

    step = NormStep(op="substr", args=["name", 1, 3])
    sql = _build_norm_step_sql(step, None, {"name"})
    assert "SUBSTR" in sql


def test_norm_step_round():
    from reconify.models import NormStep
    from reconify.tabular_engine import _build_norm_step_sql

    step = NormStep(op="round", args=["amount", 2])
    sql = _build_norm_step_sql(step, None, {"amount"})
    assert "ROUND" in sql
    assert "TRY_CAST" in sql


def test_norm_step_coalesce():
    from reconify.models import NormStep
    from reconify.tabular_engine import _build_norm_step_sql

    step = NormStep(op="coalesce", args=["a", "b", "default"])
    sql = _build_norm_step_sql(step, None, {"a", "b"})
    assert "COALESCE" in sql


def test_norm_step_with_prev_expr():
    from reconify.models import NormStep
    from reconify.tabular_engine import _build_norm_step_sql

    step = NormStep(op="upper", args=[])
    sql = _build_norm_step_sql(step, '"name"', {"name"})
    assert sql == 'UPPER("name")'
