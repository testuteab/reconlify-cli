"""End-to-end tests for the column_mapping feature (tabular engine)."""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Shared assertion helpers (reused from test_tabular_e2e)
# ---------------------------------------------------------------------------

_SUMMARY_INT_FIELDS = (
    "source_rows",
    "target_rows",
    "missing_in_target",
    "missing_in_source",
    "rows_with_mismatches",
    "mismatched_cells",
)


def _assert_tabular_base(report):
    """Validate common tabular report structure."""
    assert report["type"] == "tabular"
    s = report["summary"]
    for field in _SUMMARY_INT_FIELDS:
        assert field in s, f"summary.{field} missing"
        assert isinstance(s[field], int), f"summary.{field} should be int"


def _assert_read_rows_relationship(report):
    """Assert read_rows - excluded_rows == source_rows (post-filter)."""
    d = report["details"]
    s = report["summary"]
    fa = d["filters_applied"]
    assert d["read_rows_source"] - fa["source_excluded_rows"] == s["source_rows"]
    assert d["read_rows_target"] - fa["target_excluded_rows"] == s["target_rows"]


# ---------------------------------------------------------------------------
# Test case 1 — mapped key join + mapped non-key mismatch
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingBasic:
    def test_mapped_key_join_and_mismatch(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_mapping_basic_exit1")
        assert exit_code == 1
        _assert_tabular_base(report)

        # Key uses logical/source-side name
        assert report["details"]["keys"] == ["trade_id"]

        # Compared columns are logical names, not physical target names
        compared = report["details"]["compared_columns"]
        assert "amount" in compared
        assert "customer_name" in compared
        assert "total_amount" not in compared
        assert "client_name" not in compared

        # Correct row counts
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["missing_in_target"] == 0
        assert report["summary"]["missing_in_source"] == 0

        # Exactly one row mismatch (trade_id=2, amount 200 vs 205)
        assert report["summary"]["rows_with_mismatches"] == 1

        # Mismatch sample uses logical column name
        mismatches = report["samples"]["value_mismatches"]
        assert len(mismatches) == 1
        m = mismatches[0]
        assert m["key"]["trade_id"] == "2"
        assert "amount" in m["columns"]
        assert m["columns"]["amount"]["source"] == "200.00"
        assert m["columns"]["amount"]["target"] == "205.00"

        # Report details expose effective column_mapping
        cm = report["details"].get("column_mapping", {})
        assert cm.get("trade_id") == "id"
        assert cm.get("amount") == "total_amount"
        assert cm.get("customer_name") == "client_name"

        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# Test case 2 — string rules on a mapped column
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingStringRules:
    def test_mapping_plus_trim_case(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_mapping_string_rules_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["details"]["keys"] == ["trade_id"]

        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# Test case 3 — tolerance on a mapped numeric column
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingTolerance:
    def test_tolerance_on_mapped_column(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_mapping_tolerance_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["details"]["keys"] == ["trade_id"]
        assert "amount" in report["details"]["compared_columns"]

        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# Test case 4 — invalid mapping target column
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingInvalidTarget:
    def test_missing_target_column_error(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_mapping_invalid_target_exit2")
        assert exit_code == 2
        _assert_tabular_base(report)

        assert "error" in report
        assert "INVALID_COLUMN_MAPPING" in report["error"]["code"]
        # Error message should mention the missing column name
        err_msg = report["error"]["message"]
        assert "nonexistent_column" in err_msg


# ---------------------------------------------------------------------------
# Test case 5 — normalization + mapping (match)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingNormalization:
    def test_normalization_plus_mapping_match(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_mapping_normalization_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["details"]["keys"] == ["trade_id"]

        # Generated source column full_name is in compared_columns (logical name)
        compared = report["details"]["compared_columns"]
        assert "full_name" in compared
        assert "amount" in compared
        # Physical target column names should NOT appear
        assert "customer_full_name" not in compared
        assert "total_amount" not in compared

        # Column mapping in report
        cm = report["details"].get("column_mapping", {})
        assert cm.get("full_name") == "customer_full_name"
        assert cm.get("amount") == "total_amount"

        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# Test case 6 — normalization + mapping (mismatch)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestColumnMappingNormalizationMismatch:
    def test_normalization_plus_mapping_mismatch(self, e2e_runner):
        exit_code, report = e2e_runner(
            "tabular_column_mapping_normalization_mismatch_exit1"
        )
        assert exit_code == 1
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 1
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2

        # Mismatch uses logical column name full_name
        mismatches = report["samples"]["value_mismatches"]
        assert len(mismatches) == 1
        m = mismatches[0]
        assert m["key"]["trade_id"] == "2"
        assert "full_name" in m["columns"]
        # Source normalized: "Bob Jones", Target: "Robert Jones"
        assert m["columns"]["full_name"]["source"] == "Bob Jones"
        assert m["columns"]["full_name"]["target"] == "Robert Jones"

        _assert_read_rows_relationship(report)
