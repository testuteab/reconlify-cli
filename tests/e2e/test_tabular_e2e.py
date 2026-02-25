"""End-to-end tests for the tabular (CSV) comparison engine."""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Shared assertion helpers
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
        assert isinstance(s[field], int), f"summary.{field} should be int, got {type(s[field])}"


def _assert_keys_sorted(samples_list, key_names):
    """Assert sample entries are sorted by key values ascending."""
    if len(samples_list) <= 1:
        return
    for i in range(1, len(samples_list)):
        prev = tuple(str(samples_list[i - 1]["key"].get(k, "")) for k in key_names)
        curr = tuple(str(samples_list[i]["key"].get(k, "")) for k in key_names)
        assert prev <= curr, f"Samples not sorted at index {i}: {prev} > {curr}"


def _assert_line_numbers(sample, side):
    """Assert line number fields exist for the given side(s)."""
    if side in ("source", "both"):
        assert "line_number_source" in sample
        assert sample["line_number_source"] is not None
    if side in ("target", "both"):
        assert "line_number_target" in sample
        assert sample["line_number_target"] is not None


def _assert_no_row_filters(fa):
    """Assert row_filters sub-object is absent and breakdown counts are zero."""
    assert "row_filters" not in fa
    assert fa["source_excluded_rows_row_filters"] == 0
    assert fa["target_excluded_rows_row_filters"] == 0


def _assert_totals_equal_breakdown(fa):
    """Assert totals equal the sum of exclude_keys + row_filters breakdown."""
    assert fa["source_excluded_rows"] == (
        fa["source_excluded_rows_exclude_keys"] + fa["source_excluded_rows_row_filters"]
    )
    assert fa["target_excluded_rows"] == (
        fa["target_excluded_rows_exclude_keys"] + fa["target_excluded_rows_row_filters"]
    )


def _assert_read_rows_relationship(report):
    """Assert read_rows - excluded_rows == source_rows (post-filter)."""
    d = report["details"]
    s = report["summary"]
    fa = d["filters_applied"]
    assert d["read_rows_source"] - fa["source_excluded_rows"] == s["source_rows"]
    assert d["read_rows_target"] - fa["target_excluded_rows"] == s["target_rows"]


# ---------------------------------------------------------------------------
# Existing tests (strengthened)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularExactMatch:
    def test_exact_match_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_exact_match")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 3
        assert report["summary"]["target_rows"] == 3
        assert report["summary"]["missing_in_target"] == 0
        assert report["summary"]["missing_in_source"] == 0
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["details"]["keys"] == ["id"]
        assert "name" in report["details"]["compared_columns"]
        assert "value" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)
        assert report["details"]["read_rows_source"] == 3
        assert report["details"]["read_rows_target"] == 3


@pytest.mark.e2e
class TestTabularMissingRows:
    def test_missing_rows_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_missing_rows")
        assert exit_code == 1
        _assert_tabular_base(report)
        assert report["summary"]["missing_in_target"] == 1
        assert report["summary"]["missing_in_source"] == 0

        missing_tgt = report["samples"]["missing_in_target"]
        assert len(missing_tgt) == 1
        assert missing_tgt[0]["key"]["id"] == "2"
        _assert_line_numbers(missing_tgt[0], "source")
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularValueMismatch:
    def test_value_mismatch_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_value_mismatch")
        assert exit_code == 1
        _assert_tabular_base(report)
        assert report["summary"]["rows_with_mismatches"] == 2

        mismatches = report["samples"]["value_mismatches"]
        assert len(mismatches) == 2
        _assert_keys_sorted(mismatches, ["id"])

        assert mismatches[0]["key"]["id"] == "1"
        assert mismatches[1]["key"]["id"] == "2"

        assert "value" in mismatches[0]["columns"]
        assert mismatches[0]["columns"]["value"]["source"] == "100"
        assert mismatches[0]["columns"]["value"]["target"] == "999"

        for m in mismatches:
            _assert_line_numbers(m, "both")
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularExcludeKeys:
    def test_exclude_keys_suppresses_mismatch_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_exclude_keys")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["rows_with_mismatches"] == 0

        fa = report["details"]["filters_applied"]
        assert fa["exclude_keys_count"] == 1
        assert fa["source_excluded_rows"] == 1
        assert fa["target_excluded_rows"] == 1
        assert fa["source_excluded_rows_exclude_keys"] == 1
        assert fa["target_excluded_rows_exclude_keys"] == 1
        _assert_no_row_filters(fa)
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        assert len(excluded) >= 1
        assert all(e["reason"] == "exclude_keys" for e in excluded)
        _assert_keys_sorted(excluded, ["id"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularDuplicateKeysError:
    def test_duplicate_keys_exit_2(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_duplicate_keys_error")
        assert exit_code == 2
        _assert_tabular_base(report)
        assert report["error"]["code"] == "DUPLICATE_KEYS"
        _assert_no_row_filters(report["details"]["filters_applied"])
        # Error reports have read_rows zeroed
        assert report["details"]["read_rows_source"] == 0
        assert report["details"]["read_rows_target"] == 0


@pytest.mark.e2e
class TestTabularInvalidExcludeKeys:
    def test_invalid_exclude_keys_exit_2(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_invalid_exclude_keys")
        assert exit_code == 2
        assert report["error"]["code"] == "CONFIG_VALIDATION_ERROR"


@pytest.mark.e2e
class TestTabularRowFiltersExcludeBoth:
    def test_exclude_both_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_exclude_both_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["missing_in_target"] == 0
        assert report["summary"]["missing_in_source"] == 0

        fa = report["details"]["filters_applied"]
        assert fa["row_filters"] == {"count": 1, "apply_to": "both", "mode": "exclude"}
        assert fa["source_excluded_rows_row_filters"] == 2
        assert fa["target_excluded_rows_row_filters"] == 2
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 1
        assert all(e["key"]["id"] in ("2", "4") for e in rf_excluded)
        _assert_keys_sorted(excluded, ["id"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularRowFiltersApplyToSource:
    def test_apply_to_source_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_apply_to_source_exit1")
        assert exit_code == 1
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert fa["row_filters"]["apply_to"] == "source"
        assert fa["source_excluded_rows_row_filters"] == 1
        assert fa["target_excluded_rows_row_filters"] == 0
        _assert_totals_equal_breakdown(fa)

        assert report["summary"]["missing_in_source"] == 1
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularRowFiltersIncludeMode:
    def test_include_mode_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_include_mode_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert fa["row_filters"]["mode"] == "include"
        assert fa["source_excluded_rows_row_filters"] == 2
        assert fa["target_excluded_rows_row_filters"] == 2
        _assert_totals_equal_breakdown(fa)
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularInvalidRowFilters:
    def test_invalid_row_filters_exit_2(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_invalid_row_filters_exit2")
        assert exit_code == 2
        assert report["error"]["code"] == "INVALID_ROW_FILTERS"
        # Even on error, row_filters info is present since rules were configured
        assert report["details"]["filters_applied"]["row_filters"]["count"] == 1


# ---------------------------------------------------------------------------
# NEW: row_filters apply_to=target
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularRowFiltersApplyToTarget:
    def test_apply_to_target_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_apply_to_target_exit1")
        assert exit_code == 1
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert fa["row_filters"]["apply_to"] == "target"
        assert fa["source_excluded_rows_row_filters"] == 0
        assert fa["target_excluded_rows_row_filters"] > 0
        _assert_totals_equal_breakdown(fa)

        # Target lost id=1 (CANCELLED) -> missing_in_target
        assert report["summary"]["missing_in_target"] == 1
        assert report["summary"]["missing_in_source"] == 0

        missing_tgt = report["samples"]["missing_in_target"]
        assert len(missing_tgt) == 1
        assert missing_tgt[0]["key"]["id"] == "1"
        _assert_line_numbers(missing_tgt[0], "source")
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: row_filters operator coverage
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularRowFiltersOpRegex:
    def test_op_regex_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_op_regex_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert "row_filters" in fa
        assert fa["source_excluded_rows_row_filters"] > 0
        assert fa["target_excluded_rows_row_filters"] > 0
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 1
        # Excluded rows should be the test_ rows (id=2)
        assert any(e["key"]["id"] == "2" for e in rf_excluded)
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularRowFiltersOpContains:
    def test_op_contains_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_op_contains_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert "row_filters" in fa
        assert fa["source_excluded_rows_row_filters"] > 0
        assert fa["target_excluded_rows_row_filters"] > 0
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 1
        assert any(e["key"]["id"] == "2" for e in rf_excluded)
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularRowFiltersOpIn:
    def test_op_in_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_op_in_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert "row_filters" in fa
        assert fa["source_excluded_rows_row_filters"] == 2
        assert fa["target_excluded_rows_row_filters"] == 2
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 2
        excluded_ids = {e["key"]["id"] for e in rf_excluded}
        assert "2" in excluded_ids  # EU
        assert "3" in excluded_ids  # APAC
        _assert_keys_sorted(excluded, ["id"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularRowFiltersOpNotEquals:
    def test_op_not_equals_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_op_not_equals_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert "row_filters" in fa
        assert fa["source_excluded_rows_row_filters"] == 1
        assert fa["target_excluded_rows_row_filters"] == 1
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 1
        assert any(e["key"]["id"] == "2" for e in rf_excluded)
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: row_filters is_null / not_null
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularRowFiltersIsNullNotNull:
    def test_is_null_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_row_filters_is_null_not_null_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        fa = report["details"]["filters_applied"]
        assert "row_filters" in fa
        # id=1 and id=3 have NULL comment -> excluded from both sides
        assert fa["source_excluded_rows_row_filters"] == 2
        assert fa["target_excluded_rows_row_filters"] == 2
        _assert_totals_equal_breakdown(fa)

        excluded = report["samples"]["excluded"]
        rf_excluded = [e for e in excluded if e["reason"] == "row_filters"]
        assert len(rf_excluded) >= 2
        excluded_ids = {e["key"]["id"] for e in rf_excluded}
        assert "1" in excluded_ids
        assert "3" in excluded_ids
        _assert_keys_sorted(excluded, ["id"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: compare options - include_columns / exclude_columns
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularCompareIncludeColumns:
    def test_include_columns_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_compare_include_columns_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["details"]["compared_columns"] == ["name"]
        assert "amount" not in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularCompareExcludeColumns:
    def test_exclude_columns_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_compare_exclude_columns_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["details"]["compared_columns"] == ["name"]
        assert "amount" not in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: compare normalization - trim, case, normalize_nulls
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularCompareNormalizeTrimCase:
    def test_normalize_trim_case_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_compare_normalize_trim_case_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert "name" in report["details"]["compared_columns"]
        assert "comment" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: sampling + output flags
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularSamplingAndOutputFlags:
    def test_sampling_limit_and_no_column_stats(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_sampling_and_output_flags")
        assert exit_code == 1
        _assert_tabular_base(report)

        assert report["summary"]["rows_with_mismatches"] == 10
        assert report["summary"]["mismatched_cells"] == 10

        # sample_limit_per_type=2 caps value_mismatches
        mismatches = report["samples"]["value_mismatches"]
        assert len(mismatches) == 2
        _assert_keys_sorted(mismatches, ["id"])
        for m in mismatches:
            _assert_line_numbers(m, "both")

        # include_column_stats=false -> column_stats is empty dict
        assert report["details"].get("column_stats", {}) == {}

        # No filters -> excluded is empty
        assert report["samples"]["excluded"] == []
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: CSV options - delimiter and header=false
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularCsvDelimiterSemicolon:
    def test_semicolon_delimiter_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_csv_delimiter_semicolon_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["details"]["keys"] == ["id"]
        assert "name" in report["details"]["compared_columns"]
        assert "value" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


@pytest.mark.e2e
class TestTabularCsvHeaderFalse:
    def test_header_false_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_csv_header_false_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)

        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        # DuckDB generates column0, column1, column2 when header=false
        assert "column0" in report["details"]["keys"]
        # include_columns: [column1] limits comparison to column1
        assert report["details"]["compared_columns"] == ["column1"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: NULL-safe key matching
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularNullKeysMatching:
    def test_null_keys_matching_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_null_keys_matching_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 3
        assert report["summary"]["target_rows"] == 3
        assert report["summary"]["missing_in_target"] == 0
        assert report["summary"]["missing_in_source"] == 0
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["details"]["keys"] == ["id", "region"]
        # NULL region key should be matched via IS NOT DISTINCT FROM
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: column_stats disabled → always empty {}
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularColumnStatsDisabled:
    def test_column_stats_disabled_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_column_stats_disabled_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        # column_stats must always be present in details, empty when disabled
        assert "column_stats" in report["details"]
        assert report["details"]["column_stats"] == {}
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: ignore_columns
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularIgnoreColumns:
    def test_ignore_columns_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_ignore_columns_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        # "notes" should not be in compared_columns
        assert "notes" not in report["details"]["compared_columns"]
        assert "name" in report["details"]["compared_columns"]
        assert "value" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: tolerance (absolute, per column)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularTolerance:
    def test_tolerance_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_tolerance_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 3
        assert report["summary"]["target_rows"] == 3
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert "amount" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: string_rules - contains
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularStringRulesContains:
    def test_string_rules_contains_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_string_rules_contains_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert "description" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: string_rules - regex_extract
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularStringRulesRegexExtract:
    def test_string_rules_regex_extract_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_string_rules_regex_extract_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert "code" in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: normalization virtual columns
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularNormalizationVirtualColumn:
    def test_normalization_virtual_column_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_normalization_virtual_column_exit0")
        assert exit_code == 0
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 2
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        # full_name is a virtual column created via normalization
        assert "full_name" in report["details"]["compared_columns"]
        # email is a shared column between source and target
        assert "email" in report["details"]["compared_columns"]
        # first_name and last_name are source-only, not in target
        assert "first_name" not in report["details"]["compared_columns"]
        assert "last_name" not in report["details"]["compared_columns"]
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)


# ---------------------------------------------------------------------------
# NEW: missing detection with NULL/empty keys
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTabularMissingDetectionNullKeys:
    def test_missing_detection_null_keys_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_missing_detection_null_keys_exit1")
        assert exit_code == 1
        _assert_tabular_base(report)
        assert report["summary"]["source_rows"] == 3
        assert report["summary"]["target_rows"] == 2
        assert report["summary"]["missing_in_target"] == 1
        assert report["summary"]["missing_in_source"] == 0

        missing_tgt = report["samples"]["missing_in_target"]
        assert len(missing_tgt) == 1
        assert missing_tgt[0]["key"]["id"] == "2"
        _assert_line_numbers(missing_tgt[0], "source")
        _assert_no_row_filters(report["details"]["filters_applied"])
        _assert_read_rows_relationship(report)
