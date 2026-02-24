"""End-to-end tests for the tabular (CSV) comparison engine."""

from __future__ import annotations

import pytest


@pytest.mark.e2e
class TestTabularExactMatch:
    def test_exact_match_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_exact_match")
        assert exit_code == 0
        assert report["type"] == "tabular"
        assert report["summary"]["source_rows"] == 3
        assert report["summary"]["target_rows"] == 3
        assert report["summary"]["missing_in_target"] == 0
        assert report["summary"]["missing_in_source"] == 0
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["summary"]["mismatched_cells"] == 0
        assert report["details"]["keys"] == ["id"]
        assert "name" in report["details"]["compared_columns"]
        assert "value" in report["details"]["compared_columns"]


@pytest.mark.e2e
class TestTabularMissingRows:
    def test_missing_rows_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_missing_rows")
        assert exit_code == 1
        assert report["summary"]["missing_in_target"] == 1
        assert report["summary"]["missing_in_source"] == 0

        # Check samples
        missing_tgt = report["samples"]["missing_in_target"]
        assert len(missing_tgt) == 1
        assert missing_tgt[0]["key"]["id"] == "2"
        assert "line_number_source" in missing_tgt[0]


@pytest.mark.e2e
class TestTabularValueMismatch:
    def test_value_mismatch_exit_1(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_value_mismatch")
        assert exit_code == 1
        assert report["summary"]["rows_with_mismatches"] == 2

        mismatches = report["samples"]["value_mismatches"]
        assert len(mismatches) == 2

        # Ordered by key ASC
        assert mismatches[0]["key"]["id"] == "1"
        assert mismatches[1]["key"]["id"] == "2"

        # Check mismatch details
        assert "value" in mismatches[0]["columns"]
        assert mismatches[0]["columns"]["value"]["source"] == "100"
        assert mismatches[0]["columns"]["value"]["target"] == "999"

        # Line numbers present
        assert mismatches[0]["line_number_source"] is not None
        assert mismatches[0]["line_number_target"] is not None


@pytest.mark.e2e
class TestTabularExcludeKeys:
    def test_exclude_keys_suppresses_mismatch_exit_0(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_exclude_keys")
        assert exit_code == 0
        assert report["summary"]["rows_with_mismatches"] == 0
        assert report["details"]["filters_applied"]["exclude_keys_count"] == 1
        assert report["details"]["filters_applied"]["source_excluded_rows"] == 1
        assert report["details"]["filters_applied"]["target_excluded_rows"] == 1

        # Excluded samples present
        excluded = report["samples"]["excluded"]
        assert len(excluded) >= 1
        assert all(e["reason"] == "exclude_keys" for e in excluded)


@pytest.mark.e2e
class TestTabularDuplicateKeysError:
    def test_duplicate_keys_exit_2(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_duplicate_keys_error")
        assert exit_code == 2
        assert report["error"]["code"] == "DUPLICATE_KEYS"


@pytest.mark.e2e
class TestTabularInvalidExcludeKeys:
    def test_invalid_exclude_keys_exit_2(self, e2e_runner):
        exit_code, report = e2e_runner("tabular_invalid_exclude_keys")
        assert exit_code == 2
        assert report["error"]["code"] == "CONFIG_VALIDATION_ERROR"
