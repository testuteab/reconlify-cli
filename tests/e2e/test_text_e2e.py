"""End-to-end tests for the reconify text engine CLI.

These tests invoke the CLI as a subprocess, write reports to .artifacts/e2e/,
and validate the JSON output against expected values.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Shared assertion helpers
# ---------------------------------------------------------------------------

_SUMMARY_FIELDS = (
    "total_lines_source",
    "total_lines_target",
    "different_lines",
    "comparison_time_seconds",
)


def _assert_text_base(report):
    """Validate common text report structure."""
    assert report["type"] == "text"
    s = report["summary"]
    for field in ("total_lines_source", "total_lines_target", "different_lines"):
        assert field in s, f"summary.{field} missing"
        assert isinstance(s[field], int), f"summary.{field} not int"
    assert "comparison_time_seconds" in s
    assert isinstance(s["comparison_time_seconds"], (int, float))
    # read_lines and ignored_blank_lines must always be present
    d = report["details"]
    for field in (
        "read_lines_source",
        "read_lines_target",
        "ignored_blank_lines_source",
        "ignored_blank_lines_target",
    ):
        assert field in d, f"details.{field} missing"
        assert isinstance(d[field], int), f"details.{field} not int"


def _assert_line_by_line_samples(report):
    """Assert line_by_line samples structure."""
    assert isinstance(report["samples"], list)
    for sample in report["samples"]:
        assert "line_number_source" in sample
        assert "line_number_target" in sample
        # Raw + processed fields
        assert "raw_source" in sample
        assert "raw_target" in sample
        assert "processed_source" in sample
        assert "processed_target" in sample
        # Deprecated aliases equal processed
        assert "source" in sample
        assert "target" in sample
        assert sample["source"] == sample["processed_source"]
        assert sample["target"] == sample["processed_target"]


def _assert_unordered_report(report):
    """Assert unordered_lines report structure."""
    assert report["samples"] == []
    if report["summary"]["different_lines"] > 0:
        assert "samples_agg" in report
        assert isinstance(report["samples_agg"], list)
    stats = report["details"].get("unordered_stats")
    if stats is not None:
        assert isinstance(stats["source_only_lines"], int)
        assert isinstance(stats["target_only_lines"], int)
        assert isinstance(stats["distinct_mismatched_lines"], int)


# ---------------------------------------------------------------------------
# line_by_line mode
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestLineByLineE2E:
    def test_identical(self, e2e_runner):
        """Identical files -> exit 0, no diffs, no samples."""
        ec, r = e2e_runner("text_line_by_line_identical")
        assert ec == 0
        _assert_text_base(r)
        assert r["version"] == "1.2"
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 3
        assert r["summary"]["total_lines_target"] == 3
        assert r["samples"] == []
        assert "samples_agg" not in r
        assert "error" not in r
        _assert_line_by_line_samples(r)
        # No filtering: read_lines == total_lines, ignored == 0
        d = r["details"]
        assert d["read_lines_source"] == 3
        assert d["read_lines_target"] == 3
        assert d["ignored_blank_lines_source"] == 0
        assert d["ignored_blank_lines_target"] == 0

    def test_original_line_numbers(self, e2e_runner):
        """Dropped lines -> line_number_* reflects original file positions."""
        ec, r = e2e_runner("text_line_by_line_original_line_numbers")
        assert ec == 1
        _assert_text_base(r)
        assert r["summary"]["different_lines"] == 1
        assert r["details"]["mode"] == "line_by_line"
        assert r["details"]["rules_applied"]["drop_lines_count"] == 2

        s = r["samples"]
        assert len(s) == 1
        _assert_line_by_line_samples(r)
        # source "bbb" is on raw line 4 (lines 1 and 3 are dropped comments)
        assert s[0]["line_number_source"] == 4
        assert s[0]["line_number_target"] == 2
        assert s[0]["source"] == "bbb"
        assert s[0]["target"] == "ccc"
        # No debug fields by default
        assert "processed_line_number_source" not in s[0]

    def test_debug_processed_numbers(self, e2e_runner):
        """--debug-report adds processed_line_number_* per file."""
        ec, r = e2e_runner(
            "text_line_by_line_debug_processed_numbers",
            cli_flags=["--debug-report"],
        )
        assert ec == 1
        _assert_text_base(r)
        assert r["summary"]["different_lines"] == 2

        s = r["samples"]
        assert len(s) == 2

        # Pair 0: src "bbb" (orig 5, proc 2), tgt "xxx" (orig 2, proc 2)
        assert s[0]["line_number_source"] == 5
        assert s[0]["line_number_target"] == 2
        assert s[0]["source"] == "bbb"
        assert s[0]["target"] == "xxx"
        assert s[0]["processed_line_number_source"] == 2
        assert s[0]["processed_line_number_target"] == 2

        # Pair 1: src exhausted -> null, tgt "yyy" (orig 3, proc 3)
        assert s[1]["line_number_source"] is None
        assert s[1]["processed_line_number_source"] is None
        assert s[1]["line_number_target"] == 3
        assert s[1]["processed_line_number_target"] == 3
        assert s[1]["source"] == ""
        assert s[1]["target"] == "yyy"

    def test_missing_lines_null(self, e2e_runner):
        """When target is shorter, line_number_target is null in JSON."""
        ec, r = e2e_runner("text_line_by_line_missing_lines")
        assert ec == 1
        _assert_text_base(r)
        assert r["summary"]["different_lines"] == 2

        s = r["samples"]
        assert len(s) == 2
        _assert_line_by_line_samples(r)
        # Both samples have null line_number_target
        assert s[0]["line_number_source"] == 2
        assert s[0]["line_number_target"] is None
        assert s[0]["source"] == "bbb"
        assert s[0]["target"] == ""

        assert s[1]["line_number_source"] == 3
        assert s[1]["line_number_target"] is None


# ---------------------------------------------------------------------------
# unordered_lines mode
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestUnorderedE2E:
    def test_counts_and_agg(self, e2e_runner):
        """Aggregated samples with correct counts, line numbers, and ordering."""
        ec, r = e2e_runner("text_unordered_counts_and_agg")
        assert ec == 1
        _assert_text_base(r)
        assert r["details"]["mode"] == "unordered_lines"
        assert r["summary"]["different_lines"] == 4
        _assert_unordered_report(r)

        agg = r["samples_agg"]
        assert len(agg) == 2

        # Both have abs_diff=2 -> sorted alphabetically: "alpha" < "gamma"
        assert agg[0]["line"] == "alpha"
        assert agg[0]["source_count"] == 3
        assert agg[0]["target_count"] == 1
        assert agg[0]["source_line_numbers"] == [1, 2, 3]
        assert agg[0]["target_line_numbers"] == [1]
        assert agg[0]["source_line_numbers_truncated"] is False
        assert agg[0]["target_line_numbers_truncated"] is False

        assert agg[1]["line"] == "gamma"
        assert agg[1]["source_count"] == 0
        assert agg[1]["target_count"] == 2
        assert agg[1]["source_line_numbers"] == []
        assert agg[1]["target_line_numbers"] == [3, 4]

        # unordered_stats
        stats = r["details"]["unordered_stats"]
        assert stats["source_only_lines"] == 2
        assert stats["target_only_lines"] == 2
        assert stats["distinct_mismatched_lines"] == 2

    def test_include_line_numbers_false(self, e2e_runner):
        """--no-include-line-numbers omits line number arrays."""
        ec, r = e2e_runner(
            "text_unordered_include_line_numbers_false",
            cli_flags=["--no-include-line-numbers"],
        )
        assert ec == 1
        _assert_text_base(r)
        _assert_unordered_report(r)
        agg = r["samples_agg"]
        assert len(agg) == 2
        assert "source_line_numbers" not in agg[0]
        assert "target_line_numbers" not in agg[0]
        assert "source_line_numbers_truncated" not in agg[0]
        assert "target_line_numbers_truncated" not in agg[0]

    def test_max_line_numbers_cap(self, e2e_runner):
        """--max-line-numbers caps stored line numbers with truncated flag."""
        ec, r = e2e_runner(
            "text_unordered_max_line_numbers_cap",
            cli_flags=["--max-line-numbers", "5"],
        )
        assert ec == 1
        _assert_text_base(r)
        _assert_unordered_report(r)
        agg = r["samples_agg"]
        assert len(agg) == 1
        assert agg[0]["line"] == "x"
        assert agg[0]["source_count"] == 15
        assert agg[0]["target_count"] == 0
        assert agg[0]["source_line_numbers"] == [1, 2, 3, 4, 5]
        assert agg[0]["source_line_numbers_truncated"] is True
        assert agg[0]["target_line_numbers"] == []
        assert agg[0]["target_line_numbers_truncated"] is False


# ---------------------------------------------------------------------------
# Normalization options
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestNormalizationE2E:
    def test_normalize_newlines_true(self, e2e_runner):
        """CRLF vs LF: normalize_newlines=true -> identical after normalization."""
        ec, r = e2e_runner(
            "text_normalize_newlines_true",
            source_bytes=b"aaa\r\nbbb\r\nccc\r\n",
            target_bytes=b"aaa\nbbb\nccc\n",
        )
        assert ec == 0
        _assert_text_base(r)
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 3
        assert r["summary"]["total_lines_target"] == 3

    def test_normalize_newlines_false(self, e2e_runner):
        r"""CRLF vs LF: normalize_newlines=false -> lines differ (\r preserved)."""
        ec, r = e2e_runner(
            "text_normalize_newlines_false",
            source_bytes=b"aaa\r\nbbb\r\nccc\r\n",
            target_bytes=b"aaa\nbbb\nccc\n",
        )
        assert ec == 1
        _assert_text_base(r)
        # All 3 lines differ: "aaa\r" != "aaa", etc.
        assert r["summary"]["different_lines"] == 3
        _assert_line_by_line_samples(r)

    def test_trim_lines_false_unordered(self, e2e_runner):
        """trim_lines=false (default): leading whitespace causes mismatch."""
        ec, r = e2e_runner("text_trim_lines_false_unordered")
        assert ec == 1
        _assert_text_base(r)
        assert r["details"]["mode"] == "unordered_lines"
        # "  aaa" != "aaa", "  bbb" != "bbb" -> 4 different lines
        assert r["summary"]["different_lines"] == 4
        _assert_unordered_report(r)

    def test_ignore_blank_lines(self, e2e_runner):
        """ignore_blank_lines=true: blank lines are filtered out before comparison."""
        ec, r = e2e_runner("text_ignore_blank_lines")
        assert ec == 0
        _assert_text_base(r)
        assert r["summary"]["total_lines_source"] == 2
        assert r["summary"]["total_lines_target"] == 2
        assert r["summary"]["different_lines"] == 0
        # Source has 4 raw lines (2 blank), target has 2 raw lines (0 blank)
        d = r["details"]
        assert d["read_lines_source"] == 4
        assert d["read_lines_target"] == 2
        assert d["read_lines_source"] >= r["summary"]["total_lines_source"]
        assert d["ignored_blank_lines_source"] == 2
        assert d["ignored_blank_lines_target"] == 0


# ---------------------------------------------------------------------------
# Rules (replace_regex + drop_lines_regex)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRulesE2E:
    def test_replace_and_drop_rules(self, e2e_runner):
        """Combined replace_regex, drop_lines_regex, trim, collapse in unordered mode."""
        ec, r = e2e_runner("text_replace_and_drop_rules")
        assert ec == 0
        _assert_text_base(r)
        assert r["summary"]["different_lines"] == 0
        assert r["details"]["mode"] == "unordered_lines"
        assert r["details"]["rules_applied"]["drop_lines_count"] > 0
        assert r["details"]["rules_applied"]["replace_rules_count"] > 0


# ---------------------------------------------------------------------------
# Performance / top-N
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestPerformanceE2E:
    def test_unordered_large_mismatches(self, e2e_runner):
        """5000 distinct mismatches, sample_limit=10 -> exactly 10 agg samples."""
        source_lines = "\n".join(f"line_{i:05d}" for i in range(5000)) + "\n"
        ec, r = e2e_runner(
            "text_unordered_large_mismatches",
            cli_flags=["--sample-limit", "10"],
            source_bytes=source_lines.encode(),
            target_bytes=b"",
        )
        assert ec == 1
        _assert_text_base(r)
        _assert_unordered_report(r)
        assert r["summary"]["different_lines"] == 5000
        assert len(r["samples_agg"]) == 10
        assert r["details"]["unordered_stats"]["distinct_mismatched_lines"] == 5000
        # All diffs=1, so sorted alphabetically: line_00000, line_00001, ...
        for i, sample in enumerate(r["samples_agg"]):
            assert sample["line"] == f"line_{i:05d}"
            assert sample["source_count"] == 1
            assert sample["target_count"] == 0


# ---------------------------------------------------------------------------
# NEW: normalize.collapse_whitespace
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestCollapseWhitespaceE2E:
    def test_collapse_whitespace_true_exit0(self, e2e_runner):
        """collapse_whitespace=true normalizes tabs + multi-space to single space."""
        ec, r = e2e_runner("text_collapse_whitespace_true_exit0")
        assert ec == 0
        _assert_text_base(r)
        assert r["details"]["mode"] == "line_by_line"
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 1
        assert r["summary"]["total_lines_target"] == 1
        assert r["samples"] == []
        assert "error" not in r
        _assert_line_by_line_samples(r)


# ---------------------------------------------------------------------------
# NEW: normalize.case_insensitive
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestCaseInsensitiveE2E:
    def test_case_insensitive_true_exit0(self, e2e_runner):
        """case_insensitive=true lowercases both sides before comparison."""
        ec, r = e2e_runner("text_case_insensitive_true_exit0")
        assert ec == 0
        _assert_text_base(r)
        assert r["details"]["mode"] == "line_by_line"
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 1
        assert r["summary"]["total_lines_target"] == 1
        assert r["samples"] == []
        assert "error" not in r
        _assert_line_by_line_samples(r)


# ---------------------------------------------------------------------------
# NEW: normalize.trim_lines in line_by_line mode
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestTrimLinesLineByLineE2E:
    def test_trim_lines_true_exit0(self, e2e_runner):
        """trim_lines=true strips trailing whitespace in line_by_line mode."""
        ec, r = e2e_runner("text_trim_lines_true_line_by_line_exit0")
        assert ec == 0
        _assert_text_base(r)
        assert r["details"]["mode"] == "line_by_line"
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 2
        assert r["summary"]["total_lines_target"] == 2
        assert r["samples"] == []
        assert "error" not in r
        _assert_line_by_line_samples(r)


# ---------------------------------------------------------------------------
# NEW: sample_limit truncation (line_by_line)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestSampleLimitTruncationE2E:
    def test_sample_limit_truncation(self, e2e_runner):
        """--sample-limit 2 caps samples list at 2 even with 5 mismatches."""
        ec, r = e2e_runner(
            "text_sample_limit_truncation_line_by_line",
            cli_flags=["--sample-limit", "2"],
        )
        assert ec == 1
        _assert_text_base(r)
        assert r["details"]["mode"] == "line_by_line"
        assert r["summary"]["different_lines"] == 5
        assert isinstance(r["samples"], list)
        assert len(r["samples"]) == 2
        _assert_line_by_line_samples(r)
        assert "error" not in r


# ---------------------------------------------------------------------------
# NEW: sample_limit truncation (unordered_lines)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestSampleLimitTruncationUnorderedE2E:
    def test_sample_limit_truncation_unordered_agg(self, e2e_runner):
        """--sample-limit 2 caps samples_agg to top-2 by abs diff."""
        ec, r = e2e_runner(
            "text_sample_limit_truncation_unordered_agg",
            cli_flags=["--sample-limit", "2"],
        )
        assert ec == 1
        _assert_text_base(r)
        _assert_unordered_report(r)
        assert r["details"]["mode"] == "unordered_lines"
        assert r["summary"]["different_lines"] == 7
        assert r["samples"] == []

        agg = r["samples_agg"]
        assert len(agg) == 2

        # Top-1: D has abs diff 3 (source=0, target=3)
        assert agg[0]["line"] == "D"
        assert agg[0]["source_count"] == 0
        assert agg[0]["target_count"] == 3
        assert agg[0]["source_line_numbers"] == []
        assert agg[0]["target_line_numbers"] == [3, 4, 5]

        # Top-2: A has abs diff 2 (source=3, target=1)
        assert agg[1]["line"] == "A"
        assert agg[1]["source_count"] == 3
        assert agg[1]["target_count"] == 1
        assert agg[1]["source_line_numbers"] == [1, 2, 3]
        assert agg[1]["target_line_numbers"] == [1]

        # unordered_stats reflects all 4 distinct mismatched lines
        stats = r["details"]["unordered_stats"]
        assert stats["distinct_mismatched_lines"] == 4
        assert "error" not in r


# ---------------------------------------------------------------------------
# NEW: runtime error (missing source file)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRuntimeErrorE2E:
    def test_missing_file_exit2(self, e2e_runner):
        """Non-existent source file -> exit 2 with error in report."""
        ec, r = e2e_runner("text_runtime_error_missing_file_exit2")
        assert ec == 2
        _assert_text_base(r)
        assert "error" in r
        assert r["error"]["code"] == "RUNTIME_ERROR"
        assert "Failed to read file" in r["error"]["message"]
        # Summary is zeroed on error
        assert r["summary"]["total_lines_source"] == 0
        assert r["summary"]["total_lines_target"] == 0
        assert r["summary"]["different_lines"] == 0
