"""End-to-end tests for the reconify text engine CLI.

These tests invoke the CLI as a subprocess, write reports to .artifacts/e2e/,
and validate the JSON output against expected values.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# line_by_line mode
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestLineByLineE2E:
    def test_identical(self, e2e_runner):
        """Identical files -> exit 0, no diffs, no samples."""
        ec, r = e2e_runner("text_line_by_line_identical")
        assert ec == 0
        assert r["type"] == "text"
        assert r["version"] == "1.1"
        assert r["summary"]["different_lines"] == 0
        assert r["summary"]["total_lines_source"] == 3
        assert r["summary"]["total_lines_target"] == 3
        assert r["samples"] == []
        assert "samples_agg" not in r
        assert "error" not in r

    def test_original_line_numbers(self, e2e_runner):
        """Dropped lines -> line_number_* reflects original file positions."""
        ec, r = e2e_runner("text_line_by_line_original_line_numbers")
        assert ec == 1
        assert r["summary"]["different_lines"] == 1
        assert r["details"]["mode"] == "line_by_line"
        assert r["details"]["rules_applied"]["drop_lines_count"] == 2

        s = r["samples"]
        assert len(s) == 1
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
        assert r["summary"]["different_lines"] == 2

        s = r["samples"]
        assert len(s) == 2
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
        assert r["details"]["mode"] == "unordered_lines"
        assert r["summary"]["different_lines"] == 4
        assert r["samples"] == []

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
        # All 3 lines differ: "aaa\r" != "aaa", etc.
        assert r["summary"]["different_lines"] == 3

    def test_trim_lines_false_unordered(self, e2e_runner):
        """trim_lines=false (default): leading whitespace causes mismatch."""
        ec, r = e2e_runner("text_trim_lines_false_unordered")
        assert ec == 1
        assert r["details"]["mode"] == "unordered_lines"
        # "  aaa" != "aaa", "  bbb" != "bbb" -> 4 different lines
        assert r["summary"]["different_lines"] == 4

    def test_ignore_blank_lines(self, e2e_runner):
        """ignore_blank_lines=true: blank lines are filtered out before comparison."""
        ec, r = e2e_runner("text_ignore_blank_lines")
        assert ec == 0
        assert r["summary"]["total_lines_source"] == 2
        assert r["summary"]["total_lines_target"] == 2
        assert r["summary"]["different_lines"] == 0


# ---------------------------------------------------------------------------
# Rules (replace_regex + drop_lines_regex)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestRulesE2E:
    def test_replace_and_drop_rules(self, e2e_runner):
        """Combined replace_regex, drop_lines_regex, trim, collapse in unordered mode."""
        ec, r = e2e_runner("text_replace_and_drop_rules")
        assert ec == 0
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
        assert r["summary"]["different_lines"] == 5000
        assert len(r["samples_agg"]) == 10
        assert r["details"]["unordered_stats"]["distinct_mismatched_lines"] == 5000
        # All diffs=1, so sorted alphabetically: line_00000, line_00001, ...
        for i, sample in enumerate(r["samples_agg"]):
            assert sample["line"] == f"line_{i:05d}"
            assert sample["source_count"] == 1
            assert sample["target_count"] == 0
