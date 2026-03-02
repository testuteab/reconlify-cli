"""Tests for the text comparison engine."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from reconify.cli import app
from reconify.models import TextConfig
from reconify.text_engine import compare_text

runner = CliRunner()


# ---------------------------------------------------------------------------
# 1) unordered_lines with normalization and replace/drop rules
# ---------------------------------------------------------------------------


def test_unordered_lines_with_rules(tmp_path):
    """Same logical content in different order, with comments and dates.

    - Lines starting with '#' are dropped via drop_lines_regex.
    - Dates like 2024-01-15 are replaced with DATE via replace_regex.
    - After normalization, the two files should be identical (exit 0).
    - drop_lines_count and replace_rules_count must be > 0.
    """
    source = tmp_path / "source.txt"
    source.write_text(
        "# header comment\n"
        "alpha 2024-01-15 value\n"
        "  beta   2024-02-20   value  \n"
        "# another comment\n"
        "gamma 2024-03-10 value\n"
    )

    target = tmp_path / "target.txt"
    target.write_text(
        "gamma 2025-12-01 value\n# target comment\nalpha 2025-11-30 value\nbeta 2025-10-05 value\n"
    )

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
        normalize={
            "ignore_blank_lines": True,
            "trim_lines": True,
            "collapse_whitespace": True,
            "case_insensitive": False,
            "normalize_newlines": True,
        },
        drop_lines_regex=["^#"],
        replace_regex=[{"pattern": r"\d{4}-\d{2}-\d{2}", "replace": "DATE"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0, f"Expected exit 0, got {exit_code}. Report: {report}"
    assert report["summary"]["different_lines"] == 0
    assert report["details"]["mode"] == "unordered_lines"
    assert report["details"]["rules_applied"]["drop_lines_count"] > 0
    assert report["details"]["rules_applied"]["replace_rules_count"] > 0
    assert report["summary"]["comparison_time_seconds"] > 0
    # No mismatches -> samples_agg empty
    assert report["samples"] == []
    assert report["samples_agg"] == []


def test_unordered_lines_detects_mismatch(tmp_path):
    """Unordered comparison should detect extra/missing lines."""
    source = tmp_path / "source.txt"
    source.write_text("alpha\nbeta\nbeta\n")

    target = tmp_path / "target.txt"
    target.write_text("alpha\nbeta\ngamma\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    # beta: 2 in source, 1 in target -> diff 1
    # gamma: 0 in source, 1 in target -> diff 1
    assert report["summary"]["different_lines"] == 2
    # samples is empty for unordered mode; use samples_agg
    assert report["samples"] == []
    assert len(report["samples_agg"]) == 2


def test_unordered_aggregated_samples_content(tmp_path):
    """Verify samples_agg items have correct fields, line numbers, and ordering."""
    source = tmp_path / "source.txt"
    # alpha x3 (lines 1,2,3), beta x1 (line 4)
    source.write_text("alpha\nalpha\nalpha\nbeta\n")

    target = tmp_path / "target.txt"
    # alpha x1 (line 1), beta x1 (line 2), gamma x2 (lines 3,4)
    target.write_text("alpha\nbeta\ngamma\ngamma\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    # alpha: |3-1|=2, gamma: |0-2|=2
    assert report["summary"]["different_lines"] == 4

    agg = report["samples_agg"]
    assert len(agg) == 2

    # Both have abs_diff=2, so sorted lexicographically: "alpha" < "gamma"
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
    assert agg[1]["source_line_numbers_truncated"] is False
    assert agg[1]["target_line_numbers_truncated"] is False

    # Verify unordered_stats
    stats = report["details"]["unordered_stats"]
    assert stats["source_only_lines"] == 2  # 3-1 for alpha
    assert stats["target_only_lines"] == 2  # 2-0 for gamma
    assert stats["distinct_mismatched_lines"] == 2


def test_unordered_aggregated_samples_ordering(tmp_path):
    """Verify samples_agg sorts by abs(diff) DESC, then line lexicographically."""
    source = tmp_path / "source.txt"
    # x: 5 (lines 1-5), y: 1 (line 6), z: 1 (line 7)
    source.write_text("x\nx\nx\nx\nx\ny\nz\n")

    target = tmp_path / "target.txt"
    # x: 1 (line 1), y: 1 (line 2), z: 3 (lines 3,4,5)
    target.write_text("x\ny\nz\nz\nz\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    agg = report["samples_agg"]

    # x: abs(5-1)=4, z: abs(1-3)=2 => x first (higher diff), then z
    assert agg[0]["line"] == "x"
    assert agg[0]["source_count"] == 5
    assert agg[0]["target_count"] == 1
    assert agg[0]["source_line_numbers"] == [1, 2, 3, 4, 5]
    assert agg[0]["target_line_numbers"] == [1]

    assert agg[1]["line"] == "z"
    assert agg[1]["source_count"] == 1
    assert agg[1]["target_count"] == 3
    assert agg[1]["source_line_numbers"] == [7]
    assert agg[1]["target_line_numbers"] == [3, 4, 5]


def test_unordered_all_samples_collected(tmp_path):
    """samples_agg should include all mismatched distinct lines."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    # 20 distinct mismatched lines
    source.write_text("\n".join(f"line_{i}" for i in range(20)) + "\n")
    target.write_text("")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == 20
    assert len(report["samples_agg"]) == 20
    assert report["details"]["unordered_stats"]["distinct_mismatched_lines"] == 20


def test_unordered_line_numbers_truncated(tmp_path):
    """Line number arrays should be capped at max_line_numbers with truncated flag."""
    source = tmp_path / "source.txt"
    # "x" appears 15 times
    source.write_text("\n".join(["x"] * 15) + "\n")
    target = tmp_path / "target.txt"
    target.write_text("")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg, max_line_numbers=5)

    assert exit_code == 1
    agg = report["samples_agg"]
    assert len(agg) == 1
    assert agg[0]["source_count"] == 15
    assert agg[0]["source_line_numbers"] == [1, 2, 3, 4, 5]
    assert agg[0]["source_line_numbers_truncated"] is True
    assert agg[0]["target_line_numbers"] == []
    assert agg[0]["target_line_numbers_truncated"] is False


def test_unordered_no_line_numbers(tmp_path):
    """When include_line_numbers=False, line number arrays are omitted."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\n")
    target = tmp_path / "target.txt"
    target.write_text("bbb\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg, include_line_numbers=False)

    assert exit_code == 1
    agg = report["samples_agg"]
    assert len(agg) == 2
    assert "source_line_numbers" not in agg[0]
    assert "target_line_numbers" not in agg[0]
    assert "source_line_numbers_truncated" not in agg[0]
    assert "target_line_numbers_truncated" not in agg[0]


# ---------------------------------------------------------------------------
# 2) line_by_line detects differences and produces samples
# ---------------------------------------------------------------------------


def test_line_by_line_identical(tmp_path):
    """Identical files should produce exit 0 and no samples."""
    content = "line one\nline two\nline three\n"
    source = tmp_path / "source.txt"
    source.write_text(content)
    target = tmp_path / "target.txt"
    target.write_text(content)

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    assert report["summary"]["different_lines"] == 0
    assert report["samples"] == []
    assert report["summary"]["total_lines_source"] == 3
    assert report["summary"]["total_lines_target"] == 3
    # line_by_line should not have samples_agg
    assert "samples_agg" not in report


def test_line_by_line_detects_differences(tmp_path):
    """Line-by-line mode should report original file line numbers in samples."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\nccc\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\nBBB\nccc\nddd\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert report["summary"]["total_lines_source"] == 3
    assert report["summary"]["total_lines_target"] == 4
    # line 2 differs (bbb vs BBB), line 4 missing in source
    assert report["summary"]["different_lines"] == 2

    samples = report["samples"]
    assert len(samples) == 2

    # First diff: original line 2 on both sides
    assert samples[0]["line_number_source"] == 2
    assert samples[0]["line_number_target"] == 2
    assert samples[0]["source"] == "bbb"
    assert samples[0]["target"] == "BBB"

    # Second diff: line 4 missing in source → null line_number_source
    assert samples[1]["line_number_source"] is None
    assert samples[1]["line_number_target"] == 4
    assert samples[1]["source"] == ""
    assert samples[1]["target"] == "ddd"


def test_line_by_line_original_line_numbers_with_drops(tmp_path):
    """When drop_lines_regex removes lines, line_number_* must be original file lines."""
    # Source:
    #   Line 1: "# comment"     → dropped
    #   Line 2: "aaa"           → kept (processed idx 0)
    #   Line 3: "# comment 2"   → dropped
    #   Line 4: "bbb"           → kept (processed idx 1)
    source = tmp_path / "source.txt"
    source.write_text("# comment\naaa\n# comment 2\nbbb\n")

    # Target:
    #   Line 1: "aaa"           → kept (processed idx 0)
    #   Line 2: "ccc"           → kept (processed idx 1)
    target = tmp_path / "target.txt"
    target.write_text("aaa\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    # Processed: source=["aaa","bbb"], target=["aaa","ccc"]
    # Diff at processed idx 1: source "bbb" (orig line 4), target "ccc" (orig line 2)
    samples = report["samples"]
    assert len(samples) == 1
    assert samples[0]["line_number_source"] == 4  # original file line 4
    assert samples[0]["line_number_target"] == 2  # original file line 2
    assert samples[0]["source"] == "bbb"
    assert samples[0]["target"] == "ccc"
    # No debug fields by default
    assert "processed_line_number_source" not in samples[0]


def test_line_by_line_debug_report(tmp_path):
    """With debug_report=True, processed_line_number_* fields are included."""
    source = tmp_path / "source.txt"
    source.write_text("# skip\naaa\nbbb\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
    )

    report, exit_code = compare_text(cfg, debug_report=True)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 1

    # source "bbb" is orig line 3, processed line 2; target "ccc" is orig line 2, processed line 2
    assert samples[0]["line_number_source"] == 3
    assert samples[0]["line_number_target"] == 2
    assert samples[0]["processed_line_number_source"] == 2
    assert samples[0]["processed_line_number_target"] == 2


def test_line_by_line_debug_asymmetric_drops(tmp_path):
    """Debug processed_line_number reflects per-file position, not pair index."""
    # Source: drops 3 comments, keeps "aaa" (proc 1) and "bbb" (proc 2)
    source = tmp_path / "source.txt"
    source.write_text("# a\n# b\n# c\naaa\nbbb\n")
    # Target: no drops, keeps "aaa" (proc 1), "xxx" (proc 2), "yyy" (proc 3)
    target = tmp_path / "target.txt"
    target.write_text("aaa\nxxx\nyyy\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
    )

    report, exit_code = compare_text(cfg, debug_report=True)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 2

    # Pair 1 diff: src "bbb" (orig 5, proc 2), tgt "xxx" (orig 2, proc 2)
    assert samples[0]["line_number_source"] == 5
    assert samples[0]["line_number_target"] == 2
    assert samples[0]["processed_line_number_source"] == 2
    assert samples[0]["processed_line_number_target"] == 2

    # Pair 2 diff: src exhausted → null, tgt "yyy" (orig 3, proc 3)
    assert samples[1]["line_number_source"] is None
    assert samples[1]["processed_line_number_source"] is None
    assert samples[1]["line_number_target"] == 3
    assert samples[1]["processed_line_number_target"] == 3


def test_line_by_line_no_debug_fields_by_default(tmp_path):
    """Debug fields must not appear when debug_report is False."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\n")
    target = tmp_path / "target.txt"
    target.write_text("bbb\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert "processed_line_number_source" not in report["samples"][0]
    assert "processed_line_number_target" not in report["samples"][0]


def test_line_by_line_missing_target_lines_null(tmp_path):
    """When target is shorter, line_number_target should be null."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\nccc\n")
    target = tmp_path / "target.txt"
    target.write_text("aaa\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 2

    # Processed idx 1: source "bbb" (orig 2), target missing
    assert samples[0]["line_number_source"] == 2
    assert samples[0]["line_number_target"] is None
    assert samples[0]["target"] == ""

    # Processed idx 2: source "ccc" (orig 3), target missing
    assert samples[1]["line_number_source"] == 3
    assert samples[1]["line_number_target"] is None


def test_line_by_line_case_insensitive(tmp_path):
    """Case-insensitive normalization makes bbb == BBB."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\nccc\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\nBBB\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    assert report["summary"]["different_lines"] == 0


def test_line_by_line_with_crlf(tmp_path):
    """CRLF and LF should normalize to same result."""
    source = tmp_path / "source.txt"
    source.write_bytes(b"aaa\r\nbbb\r\nccc\r\n")

    target = tmp_path / "target.txt"
    target.write_bytes(b"aaa\nbbb\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    assert report["summary"]["different_lines"] == 0


def test_all_samples_collected(tmp_path):
    """All diff samples should be included without truncation."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    # 100 different lines
    source.write_text("\n".join(f"src_{i}" for i in range(100)) + "\n")
    target.write_text("\n".join(f"tgt_{i}" for i in range(100)) + "\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == 100
    assert len(report["samples"]) == 100


def test_ignore_blank_lines(tmp_path):
    """Blank lines should be dropped when ignore_blank_lines is True."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\n\n\nbbb\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\nbbb\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"ignore_blank_lines": True},
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    assert report["summary"]["total_lines_source"] == 2
    assert report["summary"]["total_lines_target"] == 2


def test_drop_lines_regex(tmp_path):
    """Lines matching drop regex should be removed before comparison."""
    source = tmp_path / "source.txt"
    source.write_text("# comment\ndata1\n// another\ndata2\n")

    target = tmp_path / "target.txt"
    target.write_text("data1\ndata2\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#", "^//"],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    assert report["details"]["rules_applied"]["drop_lines_count"] == 2


def test_replace_regex_count(tmp_path):
    """Replace regex should track total replacement count."""
    source = tmp_path / "source.txt"
    source.write_text("id=123 id=456\nid=789\n")

    target = tmp_path / "target.txt"
    target.write_text("id=ID id=ID\nid=ID\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        replace_regex=[{"pattern": r"\d+", "replace": "ID"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ra = report["details"]["rules_applied"]
    # replace_rules_count = number of configured rules
    assert ra["replace_rules_count"] == 1
    # 3 replacements in source, 0 in target (already "ID")
    assert ra["replacement_applications"] == 3
    # 2 lines affected in source (both had replacements), 0 in target
    assert ra["replacement_lines_affected"] == 2


def test_line_by_line_raw_and_processed_fields(tmp_path):
    """With replace_regex, samples include both raw and processed content.

    - raw_source / raw_target reflect original file content.
    - processed_source / processed_target reflect post-pipeline content.
    - source == processed_source and target == processed_target (deprecated aliases).
    """
    source = tmp_path / "source.txt"
    source.write_text("id=123 data\nid=456 data\n")

    target = tmp_path / "target.txt"
    target.write_text("id=AAA data\nid=BBB other\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        replace_regex=[{"pattern": r"id=\S+", "replace": "id=X"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    samples = report["samples"]
    # Line 1: after replace both become "id=X data", so they match.
    # Line 2: source "id=X data" != target "id=X other" → diff.
    assert len(samples) == 1

    s = samples[0]
    # Raw fields contain original file content
    assert s["raw_source"] == "id=456 data"
    assert s["raw_target"] == "id=BBB other"
    # Processed fields contain post-replacement content
    assert s["processed_source"] == "id=X data"
    assert s["processed_target"] == "id=X other"
    # Processed differs from raw
    assert s["raw_source"] != s["processed_source"]
    assert s["raw_target"] != s["processed_target"]
    # Deprecated aliases equal processed
    assert s["source"] == s["processed_source"]
    assert s["target"] == s["processed_target"]


def test_line_by_line_raw_equals_processed_without_rules(tmp_path):
    """Without normalization rules, raw and processed content are identical."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 1
    s = samples[0]
    assert s["raw_source"] == s["processed_source"] == "bbb"
    assert s["raw_target"] == s["processed_target"] == "ccc"
    assert s["source"] == s["processed_source"]
    assert s["target"] == s["processed_target"]


def test_line_by_line_raw_and_processed_exhausted_side(tmp_path):
    """When one side is exhausted, raw and processed fields are empty strings."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\n")

    target = tmp_path / "target.txt"
    target.write_text("aaa\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 1
    s = samples[0]
    # Source has content
    assert s["raw_source"] == "bbb"
    assert s["processed_source"] == "bbb"
    # Target exhausted → empty strings
    assert s["raw_target"] == ""
    assert s["processed_target"] == ""
    assert s["line_number_target"] is None


def test_replace_regex_with_case_insensitive(tmp_path):
    """replace_regex must still match when case_insensitive=true.

    Regression: the old pipeline lowercased BEFORE applying replace_regex,
    so patterns like 'T' and 'Z' in ISO-8601 timestamps would not match.
    The fixed pipeline applies replace_regex first, then lowercases.
    """
    source = tmp_path / "source.txt"
    source.write_text("2025-02-10T06:00:00.000Z\n")

    target = tmp_path / "target.txt"
    target.write_text("2026-01-01T12:00:00.000Z\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
        replace_regex=[
            {"pattern": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", "replace": "<TS>"}
        ],
    )

    report, exit_code = compare_text(cfg)

    # Both lines should become "<ts>" (replacement then lowercase), so match.
    assert exit_code == 0, f"Expected exit 0. Report: {report}"
    assert report["summary"]["different_lines"] == 0
    # Replacement should have fired on both lines
    assert report["details"]["rules_applied"]["replace_rules_count"] == 1
    assert report["details"]["rules_applied"]["replacement_applications"] == 2
    assert report["details"]["rules_applied"]["replacement_lines_affected"] == 2


def test_raw_preserves_casing_processed_lowercased(tmp_path):
    """raw_source preserves original casing; processed_source reflects lowercasing."""
    source = tmp_path / "source.txt"
    source.write_text("Hello World\n")

    target = tmp_path / "target.txt"
    target.write_text("GOODBYE WORLD\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    samples = report["samples"]
    assert len(samples) == 1
    s = samples[0]
    # Raw preserves original casing
    assert s["raw_source"] == "Hello World"
    assert s["raw_target"] == "GOODBYE WORLD"
    # Processed is lowercased
    assert s["processed_source"] == "hello world"
    assert s["processed_target"] == "goodbye world"
    # Deprecated aliases match processed
    assert s["source"] == "hello world"
    assert s["target"] == "goodbye world"


def test_drop_lines_regex_with_case_insensitive(tmp_path):
    """drop_lines_regex must match original casing even when case_insensitive=true.

    Regression: the old pipeline lowercased BEFORE evaluating drop_lines_regex,
    so a pattern like \\[HEARTBEAT\\] would not match the lowercased
    '[heartbeat]' and the line would leak through.
    """
    source = tmp_path / "source.txt"
    source.write_text("2025-02-10T06:00:05.000Z [HEARTBEAT] rid=abc status=alive\ndata line one\n")

    target = tmp_path / "target.txt"
    target.write_text("2025-02-10T06:00:07.000Z [HEARTBEAT] rid=xyz status=alive\ndata line one\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
        drop_lines_regex=[r"\[HEARTBEAT\]"],
    )

    report, exit_code = compare_text(cfg)

    # Heartbeat lines are dropped; only "data line one" remains on both sides.
    assert exit_code == 0, f"Expected exit 0, got {exit_code}. Report: {report}"
    assert report["summary"]["different_lines"] == 0
    assert report["summary"]["total_lines_source"] == 1
    assert report["summary"]["total_lines_target"] == 1
    # Both heartbeat lines were dropped
    assert report["details"]["rules_applied"]["drop_lines_count"] == 2


def test_file_not_found_returns_exit_2(tmp_path):
    """Missing files should return exit code 2 with structured error."""
    cfg = TextConfig(
        type="text",
        source=str(tmp_path / "nonexistent.txt"),
        target=str(tmp_path / "also_missing.txt"),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 2
    assert report["error"]["code"] == "RUNTIME_ERROR"
    assert report["error"]["message"]
    assert report["error"]["details"]


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


def test_cli_text_run(tmp_path):
    """CLI run with text config should write report and exit correctly."""
    source = tmp_path / "source.txt"
    source.write_text("hello\nworld\n")
    target = tmp_path / "target.txt"
    target.write_text("hello\nworld\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg_file), "--out", str(report_path)])

    assert result.exit_code == 0
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["type"] == "text"
    assert data["version"] == "1.3"
    assert data["summary"]["different_lines"] == 0
    assert data["summary"]["total_lines_source"] == 2
    # No error field in successful reports
    assert "error" not in data


def test_cli_text_run_with_diffs(tmp_path):
    """CLI run with differences should exit with code 1."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\n")
    target = tmp_path / "target.txt"
    target.write_text("bbb\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg_file), "--out", str(report_path)])

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    assert data["summary"]["different_lines"] == 1


def test_cli_all_diffs_included(tmp_path):
    """CLI includes all diff samples without truncation."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    source.write_text("\n".join(f"s{i}" for i in range(50)) + "\n")
    target.write_text("\n".join(f"t{i}" for i in range(50)) + "\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(
        app,
        ["run", str(cfg_file), "--out", str(report_path)],
    )

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    assert len(data["samples"]) == 50


def test_cli_unordered_report_structure(tmp_path):
    """CLI with unordered_lines should produce samples_agg and unordered_stats in report."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\naaa\nbbb\n")
    target = tmp_path / "target.txt"
    target.write_text("aaa\nbbb\nccc\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\nmode: unordered_lines\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg_file), "--out", str(report_path)])

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    assert data["samples"] == []
    assert "samples_agg" in data
    assert len(data["samples_agg"]) == 2
    assert data["details"]["unordered_stats"]["distinct_mismatched_lines"] == 2
    # No null fields should leak
    assert "error" not in data


def test_cli_null_line_numbers_in_json(tmp_path):
    """Null line_number values must appear as null in the JSON report, not be omitted."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\n")
    target = tmp_path / "target.txt"
    target.write_text("aaa\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg_file), "--out", str(report_path)])

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    sample = data["samples"][0]
    # line_number_target must be explicitly null (present in JSON), not missing
    assert "line_number_target" in sample
    assert sample["line_number_target"] is None


def test_cli_debug_report_flag(tmp_path):
    """--debug-report should include processed_line_number_* in report samples."""
    source = tmp_path / "source.txt"
    source.write_text("# skip\naaa\nbbb\n")
    target = tmp_path / "target.txt"
    target.write_text("aaa\nccc\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        f'type: text\nsource: {source}\ntarget: {target}\ndrop_lines_regex:\n  - "^#"\n'
    )
    report_path = tmp_path / "report.json"

    result = runner.invoke(
        app,
        ["run", str(cfg_file), "--out", str(report_path), "--debug-report"],
    )

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    sample = data["samples"][0]
    assert "processed_line_number_source" in sample
    assert "processed_line_number_target" in sample
    assert sample["processed_line_number_source"] == 2
    assert sample["processed_line_number_target"] == 2


def test_cli_no_include_line_numbers(tmp_path):
    """--no-include-line-numbers should omit line number arrays in unordered mode."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\n")
    target = tmp_path / "target.txt"
    target.write_text("bbb\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\nmode: unordered_lines\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(
        app,
        ["run", str(cfg_file), "--out", str(report_path), "--no-include-line-numbers"],
    )

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    agg = data["samples_agg"]
    assert len(agg) == 2
    assert "source_line_numbers" not in agg[0]


# ---------------------------------------------------------------------------
# Performance-safety tests
# ---------------------------------------------------------------------------


def test_unordered_all_mismatches_collected_and_ordered(tmp_path):
    """All distinct mismatched lines collected with deterministic ordering."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    n = 100
    source.write_text("\n".join(f"line_{i:05d}" for i in range(n)) + "\n")
    target.write_text("")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == n
    assert len(report["samples_agg"]) == n
    assert report["details"]["unordered_stats"]["distinct_mismatched_lines"] == n
    # All diffs are 1, so sorted alphabetically: line_00000, line_00001, ...
    for i, sample in enumerate(report["samples_agg"]):
        assert sample["line"] == f"line_{i:05d}"
        assert sample["source_count"] == 1
        assert sample["target_count"] == 0


# ---------------------------------------------------------------------------
# Audit sample tests (dropped_samples / replacement_samples)
# ---------------------------------------------------------------------------


def test_dropped_samples_populated(tmp_path):
    """drop_lines_regex produces non-empty dropped_samples with correct fields."""
    source = tmp_path / "source.txt"
    source.write_text("# comment\ndata1\n# another\ndata2\n")
    target = tmp_path / "target.txt"
    target.write_text("data1\ndata2\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ds = report["details"]["dropped_samples"]
    assert len(ds) == 2
    assert ds[0]["side"] == "source"
    assert ds[0]["line_number"] == 1
    assert ds[0]["raw"] == "# comment"
    assert ds[0]["processed"] == "# comment"
    assert ds[1]["side"] == "source"
    assert ds[1]["line_number"] == 3
    assert ds[1]["raw"] == "# another"


def test_replacement_samples_populated(tmp_path):
    """replace_regex produces replacement_samples with rules list populated."""
    source = tmp_path / "source.txt"
    source.write_text("id=123 data\nid=456 data\n")
    target = tmp_path / "target.txt"
    target.write_text("id=X data\nid=X data\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        replace_regex=[{"pattern": r"id=\d+", "replace": "id=X"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    rs = report["details"]["replacement_samples"]
    assert len(rs) == 2
    assert rs[0]["side"] == "source"
    assert rs[0]["line_number"] == 1
    assert rs[0]["raw"] == "id=123 data"
    assert rs[0]["processed"] == "id=X data"
    assert len(rs[0]["rules"]) == 1
    assert rs[0]["rules"][0]["pattern"] == r"id=\d+"
    assert rs[0]["rules"][0]["replace"] == "id=X"
    assert rs[0]["rules"][0]["matches"] == 1


def test_audit_samples_all_collected(tmp_path):
    """All 20 dropped lines are collected in dropped_samples."""
    source = tmp_path / "source.txt"
    lines = [f"# drop {i}" for i in range(20)] + ["data\n"]
    source.write_text("\n".join(lines))
    target = tmp_path / "target.txt"
    target.write_text("data\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
    )

    report, _exit_code = compare_text(cfg)

    ds = report["details"]["dropped_samples"]
    assert len(ds) == 20
    # All 20 dropped lines from source
    for i, s in enumerate(ds):
        assert s["side"] == "source"
        assert s["line_number"] == i + 1


def test_dropped_samples_with_case_insensitive(tmp_path):
    """processed in dropped_samples shows pre-case-fold content."""
    source = tmp_path / "source.txt"
    source.write_text("[HEARTBEAT] status=alive\nData Line\n")
    target = tmp_path / "target.txt"
    target.write_text("data line\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
        drop_lines_regex=[r"\[HEARTBEAT\]"],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ds = report["details"]["dropped_samples"]
    assert len(ds) == 1
    # processed is pre-case-fold (the value at the point of drop)
    assert ds[0]["processed"] == "[HEARTBEAT] status=alive"
    assert ds[0]["raw"] == "[HEARTBEAT] status=alive"


def test_replacement_samples_with_case_insensitive(tmp_path):
    """processed in replacement_samples shows post-case-fold for kept lines."""
    source = tmp_path / "source.txt"
    source.write_text("Hello 2024-01-01 World\n")
    target = tmp_path / "target.txt"
    target.write_text("hello DATE world\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True},
        replace_regex=[{"pattern": r"\d{4}-\d{2}-\d{2}", "replace": "DATE"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    rs = report["details"]["replacement_samples"]
    assert len(rs) == 1
    assert rs[0]["raw"] == "Hello 2024-01-01 World"
    # For kept lines, processed is the final comparison value (post-case-fold)
    assert rs[0]["processed"] == "hello date world"
    assert len(rs[0]["rules"]) == 1
    assert rs[0]["rules"][0]["pattern"] == r"\d{4}-\d{2}-\d{2}"
    assert rs[0]["rules"][0]["replace"] == "DATE"


def test_audit_samples_empty_when_no_rules(tmp_path):
    """No rules configured -> both audit sample lists are empty."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\n")
    target = tmp_path / "target.txt"
    target.write_text("aaa\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, _exit_code = compare_text(cfg)

    assert report["details"]["dropped_samples"] == []
    assert report["details"]["replacement_samples"] == []


def test_audit_samples_present_in_unordered_mode(tmp_path):
    """Unordered mode -> dropped_samples and replacement_samples always present."""
    source = tmp_path / "source.txt"
    source.write_text("# comment\nalpha\n")
    target = tmp_path / "target.txt"
    target.write_text("alpha\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
        drop_lines_regex=["^#"],
    )

    report, _exit_code = compare_text(cfg)

    assert "dropped_samples" in report["details"]
    assert "replacement_samples" in report["details"]
    # Dropped sample should have the comment line
    ds = report["details"]["dropped_samples"]
    assert len(ds) == 1
    assert ds[0]["side"] == "source"
    assert ds[0]["raw"] == "# comment"
    # No replacements configured
    assert report["details"]["replacement_samples"] == []


def test_dropped_and_replaced_same_line(tmp_path):
    """A line with both replace+drop appears in both audit sample lists."""
    source = tmp_path / "source.txt"
    # Line has a date (triggers replace) AND starts with # (triggers drop)
    source.write_text("# comment 2024-01-15\ndata\n")
    target = tmp_path / "target.txt"
    target.write_text("data\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        drop_lines_regex=["^#"],
        replace_regex=[{"pattern": r"\d{4}-\d{2}-\d{2}", "replace": "DATE"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ds = report["details"]["dropped_samples"]
    rs = report["details"]["replacement_samples"]
    # The line appears in both lists
    assert len(ds) == 1
    assert ds[0]["line_number"] == 1
    assert ds[0]["raw"] == "# comment 2024-01-15"
    assert ds[0]["processed"] == "# comment DATE"  # post-replace, pre-drop

    assert len(rs) == 1
    assert rs[0]["line_number"] == 1
    assert rs[0]["raw"] == "# comment 2024-01-15"
    assert rs[0]["processed"] == "# comment DATE"  # dropped_content for dropped lines
    assert len(rs[0]["rules"]) == 1
    assert rs[0]["rules"][0]["pattern"] == r"\d{4}-\d{2}-\d{2}"
    assert rs[0]["rules"][0]["replace"] == "DATE"


def test_multi_rule_replacement_single_line(tmp_path):
    """A line matching 2 replacement rules produces 1 sample with 2 rules entries."""
    source = tmp_path / "source.txt"
    source.write_text("id=123 date=2024-01-15\n")
    target = tmp_path / "target.txt"
    target.write_text("id=X date=DATE\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        replace_regex=[
            {"pattern": r"id=\d+", "replace": "id=X"},
            {"pattern": r"\d{4}-\d{2}-\d{2}", "replace": "DATE"},
        ],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ra = report["details"]["rules_applied"]
    assert ra["replace_rules_count"] == 2
    assert ra["replacement_lines_affected"] == 1
    assert ra["replacement_applications"] == 2

    rs = report["details"]["replacement_samples"]
    assert len(rs) == 1
    assert rs[0]["raw"] == "id=123 date=2024-01-15"
    assert rs[0]["processed"] == "id=X date=DATE"
    assert len(rs[0]["rules"]) == 2
    assert rs[0]["rules"][0]["pattern"] == r"id=\d+"
    assert rs[0]["rules"][0]["replace"] == "id=X"
    assert rs[0]["rules"][0]["matches"] == 1
    assert rs[0]["rules"][1]["pattern"] == r"\d{4}-\d{2}-\d{2}"
    assert rs[0]["rules"][1]["replace"] == "DATE"
    assert rs[0]["rules"][1]["matches"] == 1


def test_multi_match_single_rule(tmp_path):
    """A rule matching twice on one line reports matches=2."""
    source = tmp_path / "source.txt"
    source.write_text("id=111 id=222\n")
    target = tmp_path / "target.txt"
    target.write_text("id=X id=X\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        replace_regex=[{"pattern": r"\d+", "replace": "X"}],
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    ra = report["details"]["rules_applied"]
    assert ra["replacement_lines_affected"] == 1
    assert ra["replacement_applications"] == 2

    rs = report["details"]["replacement_samples"]
    assert len(rs) == 1
    assert len(rs[0]["rules"]) == 1
    assert rs[0]["rules"][0]["matches"] == 2


def test_normalize_in_details(tmp_path):
    """details.normalize should reflect the config normalize settings."""
    source = tmp_path / "source.txt"
    source.write_text("Hello World\n")
    target = tmp_path / "target.txt"
    target.write_text("hello world\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        normalize={"case_insensitive": True, "trim_lines": True},
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 0
    norm = report["details"]["normalize"]
    assert norm["case_insensitive"] is True
    assert norm["trim_lines"] is True
    assert norm["collapse_whitespace"] is False
    assert norm["normalize_newlines"] is True


def test_normalize_in_cli_report(tmp_path):
    """CLI report JSON should include details.normalize for text reports."""
    source = tmp_path / "source.txt"
    source.write_text("Hello\n")
    target = tmp_path / "target.txt"
    target.write_text("hello\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        f"type: text\nsource: {source}\ntarget: {target}\nnormalize:\n  case_insensitive: true\n"
    )
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg_file), "--out", str(report_path)])

    assert result.exit_code == 0
    data = json.loads(report_path.read_text())
    assert data["details"]["normalize"]["case_insensitive"] is True


def test_unordered_audit_samples_empty_when_no_rules(tmp_path):
    """Unordered mode with no rules -> both audit sample lists are empty."""
    source = tmp_path / "source.txt"
    source.write_text("aaa\nbbb\n")
    target = tmp_path / "target.txt"
    target.write_text("bbb\nccc\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
    )

    report, _exit_code = compare_text(cfg)

    assert report["details"]["dropped_samples"] == []
    assert report["details"]["replacement_samples"] == []


def test_unordered_dropped_samples_populated(tmp_path):
    """Unordered mode with drop_lines_regex -> dropped_samples populated with correct fields."""
    source = tmp_path / "source.txt"
    source.write_text("# header\nalpha\n# footer\nbeta\n")
    target = tmp_path / "target.txt"
    target.write_text("alpha\nbeta\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
        drop_lines_regex=["^#"],
    )

    report, _exit_code = compare_text(cfg)

    ds = report["details"]["dropped_samples"]
    assert len(ds) == 2
    assert ds[0]["side"] == "source"
    assert ds[0]["line_number"] == 1
    assert ds[0]["raw"] == "# header"
    assert ds[0]["processed"] == "# header"
    assert ds[1]["side"] == "source"
    assert ds[1]["line_number"] == 3
    assert ds[1]["raw"] == "# footer"
    assert report["details"]["replacement_samples"] == []


def test_unordered_replacement_samples_multi_rule(tmp_path):
    """Unordered mode with replace_regex -> replacement_samples with multi-rule support."""
    source = tmp_path / "source.txt"
    source.write_text("id=123 date=2024-01-15\nalpha\n")
    target = tmp_path / "target.txt"
    target.write_text("id=X date=DATE\nalpha\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
        replace_regex=[
            {"pattern": r"id=\d+", "replace": "id=X"},
            {"pattern": r"\d{4}-\d{2}-\d{2}", "replace": "DATE"},
        ],
    )

    report, _exit_code = compare_text(cfg)

    rs = report["details"]["replacement_samples"]
    # Both source and target lines with id/date get replacement samples
    assert len(rs) >= 1
    # Check first sample has the expected multi-rule structure
    src_samples = [s for s in rs if s["side"] == "source"]
    assert len(src_samples) == 1
    assert src_samples[0]["raw"] == "id=123 date=2024-01-15"
    assert src_samples[0]["processed"] == "id=X date=DATE"
    assert len(src_samples[0]["rules"]) == 2
    assert src_samples[0]["rules"][0]["pattern"] == r"id=\d+"
    assert src_samples[0]["rules"][1]["pattern"] == r"\d{4}-\d{2}-\d{2}"


def test_unordered_audit_samples_all_collected(tmp_path):
    """Unordered mode collects all audit samples without truncation."""
    source = tmp_path / "source.txt"
    # 20 lines that will be dropped
    source.write_text("\n".join(f"# line {i}" for i in range(20)) + "\nalpha\n")
    target = tmp_path / "target.txt"
    target.write_text("alpha\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
        mode="unordered_lines",
        drop_lines_regex=["^#"],
    )

    report, _exit_code = compare_text(cfg)

    ds = report["details"]["dropped_samples"]
    assert len(ds) == 20  # all dropped lines collected


def test_all_diffs_collected_line_by_line(tmp_path):
    """All different lines are collected without truncation."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    n = 500
    source.write_text("\n".join(f"src_{i}" for i in range(n)) + "\n")
    target.write_text("\n".join(f"tgt_{i}" for i in range(n)) + "\n")

    cfg = TextConfig(
        type="text",
        source=str(source),
        target=str(target),
    )

    report, exit_code = compare_text(cfg)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == n
    assert len(report["samples"]) == n
