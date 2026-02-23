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


def test_unordered_sample_limit(tmp_path):
    """samples_agg should respect sample_limit."""
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

    report, exit_code = compare_text(cfg, sample_limit=5)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == 20
    assert len(report["samples_agg"]) == 5
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


def test_sample_limit(tmp_path):
    """Samples should be capped at sample_limit."""
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

    report, exit_code = compare_text(cfg, sample_limit=5)

    assert exit_code == 1
    assert report["summary"]["different_lines"] == 100
    assert len(report["samples"]) == 5


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
    # 3 replacements in source, 0 in target (already "ID")
    assert report["details"]["rules_applied"]["replace_rules_count"] == 3


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
    assert data["version"] == "1.1"
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


def test_cli_sample_limit_option(tmp_path):
    """--sample-limit should be passed through to text engine."""
    source = tmp_path / "source.txt"
    target = tmp_path / "target.txt"
    source.write_text("\n".join(f"s{i}" for i in range(50)) + "\n")
    target.write_text("\n".join(f"t{i}" for i in range(50)) + "\n")

    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(
        app,
        ["run", str(cfg_file), "--out", str(report_path), "--sample-limit", "3"],
    )

    assert result.exit_code == 1
    data = json.loads(report_path.read_text())
    assert len(data["samples"]) == 3


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
