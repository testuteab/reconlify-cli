"""Smoke tests for the Reconify CLI."""

import json

from typer.testing import CliRunner

from reconify.cli import app

runner = CliRunner()


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "reconciliation" in result.output.lower()


def test_run_subcommand_help() -> None:
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "--out" in result.output


def test_run_tabular_min(tmp_path) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text("type: tabular\nsource: a.csv\ntarget: b.csv\nkey:\n  - id\n")
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])
    assert result.exit_code == 0
    assert "tabular" in result.output
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["type"] == "tabular"
    assert data["version"] == "1.0"
    assert "config_hash" in data
    assert data["summary"]["total_rows_source"] == 0


def test_run_text_min(tmp_path) -> None:
    source = tmp_path / "a.txt"
    source.write_text("hello\n")
    target = tmp_path / "b.txt"
    target.write_text("hello\n")
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"type: text\nsource: {source}\ntarget: {target}\n")
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])
    assert result.exit_code == 0
    assert "text" in result.output
    data = json.loads(report_path.read_text())
    assert data["type"] == "text"
    assert data["details"]["mode"] == "line_by_line"


def test_run_missing_key_fails(tmp_path) -> None:
    """Tabular config without key must fail with exit code 2."""
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("type: tabular\nsource: a.csv\ntarget: b.csv\n")
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])
    assert result.exit_code == 2


def test_run_invalid_type_fails(tmp_path) -> None:
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("type: unknown\nsource: a\ntarget: b\n")
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])
    assert result.exit_code == 2


def test_run_missing_file_fails(tmp_path) -> None:
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", "/nonexistent/path.yaml", "--out", str(report_path)])
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# Error report generation
# ---------------------------------------------------------------------------


def test_error_report_config_validation(tmp_path) -> None:
    """Invalid YAML config should produce report.json with error.code."""
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("type: tabular\nsource: a.csv\ntarget: b.csv\n")  # missing key
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])

    assert result.exit_code == 2
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["error"]["code"] == "CONFIG_VALIDATION_ERROR"
    assert data["error"]["message"]  # non-empty
    assert data["error"]["details"]  # non-empty
    assert data["type"] == "tabular"
    assert data["summary"]["total_rows_source"] == 0
    assert data["samples"] == []


def test_error_report_missing_config_file(tmp_path) -> None:
    """Missing config file should produce error report with exit code 2."""
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(tmp_path / "nope.yaml"), "--out", str(report_path)])

    assert result.exit_code == 2
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["error"]["code"] == "CONFIG_VALIDATION_ERROR"
    assert "not found" in data["error"]["message"].lower()


def test_error_report_text_file_not_found(tmp_path) -> None:
    """Text config pointing to missing data files should produce error report."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text("type: text\nsource: /no/such/file.txt\ntarget: /no/such/other.txt\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])

    assert result.exit_code == 2
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["error"]["code"] == "RUNTIME_ERROR"
    assert data["summary"]["total_lines_source"] == 0


def test_error_report_invalid_type(tmp_path) -> None:
    """Unknown type should produce error report."""
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("type: unknown\nsource: a\ntarget: b\n")
    report_path = tmp_path / "report.json"

    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])

    assert result.exit_code == 2
    assert report_path.exists()
    data = json.loads(report_path.read_text())
    assert data["error"]["code"] == "CONFIG_VALIDATION_ERROR"
