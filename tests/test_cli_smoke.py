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
    cfg = tmp_path / "config.yaml"
    cfg.write_text("type: text\nsource: a.txt\ntarget: b.txt\n")
    report_path = tmp_path / "report.json"
    result = runner.invoke(app, ["run", str(cfg), "--out", str(report_path)])
    assert result.exit_code == 0
    assert "text" in result.output
    data = json.loads(report_path.read_text())
    assert data["type"] == "text"
    assert data["details"]["mode"] == "line_by_line"


def test_run_missing_key_fails() -> None:
    """Tabular config without key must fail with exit code 2."""
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        cfg = Path(td) / "bad.yaml"
        cfg.write_text("type: tabular\nsource: a.csv\ntarget: b.csv\n")
        result = runner.invoke(app, ["run", str(cfg)])
        assert result.exit_code == 2


def test_run_invalid_type_fails(tmp_path) -> None:
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("type: unknown\nsource: a\ntarget: b\n")
    result = runner.invoke(app, ["run", str(cfg)])
    assert result.exit_code == 2


def test_run_missing_file_fails() -> None:
    result = runner.invoke(app, ["run", "/nonexistent/path.yaml"])
    assert result.exit_code == 2
