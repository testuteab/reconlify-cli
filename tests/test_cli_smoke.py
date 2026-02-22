"""Smoke tests for the Reconify CLI."""

from typer.testing import CliRunner

from reconify.cli import app

runner = CliRunner()


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "reconciliation" in result.output.lower()


def test_run_with_valid_config(tmp_path) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text("type: row_match\n")
    result = runner.invoke(app, ["run", str(cfg)])
    assert result.exit_code == 0
    assert "row_match" in result.output


def test_run_with_missing_type(tmp_path) -> None:
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("foo: bar\n")
    result = runner.invoke(app, ["run", str(cfg)])
    assert result.exit_code != 0
