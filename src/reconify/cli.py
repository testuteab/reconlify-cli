"""Reconify CLI entrypoint."""

from __future__ import annotations

from pathlib import Path

import typer
from pydantic import ValidationError

from reconify.config_loader import load_config
from reconify.report import build_report, config_hash, write_report

app = typer.Typer(help="Reconify - rule-based data reconciliation.", invoke_without_command=True)


@app.callback()
def main() -> None:
    """Reconify - rule-based data reconciliation."""


@app.command()
def run(
    config_path: Path = typer.Argument(..., help="Path to a YAML config file."),
    out: Path | None = typer.Option(
        None,
        "--out",
        help="Output path for the JSON report (default: report.json in cwd).",
    ),
) -> None:
    """Load a reconciliation config, validate it, and generate a report."""
    try:
        cfg = load_config(config_path)
    except ValidationError as exc:
        typer.echo(f"Config validation failed:\n{exc}", err=True)
        raise SystemExit(2) from None
    except FileNotFoundError:
        typer.echo(f"Config file not found: {config_path}", err=True)
        raise SystemExit(2) from None

    report = build_report(cfg)
    out_path = str(out) if out else "report.json"
    write_report(report, out_path)

    h = config_hash(cfg)
    typer.echo(f"Loaded config: {cfg.type}")
    typer.echo(f"Report written to {out_path} (config_hash={h})")
