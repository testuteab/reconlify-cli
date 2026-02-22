"""Reconify CLI entrypoint."""

from pathlib import Path

import typer

from reconify.config_loader import load_config

app = typer.Typer(help="Reconify - rule-based data reconciliation.", invoke_without_command=True)


@app.callback()
def main() -> None:
    """Reconify - rule-based data reconciliation."""


@app.command()
def run(config_path: Path = typer.Argument(..., help="Path to a YAML config file.")) -> None:
    """Load a reconciliation config and print its type."""
    cfg = load_config(config_path)
    typer.echo(f"Loaded config: {cfg.type}")
