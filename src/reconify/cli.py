"""Reconify CLI entrypoint."""

from __future__ import annotations

import traceback
from pathlib import Path

import typer
import yaml
from pydantic import TypeAdapter, ValidationError

from reconify.models import (
    ReconConfig,
    ReconError,
    TabularConfig,
    TextConfig,
    UnorderedStats,
)
from reconify.report import build_error_report, build_report, config_hash, write_report

_adapter = TypeAdapter(ReconConfig)

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
    sample_limit: int = typer.Option(
        2000,
        "--sample-limit",
        help="Maximum number of sample diffs to include in the report.",
    ),
    include_line_numbers: bool = typer.Option(
        True,
        "--include-line-numbers/--no-include-line-numbers",
        help="Include original file line numbers in report samples.",
    ),
    max_line_numbers: int = typer.Option(
        10,
        "--max-line-numbers",
        help="Maximum line numbers stored per distinct line in unordered mode.",
    ),
    debug_report: bool = typer.Option(
        False,
        "--debug-report",
        help="Include debug fields (processed line numbers) in report samples.",
    ),
) -> None:
    """Load a reconciliation config, validate it, and generate a report."""
    out_path = str(out) if out else "report.json"

    # --- Phase 1: read config file ---
    try:
        raw_yaml = config_path.read_text()
    except FileNotFoundError:
        report = build_error_report(
            config_type=None,
            error_code="CONFIG_VALIDATION_ERROR",
            error_message=f"Config file not found: {config_path}",
            error_details=f"FileNotFoundError: {config_path}",
        )
        write_report(report, out_path)
        typer.echo(f"Error report written to {out_path}", err=True)
        raise SystemExit(2) from None
    except Exception as exc:
        report = build_error_report(
            config_type=None,
            error_code="RUNTIME_ERROR",
            error_message=f"Failed to read config file: {exc}",
            error_details=traceback.format_exc(),
        )
        write_report(report, out_path)
        typer.echo(f"Error report written to {out_path}", err=True)
        raise SystemExit(2) from None

    # --- Phase 2: parse and validate config ---
    raw_dict = yaml.safe_load(raw_yaml)
    config_type = raw_dict.get("type") if isinstance(raw_dict, dict) else None

    try:
        if not isinstance(raw_dict, dict):
            msg = "Config file must contain a YAML mapping"
            raise ValueError(msg)
        cfg = _adapter.validate_python(raw_dict)
    except (ValidationError, ValueError) as exc:
        report = build_error_report(
            config_type=config_type,
            error_code="CONFIG_VALIDATION_ERROR",
            error_message="Config validation failed",
            error_details=str(exc),
            raw_config=raw_yaml,
        )
        write_report(report, out_path)
        typer.echo(f"Config validation failed. Error report written to {out_path}", err=True)
        raise SystemExit(2) from None

    # --- Phase 3: run comparison ---
    try:
        if isinstance(cfg, TextConfig):
            _run_text(
                cfg,
                sample_limit,
                out_path,
                include_line_numbers=include_line_numbers,
                max_line_numbers=max_line_numbers,
                debug_report=debug_report,
            )
        elif isinstance(cfg, TabularConfig):
            _run_tabular(cfg, out_path)
    except SystemExit:
        raise
    except Exception as exc:
        report = build_error_report(
            config_type=config_type,
            error_code="RUNTIME_ERROR",
            error_message=str(exc),
            error_details=traceback.format_exc(),
            raw_config=raw_yaml,
        )
        write_report(report, out_path)
        typer.echo(f"Runtime error. Error report written to {out_path}", err=True)
        raise SystemExit(2) from None


def _run_text(
    cfg: TextConfig,
    sample_limit: int,
    out_path: str,
    *,
    include_line_numbers: bool = True,
    max_line_numbers: int = 10,
    debug_report: bool = False,
) -> None:
    """Execute text engine comparison and write report."""
    from reconify.text_engine import compare_text

    result, exit_code = compare_text(
        cfg,
        sample_limit=sample_limit,
        include_line_numbers=include_line_numbers,
        max_line_numbers=max_line_numbers,
        debug_report=debug_report,
    )

    h = config_hash(cfg)
    report = build_report(cfg)

    # Merge engine results into the report
    report.summary = type(report.summary)(**result["summary"])

    details_kwargs: dict = {
        "mode": result["details"]["mode"],
        "read_lines_source": result["details"]["read_lines_source"],
        "read_lines_target": result["details"]["read_lines_target"],
        "ignored_blank_lines_source": result["details"]["ignored_blank_lines_source"],
        "ignored_blank_lines_target": result["details"]["ignored_blank_lines_target"],
        "rules_applied": type(report.details.rules_applied)(**result["details"]["rules_applied"]),
    }
    if result["details"].get("unordered_stats"):
        details_kwargs["unordered_stats"] = UnorderedStats(**result["details"]["unordered_stats"])
    report.details = type(report.details)(**details_kwargs)

    report.samples = result["samples"]
    if result.get("samples_agg") is not None:
        report.samples_agg = result["samples_agg"]
    if result.get("error"):
        report.error = ReconError(**result["error"])

    write_report(report, out_path)
    typer.echo(f"Loaded config: {cfg.type}")
    typer.echo(f"Report written to {out_path} (config_hash={h})")
    raise SystemExit(exit_code)


def _run_tabular(cfg: TabularConfig, out_path: str) -> None:
    """Execute tabular engine comparison and write report."""
    from reconify.models import TabularDetails, TabularFiltersApplied, TabularSummary
    from reconify.tabular_engine import compare_tabular

    result, exit_code = compare_tabular(cfg)

    h = config_hash(cfg)
    report = build_report(cfg)

    # Merge engine results into the report
    report.summary = TabularSummary(**result["summary"])
    report.details = TabularDetails(
        format=result["details"]["format"],
        keys=result["details"]["keys"],
        compared_columns=result["details"]["compared_columns"],
        read_rows_source=result["details"]["read_rows_source"],
        read_rows_target=result["details"]["read_rows_target"],
        filters_applied=TabularFiltersApplied(**result["details"]["filters_applied"]),
    )

    # For tabular, samples is a dict of lists
    report.samples = result["samples"]

    if result.get("column_stats"):
        report.details.column_stats = result["column_stats"]

    if result.get("error"):
        report.error = ReconError(**result["error"])

    write_report(report, out_path)
    typer.echo(f"Loaded config: {cfg.type}")
    typer.echo(f"Report written to {out_path} (config_hash={h})")
    raise SystemExit(exit_code)
