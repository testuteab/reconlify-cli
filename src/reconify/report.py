"""Report building and config hashing for Reconify V1."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Literal

from reconify.models import (
    ReconError,
    ReconReport,
    ReportCsvInfo,
    RowFiltersInfo,
    TabularConfig,
    TabularDetails,
    TabularFiltersApplied,
    TabularSummary,
    TextConfig,
    TextDetails,
    TextRulesApplied,
    TextSummary,
)


def config_hash(cfg: TabularConfig | TextConfig) -> str:
    """SHA-256 of the canonical JSON representation of a config."""
    canonical = cfg.model_dump_json(exclude_none=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def build_report(cfg: TabularConfig | TextConfig) -> ReconReport:
    """Build an empty but structurally valid report from a validated config."""
    now = datetime.now(UTC).isoformat()
    h = config_hash(cfg)

    if cfg.type == "tabular":
        return ReconReport(
            type="tabular",
            generated_at=now,
            config_hash=h,
            summary=TabularSummary(),
            details=TabularDetails(
                format=cfg.format,
                keys=list(cfg.keys),
                filters_applied=TabularFiltersApplied(
                    exclude_keys_count=len(cfg.filters.exclude_keys),
                    row_filters=RowFiltersInfo(
                        count=len(cfg.filters.row_filters.rules),
                        apply_to=cfg.filters.row_filters.apply_to,
                        mode=cfg.filters.row_filters.mode,
                    )
                    if (cfg.filters.row_filters and cfg.filters.row_filters.rules)
                    else None,
                ),
                csv=ReportCsvInfo(
                    delimiter=cfg.csv.delimiter,
                    encoding=cfg.csv.encoding,
                    header=cfg.csv.header,
                ),
            ),
        )

    return ReconReport(
        type="text",
        generated_at=now,
        config_hash=h,
        summary=TextSummary(),
        details=TextDetails(
            mode=cfg.mode.value,
            rules_applied=TextRulesApplied(),
        ),
    )


def build_error_report(
    *,
    config_type: str | None,
    error_code: str,
    error_message: str,
    error_details: str,
    raw_config: str = "",
) -> ReconReport:
    """Build a report that contains error information with zeroed-out summaries."""
    now = datetime.now(UTC).isoformat()
    h = hashlib.sha256(raw_config.encode()).hexdigest() if raw_config else ""

    report_type: Literal["tabular", "text"] = (
        config_type if config_type in ("tabular", "text") else "text"  # type: ignore[assignment]
    )
    error = ReconError(code=error_code, message=error_message, details=error_details)

    if report_type == "tabular":
        return ReconReport(
            type="tabular",
            generated_at=now,
            config_hash=h,
            summary=TabularSummary(),
            details=TabularDetails(),
            error=error,
        )

    return ReconReport(
        type="text",
        generated_at=now,
        config_hash=h,
        summary=TextSummary(),
        details=TextDetails(),
        error=error,
    )


def write_report(report: ReconReport, path: str) -> None:
    """Serialize report to a JSON file.

    Optional model-level fields (error, samples_agg, unordered_stats) are
    omitted when absent.  Explicit ``null`` values inside sample dicts
    (e.g. line_number_source for a missing side) are preserved in the
    JSON output.
    """
    data = report.model_dump(mode="json")

    # Strip known optional fields that are None at specific levels
    for key in ("error", "samples_agg"):
        if key in data and data[key] is None:
            del data[key]
    details = data.get("details")
    if isinstance(details, dict):
        if "unordered_stats" in details and details["unordered_stats"] is None:
            del details["unordered_stats"]
        if "csv" in details and details["csv"] is None:
            del details["csv"]
        # Omit row_filters from filters_applied when not enabled
        fa = details.get("filters_applied")
        if isinstance(fa, dict) and "row_filters" in fa and fa["row_filters"] is None:
            del fa["row_filters"]

    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
