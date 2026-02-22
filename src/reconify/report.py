"""Report building and config hashing for Reconify V1."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from reconify.models import (
    ReconReport,
    TabularConfig,
    TabularDetails,
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
            details=TabularDetails(),
        )

    return ReconReport(
        type="text",
        generated_at=now,
        config_hash=h,
        summary=TextSummary(),
        details=TextDetails(
            mode=cfg.mode.value,
            rules_applied=TextRulesApplied(
                drop_lines_count=len(cfg.drop_lines_regex),
                replace_rules_count=len(cfg.replace_regex),
            ),
        ),
    )


def write_report(report: ReconReport, path: str) -> None:
    """Serialize report to a JSON file."""
    with open(path, "w") as f:
        f.write(report.model_dump_json(indent=2))
        f.write("\n")
