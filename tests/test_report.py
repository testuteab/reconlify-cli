"""Unit tests for report building and hashing."""

import json

from reconify.models import TabularConfig, TextConfig
from reconify.report import build_report, config_hash


def test_tabular_report_structure() -> None:
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    report = build_report(cfg)
    data = json.loads(report.model_dump_json())
    assert data["type"] == "tabular"
    assert data["version"] == "1.1"
    assert "generated_at" in data
    assert "config_hash" in data
    assert data["summary"]["source_rows"] == 0
    assert data["summary"]["missing_in_target"] == 0
    assert data["details"]["keys"] == ["id"]
    assert data["details"]["format"] == "csv"
    assert data["samples"] == []


def test_text_report_structure() -> None:
    cfg = TextConfig(type="text", source="a.txt", target="b.txt")
    report = build_report(cfg)
    data = json.loads(report.model_dump_json())
    assert data["type"] == "text"
    assert data["details"]["mode"] == "line_by_line"
    assert data["details"]["rules_applied"]["drop_lines_count"] == 0


def test_text_rules_applied_starts_at_zero() -> None:
    """build_report must not seed rules_applied with config counts (they are runtime counts)."""
    cfg = TextConfig(
        type="text",
        source="a.txt",
        target="b.txt",
        drop_lines_regex=["^#", "^//"],
        replace_regex=[{"pattern": "foo", "replace": "bar"}],
    )
    report = build_report(cfg)
    assert report.details.rules_applied.drop_lines_count == 0
    assert report.details.rules_applied.replace_rules_count == 0


def test_build_report_text_rules_applied_starts_at_zero() -> None:
    from reconify.models import TextNormalize

    cfg = TextConfig(
        type="text",
        source="a.txt",
        target="b.txt",
        drop_lines_regex=["foo"],
        replace_regex=[{"pattern": "bar", "replace": "baz"}],
        normalize=TextNormalize(),
    )

    report = build_report(cfg)

    assert report.details.rules_applied.drop_lines_count == 0
    assert report.details.rules_applied.replace_rules_count == 0


def test_config_hash_deterministic() -> None:
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    assert config_hash(cfg) == config_hash(cfg)
    assert len(config_hash(cfg)) == 64  # SHA-256 hex


def test_config_hash_differs_for_different_configs() -> None:
    cfg1 = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    cfg2 = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id", "name"])
    assert config_hash(cfg1) != config_hash(cfg2)
