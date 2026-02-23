"""Unit tests for report building and hashing."""

import json

from reconify.models import TabularConfig, TextConfig
from reconify.report import build_report, config_hash


def test_tabular_report_structure() -> None:
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", key=["id"])
    report = build_report(cfg)
    data = json.loads(report.model_dump_json())
    assert data["type"] == "tabular"
    assert data["version"] == "1.1"
    assert "generated_at" in data
    assert "config_hash" in data
    assert data["summary"]["total_rows_source"] == 0
    assert data["summary"]["matched_rows"] == 0
    assert data["details"]["column_stats"] == {}
    assert data["samples"] == []


def test_text_report_structure() -> None:
    cfg = TextConfig(type="text", source="a.txt", target="b.txt")
    report = build_report(cfg)
    data = json.loads(report.model_dump_json())
    assert data["type"] == "text"
    assert data["details"]["mode"] == "line_by_line"
    assert data["details"]["rules_applied"]["drop_lines_count"] == 0


def test_config_hash_deterministic() -> None:
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", key=["id"])
    assert config_hash(cfg) == config_hash(cfg)
    assert len(config_hash(cfg)) == 64  # SHA-256 hex


def test_config_hash_differs_for_different_configs() -> None:
    cfg1 = TabularConfig(type="tabular", source="a.csv", target="b.csv", key=["id"])
    cfg2 = TabularConfig(type="tabular", source="a.csv", target="b.csv", key=["id", "name"])
    assert config_hash(cfg1) != config_hash(cfg2)
