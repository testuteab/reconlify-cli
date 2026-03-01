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


def test_tabular_report_csv_defaults() -> None:
    """Tabular report details.csv should reflect effective CSV settings (defaults)."""
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    report = build_report(cfg)
    assert report.details.csv is not None
    assert report.details.csv.delimiter == ","
    assert report.details.csv.encoding == "utf-8"
    assert report.details.csv.header is True


def test_tabular_report_csv_custom() -> None:
    """Tabular report details.csv should reflect custom CSV settings from config."""
    cfg = TabularConfig(
        type="tabular",
        source="a.csv",
        target="b.csv",
        keys=["id"],
        csv={"delimiter": "|", "encoding": "utf-8", "header": False},
    )
    report = build_report(cfg)
    assert report.details.csv is not None
    assert report.details.csv.delimiter == "|"
    assert report.details.csv.encoding == "utf-8"
    assert report.details.csv.header is False


def test_tabular_report_csv_in_json() -> None:
    """details.csv must serialize into the JSON report for tabular configs."""
    cfg = TabularConfig(
        type="tabular",
        source="a.csv",
        target="b.csv",
        keys=["id"],
        csv={"delimiter": "\t", "encoding": "utf-8", "header": True},
    )
    report = build_report(cfg)
    data = json.loads(report.model_dump_json())
    assert data["details"]["csv"] == {
        "delimiter": "\t",
        "encoding": "utf-8",
        "header": True,
    }


def test_text_report_no_csv_field() -> None:
    """Text reports must not include details.csv."""
    from reconify.report import write_report

    cfg = TextConfig(type="text", source="a.txt", target="b.txt")
    report = build_report(cfg)
    # Model-level: text details has no csv attribute
    assert not hasattr(report.details, "csv") or getattr(report.details, "csv", None) is None


def test_tabular_details_csv_optional_backward_compat() -> None:
    """TabularDetails with csv=None should parse fine (backward compatibility)."""
    from reconify.models import TabularDetails

    # Simulate an older report that has no csv field
    details = TabularDetails(format="csv", keys=["id"])
    assert details.csv is None


def test_write_report_tabular_includes_csv(tmp_path) -> None:
    """write_report must include details.csv in the JSON for tabular reports."""
    from reconify.report import write_report

    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    report = build_report(cfg)
    path = str(tmp_path / "report.json")
    write_report(report, path)
    data = json.loads((tmp_path / "report.json").read_text())
    assert "csv" in data["details"]
    assert data["details"]["csv"]["delimiter"] == ","
    assert data["details"]["csv"]["encoding"] == "utf-8"
    assert data["details"]["csv"]["header"] is True


def test_write_report_text_omits_csv(tmp_path) -> None:
    """write_report must omit details.csv from the JSON for text reports."""
    from reconify.report import write_report

    cfg = TextConfig(type="text", source="a.txt", target="b.txt")
    report = build_report(cfg)
    path = str(tmp_path / "report.json")
    write_report(report, path)
    data = json.loads((tmp_path / "report.json").read_text())
    assert "csv" not in data["details"]


def test_text_details_audit_samples_default_empty() -> None:
    """New dropped_samples and replacement_samples fields default to []."""
    from reconify.models import TextDetails

    details = TextDetails()
    assert details.dropped_samples == []
    assert details.replacement_samples == []


def test_text_details_with_audit_samples() -> None:
    """TextDetails accepts and validates the new sample types."""
    from reconify.models import TextDetails, TextDroppedSample, TextReplacementSample

    dropped = [
        TextDroppedSample(side="source", line_number=1, raw="# comment", processed="# comment"),
    ]
    replaced = [
        TextReplacementSample(
            side="target",
            line_number=2,
            raw="id=123",
            processed="id=X",
            pattern=r"\d+",
            replace="X",
        ),
    ]
    details = TextDetails(dropped_samples=dropped, replacement_samples=replaced)
    assert len(details.dropped_samples) == 1
    assert details.dropped_samples[0].side == "source"
    assert details.dropped_samples[0].line_number == 1
    assert len(details.replacement_samples) == 1
    assert details.replacement_samples[0].pattern == r"\d+"
    assert details.replacement_samples[0].replace == "X"


def test_config_hash_deterministic() -> None:
    cfg = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    assert config_hash(cfg) == config_hash(cfg)
    assert len(config_hash(cfg)) == 64  # SHA-256 hex


def test_config_hash_differs_for_different_configs() -> None:
    cfg1 = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id"])
    cfg2 = TabularConfig(type="tabular", source="a.csv", target="b.csv", keys=["id", "name"])
    assert config_hash(cfg1) != config_hash(cfg2)
