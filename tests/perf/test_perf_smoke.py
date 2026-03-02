"""Lightweight perf smoke tests — verify generated fixtures run correctly.

These use small-scale fixtures (env vars override defaults) so they finish
quickly.  They are skipped unless explicitly selected::

    pytest -m perf          # run these
    make perf-smoke         # same via Makefile
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PERF_ROOT = REPO_ROOT / ".artifacts" / "perf_smoke"
GEN_SCRIPT = REPO_ROOT / "scripts" / "perf" / "gen_perf_fixtures.py"

# Small-scale overrides so smoke tests finish in seconds
_SMOKE_ENV = {
    **os.environ,
    "TABULAR_ROWS": "5000",
    "TABULAR_COLS": "8",
    "TEXT_LINES": "10000",
    "PERF_OUT": str(PERF_ROOT),
    "SEED": "42",
}


@pytest.fixture(scope="session")
def perf_fixtures() -> Path:
    """Generate small-scale fixtures once per session."""
    if not (PERF_ROOT / "tabular_exact_large" / "config.yaml").is_file():
        subprocess.run(
            [sys.executable, str(GEN_SCRIPT)],
            env=_SMOKE_ENV,
            check=True,
            capture_output=True,
        )
    return PERF_ROOT


def _run_case(case_dir: Path) -> tuple[int, dict]:
    """Run reconify on a case directory and return (exit_code, report)."""
    config = case_dir / "config.yaml"
    report_path = case_dir / "report.json"
    report_path.unlink(missing_ok=True)
    result = subprocess.run(
        [sys.executable, "-m", "reconify", "run", str(config), "--out", str(report_path)],
        capture_output=True,
        text=True,
        cwd=str(case_dir),
    )
    assert report_path.is_file(), f"report.json not created for {case_dir.name}"
    report = json.loads(report_path.read_text())
    return result.returncode, report


# ---------------------------------------------------------------------------
# Tabular smoke tests
# ---------------------------------------------------------------------------


@pytest.mark.perf
class TestTabularPerfSmoke:
    def test_exact_large(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "tabular_exact_large")
        assert ec == 0
        assert r["summary"]["rows_with_mismatches"] == 0
        assert r["summary"]["missing_in_target"] == 0

    def test_many_mismatches(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "tabular_many_mismatches")
        assert ec == 1
        assert r["summary"]["rows_with_mismatches"] > 0

    def test_many_missing(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "tabular_many_missing")
        assert ec == 1
        assert r["summary"]["missing_in_target"] > 0 or r["summary"]["missing_in_source"] > 0

    def test_many_excluded(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "tabular_many_excluded")
        assert ec in (0, 1)
        fa = r["details"]["filters_applied"]
        assert fa["source_excluded_rows"] > 0


# ---------------------------------------------------------------------------
# Text smoke tests
# ---------------------------------------------------------------------------


@pytest.mark.perf
class TestTextPerfSmoke:
    def test_line_many_diffs(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "text_line_many_diffs")
        assert ec == 1
        assert r["summary"]["different_lines"] > 0
        assert r["details"]["mode"] == "line_by_line"

    def test_line_many_dropped(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "text_line_many_dropped")
        assert ec == 0
        assert r["details"]["rules_applied"]["drop_lines_count"] > 0
        assert len(r["details"]["dropped_samples"]) > 0

    def test_line_many_replacements(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "text_line_many_replacements")
        assert ec == 0
        assert r["details"]["rules_applied"]["replacement_applications"] > 0
        assert len(r["details"]["replacement_samples"]) > 0

    def test_unordered_large_imbalance(self, perf_fixtures: Path) -> None:
        ec, r = _run_case(perf_fixtures / "text_unordered_large_imbalance")
        assert ec == 1
        assert r["details"]["mode"] == "unordered_lines"
        assert r["summary"]["different_lines"] > 0
        assert "samples_agg" in r
