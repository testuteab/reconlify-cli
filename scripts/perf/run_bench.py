#!/usr/bin/env python3
"""Run reconify benchmarks on generated perf fixtures.

Iterates over case directories under ``.artifacts/perf/``, executes
``reconify run`` via subprocess, and collects timing + report-size metrics.

Usage::

    python scripts/perf/run_bench.py                  # all cases
    python scripts/perf/run_bench.py --filter tabular  # tabular only
    python scripts/perf/run_bench.py --filter text     # text only
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

PERF_ROOT = Path(os.environ.get("PERF_OUT", ".artifacts/perf"))


def _find_cases(root: Path, filt: str | None) -> list[Path]:
    """Return sorted list of case directories that have a config.yaml."""
    cases = sorted(
        d for d in root.iterdir() if d.is_dir() and (d / "config.yaml").is_file()
    )
    if filt:
        cases = [c for c in cases if filt in c.name]
    return cases


def _run_case(case_dir: Path) -> dict:
    """Execute reconify on a single case and return metrics."""
    config = (case_dir / "config.yaml").resolve()
    report = (case_dir / "report.json").resolve()
    report.unlink(missing_ok=True)

    t0 = time.monotonic()
    ru_before = resource.getrusage(resource.RUSAGE_CHILDREN)

    result = subprocess.run(
        [sys.executable, "-m", "reconify", "run", str(config), "--out", str(report)],
        capture_output=True,
        text=True,
        cwd=str(case_dir.resolve()),
    )

    elapsed = time.monotonic() - t0
    ru_after = resource.getrusage(resource.RUSAGE_CHILDREN)

    # maxrss is in bytes on macOS, KB on Linux
    peak_rss_kb = ru_after.ru_maxrss - ru_before.ru_maxrss
    if sys.platform == "darwin":
        peak_rss_kb = peak_rss_kb // 1024  # bytes -> KB

    report_size = report.stat().st_size if report.is_file() else 0

    # Extract summary counts from report
    summary_info = ""
    if report.is_file():
        try:
            data = json.loads(report.read_text())
            s = data.get("summary", {})
            rtype = data.get("type", "")
            if rtype == "tabular":
                miss_t = s.get("missing_in_target", "?")
                miss_s = s.get("missing_in_source", "?")
                mm = s.get("rows_with_mismatches", "?")
                summary_info = (
                    f"src={s.get('source_rows', '?')}"
                    f" tgt={s.get('target_rows', '?')}"
                    f" miss_t={miss_t} miss_s={miss_s}"
                    f" mismatch={mm}"
                )
            elif rtype == "text":
                src = s.get("total_lines_source", "?")
                tgt = s.get("total_lines_target", "?")
                diffs = s.get("different_lines", "?")
                summary_info = (
                    f"src={src} tgt={tgt} diffs={diffs}"
                )
        except Exception:
            summary_info = "(parse error)"

    return {
        "case": case_dir.name,
        "exit_code": result.returncode,
        "elapsed_s": round(elapsed, 3),
        "report_bytes": report_size,
        "report_mb": round(report_size / (1024 * 1024), 2),
        "peak_rss_delta_kb": max(peak_rss_kb, 0),
        "summary": summary_info,
        "stderr": result.stderr.strip()[-200:] if result.returncode == 2 else "",
    }


def _print_table(results: list[dict]) -> None:
    """Print a nicely-formatted console table."""
    hdr = (
        f"{'Case':<40} {'Exit':>4} {'Time(s)':>8}"
        f" {'Report':>10} {'RSS delta':>10}  Summary"
    )
    print(hdr)
    print("-" * len(hdr) + "-" * 40)
    for r in results:
        rb = r["report_bytes"]
        report_str = (
            f"{r['report_mb']:.1f} MB" if r["report_mb"] >= 1
            else f"{rb // 1024} KB"
        )
        rss = r["peak_rss_delta_kb"]
        rss_str = (
            f"{rss // 1024} MB" if rss > 1024 else f"{rss} KB"
        )
        print(
            f"{r['case']:<40} {r['exit_code']:>4}"
            f" {r['elapsed_s']:>8.2f} {report_str:>10}"
            f" {rss_str:>10}  {r['summary']}"
        )
        if r["stderr"]:
            print(f"  stderr: {r['stderr']}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reconify perf benchmarks")
    parser.add_argument("--filter", type=str, default=None, help="Filter cases by substring")
    parser.add_argument("--root", type=str, default=str(PERF_ROOT), help="Perf fixtures root")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.is_dir():
        print(f"ERROR: perf fixtures not found at {root}")
        print("Run `make perf-gen` first to generate fixtures.")
        sys.exit(1)

    cases = _find_cases(root, args.filter)
    if not cases:
        print(f"No cases found in {root}" + (f" matching '{args.filter}'" if args.filter else ""))
        sys.exit(1)

    print(f"Running {len(cases)} benchmark(s) from {root.resolve()}\n")

    results: list[dict] = []
    for case_dir in cases:
        print(f"  Running {case_dir.name} ...", end=" ", flush=True)
        r = _run_case(case_dir)
        print(f"{r['elapsed_s']:.2f}s  exit={r['exit_code']}")
        results.append(r)

    print()
    _print_table(results)

    # Write JSON summary
    summary_path = root / "bench_summary.json"
    summary_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    main()
