#!/usr/bin/env python3
"""Generate deterministic performance-test fixtures for reconify-cli.

All env-var knobs have sensible defaults. Output lands under
``.artifacts/perf/`` (gitignored) by default.

Usage::

    python scripts/perf/gen_perf_fixtures.py          # defaults
    TABULAR_ROWS=500000 python scripts/perf/gen_perf_fixtures.py
"""

from __future__ import annotations

import csv
import io
import os
import random
import textwrap
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Env-var knobs
# ---------------------------------------------------------------------------

SEED = int(os.environ.get("SEED", "1337"))
TABULAR_ROWS = int(os.environ.get("TABULAR_ROWS", "200000"))
TABULAR_COLS = int(os.environ.get("TABULAR_COLS", "10"))
TEXT_LINES = int(os.environ.get("TEXT_LINES", "500000"))
MISMATCH_RATE = float(os.environ.get("MISMATCH_RATE", "0.3"))
MISSING_RATE = float(os.environ.get("MISSING_RATE", "0.1"))
EXTRA_RATE = float(os.environ.get("EXTRA_RATE", "0.1"))
DROPPED_RATE = float(os.environ.get("DROPPED_RATE", "0.2"))
REPLACE_RATE = float(os.environ.get("REPLACE_RATE", "0.4"))
OUT_ROOT = Path(os.environ.get("PERF_OUT", ".artifacts/perf"))

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]
STATUSES = ["active", "pending", "settled", "cancelled", "reversed"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    path.write_text(buf.getvalue())


def _write_yaml(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip())


def _tabular_header(ncols: int) -> list[str]:
    base = ["id", "account", "amount", "currency", "booking_date", "memo", "status"]
    extra = [f"field_{i}" for i in range(max(0, ncols - len(base)))]
    return (base + extra)[:ncols]


def _tabular_row(rng: random.Random, row_id: int, header: list[str]) -> list[str]:
    vals: list[str] = []
    for col in header:
        if col == "id":
            vals.append(str(row_id))
        elif col == "account":
            vals.append(f"ACC-{rng.randint(10000, 99999)}")
        elif col == "amount":
            vals.append(f"{rng.uniform(1, 100000):.2f}")
        elif col == "currency":
            vals.append(rng.choice(CURRENCIES))
        elif col == "booking_date":
            y = rng.randint(2020, 2025)
            m = rng.randint(1, 12)
            d = rng.randint(1, 28)
            vals.append(f"{y}-{m:02d}-{d:02d}")
        elif col == "memo":
            vals.append(f"memo-{rng.randint(0, 999999):06d}")
        elif col == "status":
            vals.append(rng.choice(STATUSES))
        else:
            vals.append(f"v{rng.randint(0, 9999)}")
    return vals


def _mutate_row(rng: random.Random, row: list[str], header: list[str], rate: float) -> list[str]:
    """Return a copy with some non-key cells randomly changed."""
    out = list(row)
    for i in range(1, len(row)):  # skip id
        if rng.random() < rate:
            col = header[i]
            if col == "amount":
                out[i] = f"{rng.uniform(1, 100000):.2f}"
            elif col == "currency":
                out[i] = rng.choice(CURRENCIES)
            elif col == "status":
                out[i] = rng.choice(STATUSES)
            else:
                out[i] = f"changed-{rng.randint(0, 999999)}"
    return out


# ---------------------------------------------------------------------------
# Tabular generators
# ---------------------------------------------------------------------------


def gen_tabular_exact_large(root: Path, rng: random.Random) -> Path:
    """Identical source/target — tests baseline speed."""
    d = _ensure(root / "tabular_exact_large")
    header = _tabular_header(TABULAR_COLS)
    rows = [_tabular_row(rng, i, header) for i in range(TABULAR_ROWS)]
    _write_csv(d / "source.csv", header, rows)
    _write_csv(d / "target.csv", header, rows)
    _write_yaml(
        d / "config.yaml",
        """\
        type: tabular
        source: source.csv
        target: target.csv
        keys: [id]
        """,
    )
    return d


def gen_tabular_many_mismatches(root: Path, rng: random.Random) -> Path:
    """Same keys, many columns differ."""
    d = _ensure(root / "tabular_many_mismatches")
    header = _tabular_header(TABULAR_COLS)
    src_rows = [_tabular_row(rng, i, header) for i in range(TABULAR_ROWS)]
    tgt_rows = [_mutate_row(rng, r, header, MISMATCH_RATE) for r in src_rows]
    _write_csv(d / "source.csv", header, src_rows)
    _write_csv(d / "target.csv", header, tgt_rows)
    _write_yaml(
        d / "config.yaml",
        """\
        type: tabular
        source: source.csv
        target: target.csv
        keys: [id]
        """,
    )
    return d


def gen_tabular_many_missing(root: Path, rng: random.Random) -> Path:
    """Target missing MISSING_RATE keys; target has EXTRA_RATE extra keys."""
    d = _ensure(root / "tabular_many_missing")
    header = _tabular_header(TABULAR_COLS)
    n = TABULAR_ROWS

    src_rows = [_tabular_row(rng, i, header) for i in range(n)]

    # Target: drop MISSING_RATE of source rows, add EXTRA_RATE new rows
    keep_mask = [rng.random() >= MISSING_RATE for _ in range(n)]
    tgt_rows = [r for r, keep in zip(src_rows, keep_mask, strict=True) if keep]
    extra_start = n
    n_extra = int(n * EXTRA_RATE)
    for i in range(n_extra):
        tgt_rows.append(_tabular_row(rng, extra_start + i, header))

    _write_csv(d / "source.csv", header, src_rows)
    _write_csv(d / "target.csv", header, tgt_rows)
    _write_yaml(
        d / "config.yaml",
        """\
        type: tabular
        source: source.csv
        target: target.csv
        keys: [id]
        """,
    )
    return d


def gen_tabular_many_excluded(root: Path, rng: random.Random) -> Path:
    """Heavy filtering via exclude_keys + row_filters."""
    d = _ensure(root / "tabular_many_excluded")
    header = _tabular_header(TABULAR_COLS)
    src_rows = [_tabular_row(rng, i, header) for i in range(TABULAR_ROWS)]
    tgt_rows = [_mutate_row(rng, r, header, 0.05) for r in src_rows]
    _write_csv(d / "source.csv", header, src_rows)
    _write_csv(d / "target.csv", header, tgt_rows)

    # Exclude first 100 specific keys + all rows with status=cancelled
    # Build config as a dict and use yaml.dump for correct serialization
    config = {
        "type": "tabular",
        "source": "source.csv",
        "target": "target.csv",
        "keys": ["id"],
        "filters": {
            "exclude_keys": [{"id": str(i)} for i in range(100)],
            "row_filters": {
                "apply_to": "both",
                "mode": "exclude",
                "rules": [
                    {"column": "status", "op": "equals", "value": "cancelled"},
                ],
            },
        },
    }
    with open(d / "config.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    return d


# ---------------------------------------------------------------------------
# Text generators
# ---------------------------------------------------------------------------


def gen_text_line_many_diffs(root: Path, rng: random.Random) -> Path:
    """line_by_line with MISMATCH_RATE positional diffs."""
    d = _ensure(root / "text_line_many_diffs")
    src_lines: list[str] = []
    tgt_lines: list[str] = []
    for i in range(TEXT_LINES):
        line = f"line-{i:08d} data={rng.randint(0, 999999):06d}"
        src_lines.append(line)
        if rng.random() < MISMATCH_RATE:
            tgt_lines.append(f"line-{i:08d} data=CHANGED-{rng.randint(0, 999999):06d}")
        else:
            tgt_lines.append(line)
    (d / "source.txt").write_text("\n".join(src_lines) + "\n")
    (d / "target.txt").write_text("\n".join(tgt_lines) + "\n")
    _write_yaml(
        d / "config.yaml",
        """\
        type: text
        source: source.txt
        target: target.txt
        mode: line_by_line
        """,
    )
    return d


def gen_text_line_many_dropped(root: Path, rng: random.Random) -> Path:
    """line_by_line with DROPPED_RATE lines matching drop_lines_regex."""
    d = _ensure(root / "text_line_many_dropped")
    lines: list[str] = []
    for i in range(TEXT_LINES):
        if rng.random() < DROPPED_RATE:
            lines.append(f"# COMMENT line {i}")
        else:
            lines.append(f"data-{i:08d} value={rng.randint(0, 999999):06d}")
    content = "\n".join(lines) + "\n"
    (d / "source.txt").write_text(content)
    (d / "target.txt").write_text(content)
    _write_yaml(
        d / "config.yaml",
        """\
        type: text
        source: source.txt
        target: target.txt
        mode: line_by_line
        drop_lines_regex:
          - "^# COMMENT"
        """,
    )
    return d


def gen_text_line_many_replacements(root: Path, rng: random.Random) -> Path:
    """line_by_line with REPLACE_RATE lines containing timestamps/ids to replace."""
    d = _ensure(root / "text_line_many_replacements")
    src_lines: list[str] = []
    tgt_lines: list[str] = []
    for i in range(TEXT_LINES):
        if rng.random() < REPLACE_RATE:
            ts_src = f"2024-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}"
            ts_tgt = f"2025-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}"
            uid_src = f"{rng.randint(0, 0xFFFFFFFF):08x}"
            uid_tgt = f"{rng.randint(0, 0xFFFFFFFF):08x}"
            src_lines.append(f"event-{i} ts={ts_src} uid={uid_src} ok")
            tgt_lines.append(f"event-{i} ts={ts_tgt} uid={uid_tgt} ok")
        else:
            line = f"event-{i} static-data-{rng.randint(0, 999999):06d}"
            src_lines.append(line)
            tgt_lines.append(line)
    (d / "source.txt").write_text("\n".join(src_lines) + "\n")
    (d / "target.txt").write_text("\n".join(tgt_lines) + "\n")
    # Use single-quoted YAML strings to avoid backslash escape issues
    _write_yaml(
        d / "config.yaml",
        """\
        type: text
        source: source.txt
        target: target.txt
        mode: line_by_line
        replace_regex:
          - pattern: '\\d{4}-\\d{2}-\\d{2}'
            replace: "DATE"
          - pattern: 'uid=[0-9a-f]{8}'
            replace: "uid=<ID>"
        """,
    )
    return d


def gen_text_unordered_large_imbalance(root: Path, rng: random.Random) -> Path:
    """unordered_lines with large count imbalances for a subset of lines."""
    d = _ensure(root / "text_unordered_large_imbalance")

    # 1000 distinct line templates; source/target get varying counts
    n_distinct = 1000
    templates = [f"entry-{i:04d} payload={rng.randint(0, 999999):06d}" for i in range(n_distinct)]

    src_lines: list[str] = []
    tgt_lines: list[str] = []
    per_side = TEXT_LINES // 2
    for _ in range(per_side):
        # Source: uniform random pick
        src_lines.append(rng.choice(templates))
    for _ in range(per_side):
        # Target: skewed — first 200 templates get most picks
        if rng.random() < 0.7:
            tgt_lines.append(templates[rng.randint(0, 199)])
        else:
            tgt_lines.append(rng.choice(templates))

    (d / "source.txt").write_text("\n".join(src_lines) + "\n")
    (d / "target.txt").write_text("\n".join(tgt_lines) + "\n")
    _write_yaml(
        d / "config.yaml",
        """\
        type: text
        source: source.txt
        target: target.txt
        mode: unordered_lines
        """,
    )
    return d


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ALL_GENERATORS = [
    ("tabular_exact_large", gen_tabular_exact_large),
    ("tabular_many_mismatches", gen_tabular_many_mismatches),
    ("tabular_many_missing", gen_tabular_many_missing),
    ("tabular_many_excluded", gen_tabular_many_excluded),
    ("text_line_many_diffs", gen_text_line_many_diffs),
    ("text_line_many_dropped", gen_text_line_many_dropped),
    ("text_line_many_replacements", gen_text_line_many_replacements),
    ("text_unordered_large_imbalance", gen_text_unordered_large_imbalance),
]


def main() -> None:
    root = _ensure(OUT_ROOT)
    rng = random.Random(SEED)

    print(f"Generating perf fixtures into {root.resolve()}")
    print(f"  SEED={SEED}  TABULAR_ROWS={TABULAR_ROWS}  TABULAR_COLS={TABULAR_COLS}")
    print(f"  TEXT_LINES={TEXT_LINES}  MISMATCH_RATE={MISMATCH_RATE}")
    print(f"  MISSING_RATE={MISSING_RATE}  EXTRA_RATE={EXTRA_RATE}")
    print(f"  DROPPED_RATE={DROPPED_RATE}  REPLACE_RATE={REPLACE_RATE}")
    print()

    manifest: list[str] = []
    for name, gen_fn in ALL_GENERATORS:
        print(f"  Generating {name} ...", end=" ", flush=True)
        d = gen_fn(root, rng)
        files = sorted(d.iterdir())
        sizes = {f.name: f.stat().st_size for f in files}
        total_mb = sum(sizes.values()) / (1024 * 1024)
        print(f"{total_mb:.1f} MB  ({', '.join(f'{k}={v/1024:.0f}K' for k, v in sizes.items())})")
        manifest.append(str(d))

    print()
    print("Manifest:")
    for p in manifest:
        print(f"  {p}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
