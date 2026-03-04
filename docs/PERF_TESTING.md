# Performance Testing

Reconlify ships with a suite of deterministic performance benchmarks that
stress-test both the tabular and text engines at scale. Test data is
generated on demand into `.artifacts/perf/` (gitignored) and never committed.

## Quick Reference

```bash
make perf-smoke          # lightweight pytest smoke tests (~10s)
make perf                # full benchmark: generate data + run all cases
make perf-tabular        # benchmark tabular cases only
make perf-text           # benchmark text cases only
make perf-gen            # generate fixtures without running benchmarks
make perf-clean          # delete all generated perf data
```

## Test Cases

### Tabular

| Case | What it stresses | Expected exit |
|------|-----------------|---------------|
| `tabular_exact_large` | Baseline: identical source/target, large row count | 0 |
| `tabular_many_mismatches` | High mismatch rate across many columns | 1 |
| `tabular_many_missing` | Many missing rows on both sides (missing + extra keys) | 1 |
| `tabular_many_excluded` | Heavy filtering: 100 exclude_keys + row_filter on status | 0 or 1 |

### Text

| Case | What it stresses | Expected exit |
|------|-----------------|---------------|
| `text_line_many_diffs` | line_by_line with ~30% positional diffs | 1 |
| `text_line_many_dropped` | line_by_line with ~20% lines matching drop_lines_regex | 0 |
| `text_line_many_replacements` | line_by_line with ~40% lines needing regex replacements (timestamps + UIDs) | 0 |
| `text_unordered_large_imbalance` | unordered_lines with skewed distribution across 1000 distinct lines | 1 |

## Scaling via Environment Variables

All knobs have sensible defaults. Override them to run larger or smaller:

| Variable | Default | Description |
|----------|---------|-------------|
| `TABULAR_ROWS` | 200000 | Rows per tabular source file |
| `TABULAR_COLS` | 10 | Columns per tabular file |
| `TEXT_LINES` | 500000 | Lines per text source file |
| `MISMATCH_RATE` | 0.3 | Fraction of rows/lines with value differences |
| `MISSING_RATE` | 0.1 | Fraction of source keys missing from target |
| `EXTRA_RATE` | 0.1 | Fraction of extra keys in target |
| `DROPPED_RATE` | 0.2 | Fraction of text lines matching drop regex |
| `REPLACE_RATE` | 0.4 | Fraction of text lines with replaceable content |
| `SEED` | 1337 | RNG seed for deterministic generation |
| `PERF_OUT` | `.artifacts/perf` | Output directory for fixtures |

Example — run a larger benchmark:

```bash
TABULAR_ROWS=1000000 TEXT_LINES=2000000 make perf
```

Example — quick sanity check with small data:

```bash
TABULAR_ROWS=10000 TEXT_LINES=20000 make perf
```

## Smoke Tests (pytest)

The `make perf-smoke` target runs pytest tests marked `@pytest.mark.perf`.
These use small-scale fixtures (5K tabular rows, 10K text lines) and verify
that each case produces a valid report with the expected exit code and
non-zero counts where applicable. They run in under 30 seconds.

These tests are **not** included in `make test` or `make e2e` — they only
run when explicitly selected.

## Interpreting Results

The benchmark runner (`scripts/perf/run_bench.py`) prints a table with:

| Column | Meaning |
|--------|---------|
| Exit | CLI exit code (0 = no diffs, 1 = diffs, 2 = error) |
| Time(s) | Wall-clock seconds for the `reconlify run` subprocess |
| Report | Size of the generated `report.json` |
| RSS delta | Approximate peak-memory delta (best-effort via `resource` module) |
| Summary | Key counts from the report (rows, mismatches, diffs, etc.) |

A JSON summary is also written to `.artifacts/perf/bench_summary.json` for
programmatic analysis or CI comparison.

### What to look for

- **Time regression:** Compare wall-clock times across runs. The same seed
  produces identical data, so time differences reflect code changes.
- **Report size growth:** After removing sample limits, reports include all
  diffs. Watch for unexpected size jumps that indicate a logic change.
- **Exit code 2:** Indicates a runtime error — investigate immediately.
- **RSS delta:** Gives a rough sense of memory usage. The `resource` module
  reports cumulative child process maxrss, so deltas are approximate.

### Determinism

With the same `SEED` and scale variables, the generator produces
byte-identical files. This means benchmark results are reproducible across
runs on the same machine (modulo system load variance).

---

## Baseline Results (default scale)

Measured on a MacBook with default settings: `TABULAR_ROWS=200000`,
`TABULAR_COLS=10`, `TEXT_LINES=500000`, `SEED=1337`.

### Raw output

```
Case                                     Exit  Time(s)     Report  RSS delta  Summary
-----------------------------------------------------------------------------------------------------------------------------
tabular_exact_large                         0     2.00       1 KB     217 MB  src=200000 tgt=200000 miss_t=0 miss_s=0 mismatch=0
tabular_many_excluded                       1     7.14    54.0 MB     101 MB  src=160197 tgt=160234 miss_t=1638 miss_s=1675 mismatch=55565
tabular_many_mismatches                     1    11.95    84.0 MB     318 MB  src=200000 tgt=200000 miss_t=0 miss_s=0 mismatch=190639
tabular_many_missing                        1     2.80    16.2 MB       0 KB  src=200000 tgt=199789 miss_t=20211 miss_s=20000 mismatch=0
text_line_many_diffs                        1     2.70    57.8 MB       0 KB  src=500000 tgt=500000 diffs=149405
text_line_many_dropped                      0     2.64    30.2 MB       0 KB  src=399959 tgt=399959 diffs=0
text_line_many_replacements                 0    30.14   180.8 MB     263 MB  src=500000 tgt=500000 diffs=0
text_unordered_large_imbalance              1     0.82     548 KB       0 KB  src=250000 tgt=250000 diffs=280642
```

### Tabular engine analysis (200K rows, 10 columns)

| Case | Time | Report | Analysis |
|------|------|--------|----------|
| **exact_large** | 2.0s | 1 KB | Baseline. DuckDB loads both CSVs, does a full outer join, finds zero diffs. Fast because the report is tiny (just summary counts). 217 MB RSS is DuckDB's in-memory footprint for ~30 MB of CSV data. |
| **many_missing** | 2.8s | 16 MB | ~20K missing on each side. Slightly slower than baseline because it serializes 40K sample rows into the report. The SQL join itself is still fast. |
| **many_excluded** | 7.1s | 54 MB | 100 exclude_keys + row_filter on `status=cancelled`. Filtering drops ~40K rows per side, then the remaining 160K rows still have ~56K mismatches (from the 5% mutation rate). The 7s comes from both the filtering overhead and serializing 55K mismatch samples with per-column diffs. |
| **many_mismatches** | 12.0s | 84 MB | The slowest tabular case. 190K of 200K rows have at least one mismatched column (30% per-cell rate). The engine must diff every column on every mismatched row and serialize all of it. The 84 MB report and 318 MB RSS reflect the cost of collecting every single mismatch without truncation. |

**Takeaway:** DuckDB's join/compare is not the bottleneck — report
serialization is. The `many_mismatches` case takes 6x longer than
`exact_large` because it writes 84 MB of JSON. Before sample limits were
removed, this would have been capped at ~200 samples.

### Text engine analysis (500K lines)

| Case | Time | Report | Analysis |
|------|------|--------|----------|
| **unordered_large_imbalance** | 0.8s | 548 KB | Fastest. Building a Counter of 250K lines per side and diffing the counts is O(n). Only ~1000 distinct line templates means the `samples_agg` list is small. |
| **line_many_dropped** | 2.6s | 30 MB | 500K lines, ~20% are comments matched by `drop_lines_regex`. The 0-diff exit is correct (source=target after drops). The 30 MB report comes from ~100K `dropped_samples` audit entries (both sides combined). |
| **line_many_diffs** | 2.7s | 58 MB | 500K lines, ~150K positional diffs. Each diff sample has raw + processed content for both sides. At ~400 bytes per sample, 150K samples = ~58 MB. |
| **line_many_replacements** | 30.1s | 181 MB | **By far the slowest case.** 500K lines, ~40% have two regex replacements each (timestamp + UID patterns). That's ~400K lines x 2 patterns = 800K+ regex calls via Python `re.sub()`. The 181 MB report stores `replacement_samples` with full rule details for every affected line. 263 MB RSS spike confirms heavy string allocation during regex processing. |

**Takeaway:** Regex replacement is the clear performance bottleneck. The
`replace_regex` pipeline runs pure Python `re.sub()` line-by-line — at
500K lines with 40% hit rate and 2 patterns, that's ~800K regex calls.
The 30s runtime is ~15x slower than the non-regex cases. This is the most
important optimization target if performance matters.

### Key observations

1. **Report size is the new concern** after removing sample limits. The
   `many_mismatches` tabular case produces 84 MB and the
   `many_replacements` text case produces 181 MB. For production use at
   larger scales, these could become problematic.

2. **Regex is expensive.** The `text_line_many_replacements` case at 30s
   is an outlier — everything else finishes under 12s. If users have heavy
   `replace_regex` usage at scale, this will be the bottleneck.

3. **DuckDB is efficient.** The tabular engine handles 200K x 10 joins in
   2s baseline. The cost scales with output volume, not input volume.

4. **Memory is reasonable.** Peak RSS stays under 320 MB even for the
   heaviest cases. The tabular engine's DuckDB footprint (~200 MB for
   30 MB of CSV) is the main memory consumer.

---

## File Layout

```
scripts/perf/
  gen_perf_fixtures.py    # Data generator
  run_bench.py            # Benchmark runner

tests/perf/
  test_perf_smoke.py      # pytest smoke tests (@pytest.mark.perf)

.artifacts/perf/          # Generated (gitignored)
  tabular_exact_large/
    config.yaml
    source.csv
    target.csv
    report.json           # After benchmark run
  tabular_many_mismatches/
  tabular_many_missing/
  tabular_many_excluded/
  text_line_many_diffs/
  text_line_many_dropped/
  text_line_many_replacements/
  text_unordered_large_imbalance/
  bench_summary.json      # After benchmark run
```
