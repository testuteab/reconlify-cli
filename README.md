[![Home Page](https://img.shields.io/badge/Website-reconlify.com-blue)](https://reconlify.com/)

# Reconlify CLI

**Rule-based data reconciliation for exported files. Local-first. Deterministic.**

Reconlify compares structured datasets — CSV exports, report outputs, migration snapshots — using declarative YAML rules. It matches rows by key, tolerates expected noise, and produces a deterministic JSON report suitable for CI/CD pipelines.

No data leaves your machine.

## 30-Second Example

Two systems export the same trades, but with different column names, different row order, and minor numeric formatting differences:

**source.csv**

```
trade_id,amount,currency
T001,100,USD
T002,200.00,USD
```

**target.csv**

```
id,total_amount,currency
T002,200.02,USD
T001,100.00,USD
```

**config.yaml**

```yaml
type: tabular
source: source.csv
target: target.csv

keys:
  - trade_id

column_mapping:
  trade_id: id
  amount: total_amount

tolerance:
  amount: 0.05
```

```bash
pip install reconlify-cli
reconlify run config.yaml
# Exit code: 0 — no meaningful differences
```

**Why exit code 0?**

- Rows are matched by key (`trade_id`), not by position — row order does not matter
- `column_mapping` aligns `trade_id` to `id` and `amount` to `total_amount`
- `100` vs `100.00` are equal after numeric casting
- `200.00` vs `200.02` — the difference of `0.02` is within the configured tolerance of `0.05`

A line-based `diff` would flag every line. Reconlify finds zero meaningful differences.

## Why Reconlify Exists

If you work with exported data, you have probably written ad-hoc comparison scripts more than once:

- Validating a data migration by comparing before/after CSV exports
- Checking that an ETL pipeline still produces the same output after a code change
- Reconciling financial exports between two systems
- Comparing log files where timestamps or formatting vary

These comparisons share a pattern: you need to match rows by key, ignore harmless noise (whitespace, casing, small rounding differences), and produce a clear report of what actually changed.

Line-based tools like `diff` compare by position and treat any byte-level difference as a mismatch. That means reordered rows, renamed columns, trailing whitespace, and `100` vs `100.00` all show up as differences — even when the data is semantically identical.

Reconlify handles this with declarative YAML rules instead of throwaway scripts.

## How does Reconlify compare?

| Capability | diff | csvdiff | Excel Compare | Beyond Compare | Datafold | **Reconlify** |
|---|---|---|---|---|---|---|
| Understands tabular datasets | No | Yes | Yes | Yes | Yes | **Yes** |
| Key-based row matching | No | Yes | Manual | Yes | Yes | **Yes** |
| Detects missing rows | No | Yes | Manual | Partial | Yes | **Yes** |
| Column-level mismatch detection | No | Yes | Manual | Partial | Yes | **Yes** |
| Rule-based normalization | No | No | No | No | No | **Yes** |
| Regex transformations | No | No | No | No | No | **Yes** |
| Numeric tolerance | No | No | No | Yes | Yes | **Yes** |
| Noise filtering | No | No | Manual | Manual | Partial | **Yes** |
| Deterministic JSON reconciliation report | No | No | No | No | Partial | **Yes** |
| Works with exported files | Yes | Yes | Yes | Yes | No | **Yes** |
| Database integration | No | No | No | No | **Yes** | Planned |
| CI/CD automation ready | Yes | Partial | No | No | Yes | **Yes** |
| Schema-aware column mapping | No | No | Manual | Partial | Partial | **Yes** |
| Local-first execution | Yes | Yes | Yes | Yes | No | **Yes** |

Tools like Datafold are designed for comparing database tables inside data warehouses. Reconlify focuses on a different problem: validating **exported files** produced by pipelines, migrations, or financial systems — locally, deterministically, and without requiring database access.

## Core Capabilities

- **Key-based row matching** — single or composite keys; row order does not matter
- **Column mapping** — compare files with different column names via `column_mapping`
- **Missing row detection** — identifies rows present on one side but not the other
- **Column-level mismatch reporting** — pinpoints which columns differ, with source and target values
- **Numeric tolerance** — per-column absolute tolerance (e.g. `amount: 0.01`)
- **Normalization rules** — trim whitespace, case-insensitive comparison, null normalization, regex extraction
- **Source-side virtual columns** — generate computed columns via `normalization` pipelines (`concat`, `substr`, `map`, `round`, and more)
- **Row filters** — exclude rows by key value or column-level filter rules
- **Column control** — include, exclude, or ignore specific columns
- **Deterministic JSON reports** — same inputs and config always produce the same report
- **Text engine** — line-by-line or unordered comparison for log files and text outputs
- **CI/CD ready** — exit codes `0` (match) / `1` (differences) / `2` (error)
- **Fully local** — no network calls, no data upload

## Column Mapping

*Released in v0.1.1.*

When source and target files use different column names for the same data, `column_mapping` declares the correspondence:

```yaml
column_mapping:
  trade_id: id            # source "trade_id" matches target "id"
  amount: total_amount    # source "amount" matches target "total_amount"
```

All other config fields — `keys`, `tolerance`, `string_rules`, `compare.include_columns` — use source-side (logical) column names. The mapping only affects how target columns are resolved.

This works with all existing features: tolerance, string rules, normalization, and column controls all apply to the logical column name.

## Installation

Requires **Python 3.11+**.

```bash
pip install reconlify-cli
```

Or with [pipx](https://pipx.pypa.io/) for isolated installs:

```bash
pipx install reconlify-cli
```

## CLI Usage

```bash
reconlify run <config.yaml>                # default output: report.json
reconlify run <config.yaml> --out out.json # custom output path
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No differences found |
| 1 | Differences found |
| 2 | Error (config validation, file not found, runtime failure) |

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out PATH` | `report.json` | Output path for the JSON report |
| `--include-line-numbers` / `--no-include-line-numbers` | on | Include original line numbers in text report samples |
| `--max-line-numbers N` | `0` (unlimited) | Max line numbers per distinct line in unordered mode |
| `--debug-report` | off | Include processed line numbers in text report samples |

## Common Use Cases

**Migration validation** — Export tables before and after a migration. Reconlify matches rows by key and reports exactly what changed, ignoring column renames and expected rounding.

**ETL regression testing** — Run your pipeline, compare the output against a known-good snapshot. Add tolerance for acceptable numeric drift. Automate it in CI.

**Financial reconciliation** — Compare exports from two systems (ERP vs ledger, internal vs external). Use normalization to handle whitespace, casing, and NULL representation differences.

**Report comparison** — Verify that a report generator produces the same output after a code change, even if column order or formatting varies.

**Log comparison** — Use the text engine with `unordered_lines` mode to compare log files where line order is non-deterministic. Apply `drop_lines_regex` to strip timestamps.

## Real-World Example

A finance team exports transactions from two systems and needs to reconcile them nightly:

```yaml
type: tabular
source: erp_export.csv
target: ledger_export.csv

keys:
  - txn_id

compare:
  trim_whitespace: true
  case_insensitive: true
  normalize_nulls: ["", "NULL", "null"]
  exclude_columns:
    - memo

filters:
  row_filters:
    both:
      - column: status
        op: equals
        value: CANCELLED
```

This config matches rows by `txn_id`, ignores whitespace and casing differences, treats blank and `"NULL"` as equivalent, skips the free-text `memo` column, and filters out cancelled transactions before comparison.

```bash
reconlify run recon.yaml --out report.json
```

The JSON report contains summary counts, per-column mismatch statistics, and concrete sample rows for every category of difference.

## Documentation

- [User Guide](https://github.com/testuteab/reconlify-cli/blob/main/docs/RECONLIFY_CLI_USER_GUIDE_v1.md) — In-depth guide covering both engines and best practices
- [Column Mapping](https://github.com/testuteab/reconlify-cli/blob/main/docs/COLUMN_MAPPING_V1_1.md) — Column mapping semantics, examples, and limitations
- [YAML Config Schema](https://github.com/testuteab/reconlify-cli/blob/main/docs/YAML_SCHEMA_v1.md) — Full reference for all configuration options
- [Report Schema](https://github.com/testuteab/reconlify-cli/blob/main/docs/REPORT_SCHEMA_v1.md) — Complete specification of the JSON report format
- [Performance Testing](https://github.com/testuteab/reconlify-cli/blob/main/docs/PERF_TESTING.md) — Benchmark methodology and baseline results

## Current Scope

Reconlify currently supports:

- **Tabular:** CSV and TSV files (any delimiter)
- **Text:** line-by-line and unordered line comparison
- **Execution:** local only, single-machine

Not currently in scope: direct database connections, Excel/Parquet file formats, cloud execution, or multi-user workflows. See the [changelog](https://github.com/testuteab/reconlify-cli/blob/main/CHANGELOG.md) for what has shipped.

## Development

```bash
make install       # install dependencies
make test          # unit + integration tests (excludes e2e and perf)
make e2e           # end-to-end CLI tests
make lint          # ruff linter
make format        # auto-fix lint + format
make clean         # remove build artifacts and caches
```

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Testute AB.
