# Reconlify CLI

**Semantic data reconciliation for the command line.**

Validate structured datasets using declarative YAML rules and produce deterministic JSON reconciliation reports suitable for CI/CD pipelines.

Fully local. No data leaves your machine.

Typical use cases:

- ETL validation
- Data migration verification
- Financial transaction reconciliation
- CI pipeline dataset checks
- Log comparison with normalization rules

## Quick Example

**source.csv**

```
txn_id,amount
1,100
2,200
```

**target.csv**

```
txn_id,amount
1,100
2,210
```

**config.yaml**

```yaml
type: tabular
source: source.csv
target: target.csv
keys:
  - txn_id
```

```bash
reconlify run config.yaml
```

Exit code: **1** (differences found). Report highlights:

```
rows_with_mismatches: 1
missing_in_source:    0
missing_in_target:    0
```

## How Reconlify Compares

| Capability | `diff` / `difflib` | `csvdiff` | Beyond Compare | **Reconlify** |
|---|---|---|---|---|
| Understands tabular datasets | No | Yes | Partial | **Yes** |
| Key-based row matching | No | Yes | No | **Yes** |
| Detects missing rows | No | Yes | Partial | **Yes** |
| Rule-based normalization | No | No | No | **Yes** |
| Numeric tolerance | No | No | No | **Yes** |
| Regex replacements | No | No | No | **Yes** |
| Noise filtering | No | No | Manual | **Yes** |
| Structured JSON report | No | No | No | **Yes** |
| Automation / CI friendly | Yes | Partial | No | **Yes** |
| Local-first | Yes | Yes | Yes | **Yes** |

## Features

- Key-based dataset reconciliation (single or composite keys)
- Automatic missing-row detection (both directions)
- Column-level mismatch detection with include/exclude control
- Numeric tolerance support (per-column absolute tolerance)
- Normalization rules (trim, case-insensitive, null handling, regex, virtual columns)
- Row filters and exclusions
- Deterministic JSON reconciliation reports
- Machine-readable exit codes (0 / 1 / 2)
- Two engines: **tabular** (CSV/TSV) and **text** (line-by-line / unordered)
- CI/CD pipeline friendly
- Fully local execution — no network calls

## Performance

Reconlify uses DuckDB-backed tabular processing, streaming text comparison, and a local-first architecture. It processes large datasets locally without requiring a database or external service.

| Dataset | Rows / Lines | Mode | Time |
|---------|-------------|------|------|
| CSV reconciliation (exact match) | 200k rows | tabular | ~2 s |
| CSV reconciliation (high mismatch) | 200k rows | tabular | ~12 s |
| Log comparison (positional diffs) | 500k lines | line_by_line | ~3 s |
| Log comparison (unordered) | 250k lines | unordered_lines | < 1 s |

Benchmarks were executed on a MacBook (Apple Silicon / Python 3.11) with default fixture settings.

Performance depends on dataset structure, rule complexity, and system hardware.

Full benchmark methodology and results: [PERF_TESTING.md](https://github.com/testuteab/reconlify-cli/blob/main/docs/PERF_TESTING.md)

## Installation

Requires **Python 3.11+**.

```bash
pip install reconlify-cli
```

Or with [pipx](https://pipx.pypa.io/) for isolated installs:

```bash
pipx install reconlify-cli
```

> **Package name on PyPI:** `reconlify-cli`

For development:

```bash
git clone https://github.com/testuteab/reconlify-cli.git && cd reconlify-cli
make install          # runs: poetry install
reconlify --help
```

## CLI Usage

```bash
reconlify run <config.yaml>                # default output: report.json
reconlify run <config.yaml> --out out.json # custom output path
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out PATH` | `report.json` | Output path for the JSON report |
| `--include-line-numbers` / `--no-include-line-numbers` | on | Include original line numbers in text report samples |
| `--max-line-numbers N` | `0` (unlimited) | Max line numbers per distinct line in unordered mode |
| `--debug-report` | off | Include processed line numbers in text report samples |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No differences found |
| 1 | Differences found |
| 2 | Error (config validation, file not found, runtime failure) |

### Version

```bash
reconlify --version
# reconlify 0.1.0
```

Reconlify follows [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`.

## Real-World Example: Accounting Transaction Reconciliation

A finance team exports transactions from two systems (ERP and bank ledger) and needs to reconcile them nightly. Here's how Reconlify handles it.

### 1. Create sample data

```bash
cat <<'EOF' > source.csv
txn_id,booking_date,account,amount,currency,counterparty,status,memo
TXN-001,2026-01-15,4100,1500.00,USD,Acme Corp,BOOKED,Invoice 9201
TXN-002,2026-01-16,4100,320.50,EUR,Globex Inc,BOOKED,Wire transfer
TXN-003,2026-01-17,4200,75.00,USD,Jane Doe,BOOKED,Expense report
TXN-004,2026-01-18,4100,10000.00,USD,Initech,CANCELLED,Reversed
TXN-005,2026-01-19,4300,249.99,USD,Umbrella Ltd,BOOKED,Subscription
TXN-006,2026-01-20,4100,,USD,Soylent Corp,BOOKED,Pending allocation
EOF

cat <<'EOF' > target.csv
txn_id,booking_date,account,amount,currency,counterparty,status,memo
TXN-001,2026-01-15,4100,1500.00,USD,  acme corp  ,BOOKED,Invoice 9201
TXN-002,2026-01-16,4100,320.75,EUR,Globex Inc,BOOKED,Wire transfer
TXN-003,2026-01-17,4200,75.00,USD,Jane Doe,BOOKED,Expense report
TXN-005,2026-01-19,4300,249.99,USD,Umbrella Ltd,BOOKED,Subscription
TXN-006,2026-01-20,4100,NULL,USD,Soylent Corp,BOOKED,Pending allocation
TXN-007,2026-01-21,4100,430.00,USD,Wayne Ent,BOOKED,New deposit
EOF
```

What's different between the two files:

| Scenario | Row | Detail |
|---|---|---|
| Matches after normalization | TXN-001 | `counterparty` has extra spaces + lowercase in target — should match with trim + case rules |
| Value mismatch | TXN-002 | `amount` is `320.50` vs `320.75` (exceeds tolerance) |
| Exact match | TXN-003 | Identical |
| Filtered out | TXN-004 | Status `CANCELLED` — excluded by row filter |
| Exact match | TXN-005 | Identical |
| NULL normalization | TXN-006 | Source has blank `amount`, target has `NULL` string — should match |
| Missing in source | TXN-007 | Exists only in target |

### 2. Create the reconciliation config

```bash
cat <<'EOF' > recon.yaml
type: tabular
source: source.csv
target: target.csv

keys:
  - txn_id

csv:
  delimiter: ","
  header: true
  encoding: utf-8

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
EOF
```

Key config choices:

- **`keys: [txn_id]`** — match rows by transaction ID
- **`trim_whitespace` + `case_insensitive`** — ignore formatting noise
- **`normalize_nulls`** — treat blank, `NULL`, and `null` as equivalent
- **`exclude_columns: [memo]`** — skip free-text fields
- **`row_filters`** — drop `CANCELLED` transactions before comparison

### 3. Run the reconciliation

```bash
reconlify run recon.yaml --out report.json
echo $?
```

**Expected exit code: `1`** (differences found).

The report will contain:

- **missing_in_target: 0** — TXN-004 was filtered out, so not counted
- **missing_in_source: 1** — TXN-007 exists only in target
- **rows_with_mismatches: 1** — TXN-002 has an `amount` mismatch (`320.50` vs `320.75`)

TXN-001 and TXN-006 match cleanly thanks to normalization rules.

The report is written to `report.json`. It is **deterministic** — identical inputs and config always produce identical output (except the `generated_at` timestamp).

## Text Engine

Reconlify also compares text files line-by-line or as unordered line sets:

```yaml
type: text
source: expected.log
target: actual.log
mode: unordered_lines
normalize:
  trim_lines: true
  ignore_blank_lines: true
```

```bash
reconlify run text_recon.yaml
```

In `unordered_lines` mode, line order is ignored — Reconlify compares occurrence counts of each distinct line. In `line_by_line` mode (default), lines are compared positionally.

See the `examples/` directory for config samples.

## Engines

### Tabular Engine

- **Key-based reconciliation** — Single or composite keys. Detects missing rows on either side and cell-level value mismatches.
- **Column control** — Include or exclude specific columns from comparison.
- **Numeric tolerance** — Absolute tolerance per column (e.g. `amount: 0.01`).
- **String rules** — Per-column normalization: `trim`, `case_insensitive`, `contains`, `regex_extract`.
- **Source-side normalization** — Virtual columns via ops: `map`, `concat`, `substr`, `add`, `sub`, `mul`, `div`, `coalesce`, `date_format`, `upper`, `lower`, `trim`, `round`.
- **Row filters** — Exclude specific key values or filter rows by column rules.
- **TSV support** — Set `csv.delimiter: "\t"`.

### Text Engine

- **Two comparison modes:** `line_by_line` (positional) and `unordered_lines` (multiset).
- **Normalization:** `trim_lines`, `collapse_whitespace`, `case_insensitive`, `ignore_blank_lines`, `normalize_newlines`.
- **Regex rules:** `drop_lines_regex` to remove lines, `replace_regex` to transform lines before comparison.

## Report Format

Every run produces a JSON report with a consistent structure:

| Section | Description |
|---------|-------------|
| `summary` | Aggregate counts (rows, mismatches, missing). Zero differences = exit code 0 |
| `details` | Metadata: keys used, columns compared, filters applied, per-column mismatch stats |
| `samples` | Concrete examples of differences (tabular: `missing_in_target`, `missing_in_source`, `value_mismatches`, `excluded`; text: flat list or `samples_agg`) |
| `error` | Present only on exit code 2. Machine-readable `code`, human-readable `message`, and `details` |
| `warnings` | Optional list of warning strings (e.g. large line-number arrays in unordered mode) |

## CI Usage

```bash
reconlify run recon.yaml --out report.json
rc=$?

if [ $rc -eq 2 ]; then
  echo "ERROR: config or runtime failure" >&2
  exit 1
elif [ $rc -eq 1 ]; then
  echo "WARN: differences found — see report.json" >&2
fi
```

Exit code **1** means differences were found — your pipeline decides whether that's a warning or a failure. Exit code **2** is always an error.

### GitHub Actions

```yaml
- name: Reconcile data
  run: |
    reconlify run recon.yaml --out report.json
    exit_code=$?
    if [ $exit_code -eq 2 ]; then
      echo "::error::Reconciliation failed with error"
      exit 1
    elif [ $exit_code -eq 1 ]; then
      echo "::warning::Differences found — see report.json"
    fi

- name: Upload report
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: recon-report
    path: report.json
```

## Documentation

- [User Guide](https://github.com/testuteab/reconlify-cli/blob/main/docs/RECONLIFY_CLI_USER_GUIDE_v1.md) — In-depth guide covering both engines and best practices
- [YAML Config Schema](https://github.com/testuteab/reconlify-cli/blob/main/docs/YAML_SCHEMA_v1.md) — Full reference for all configuration options
- [Report Schema](https://github.com/testuteab/reconlify-cli/blob/main/docs/REPORT_SCHEMA_v1.md) — Complete specification of the JSON report format
- [Performance Testing](https://github.com/testuteab/reconlify-cli/blob/main/docs/PERF_TESTING.md) — Benchmark methodology and baseline results

## Reconlify Desktop

Reconlify Desktop is a graphical interface for Reconlify CLI. It allows users to:

- Visually build YAML reconciliation configs
- Run reconciliations without using the terminal
- Inspect reconciliation reports interactively

Reconlify CLI remains the core reconciliation engine.

## Development

```bash
make install       # install dependencies
make test          # unit + integration tests (excludes e2e and perf)
make e2e           # end-to-end CLI tests
make lint          # ruff linter
make format        # auto-fix lint + format
make clean         # remove build artifacts and caches
```

### Performance Testing

```bash
make perf          # generate fixtures + run full benchmark suite
make perf-smoke    # lightweight perf smoke tests
make perf-clean    # remove generated fixtures
```

See [Performance Testing](https://github.com/testuteab/reconlify-cli/blob/main/docs/PERF_TESTING.md) for details and baseline results.

## Changelog

See [CHANGELOG.md](https://github.com/testuteab/reconlify-cli/blob/main/CHANGELOG.md) for release history.

---

Reconlify sits between simple file diff tools and heavy enterprise reconciliation systems, providing a deterministic, developer-friendly workflow for validating structured data locally.

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Testute AB.
