# Reconlify

Reconlify is a local-first CLI for semantic data reconciliation.

It compares datasets using declarative YAML rules and produces deterministic
JSON reports with machine-readable exit codes. All processing happens
locally — no data leaves your machine.

Reconlify is designed for:

- ETL pipeline validation
- Data migration verification
- CI/CD dataset checks
- Reconciliation audits

## Why Reconlify?

Traditional diff tools show textual differences.

Reconlify performs **semantic reconciliation**:

- Key-based row comparison for tabular data
- Column-level mismatch detection
- Normalization and transformation rules
- Deterministic machine-readable JSON reports

## Architecture

Reconlify consists of two projects:

- **reconlify-cli** — The deterministic reconciliation engine. It can be used
  standalone in scripts, CI/CD pipelines, and automation.
- **reconlify-ui** — A desktop workbench that calls the CLI and visualizes
  reports in a graphical interface.

This repository contains `reconlify-cli`.

## Installation

Requires **Python 3.11+**.

```bash
pip install reconlify-cli
```

For development, use [Poetry](https://python-poetry.org/):

```bash
git clone <repo-url> && cd reconlify-cli
make install          # runs: poetry install
reconlify --help
```

## Quick Start

### Tabular (CSV/TSV)

Create a config file `recon.yaml`:

```yaml
type: tabular
source: source.csv
target: target.csv
keys:
  - id
compare:
  include_columns:
    - amount
    - currency
```

Run it:

```bash
reconlify run recon.yaml
# Output report written to: report.json

reconlify run recon.yaml --out report.json
echo $?   # 0 = match, 1 = differences, 2 = error
```

### Text (line_by_line / unordered_lines)

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
# Output report written to: report.json
```

In `unordered_lines` mode, line order is ignored — Reconlify compares
occurrence counts of each distinct line across both files.
In `line_by_line` mode (default), lines are compared positionally.

See the `examples/` directory for minimal and full-featured config samples.

## Example Output

Running:

```bash
reconlify run recon.yaml --out report.json
```

produces a structured JSON report:

```json
{
  "type": "tabular",
  "summary": {
    "missing_in_target": 3,
    "missing_in_source": 1,
    "rows_with_mismatches": 5
  }
}
```

The full report schema is documented in [REPORT_SCHEMA_v1.md](docs/REPORT_SCHEMA_v1.md).

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
| `--max-line-numbers N` | `0` (unlimited) | Max line numbers per distinct line in unordered mode. 0 = unlimited |
| `--debug-report` | off | Include processed line numbers in text report samples |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No differences found |
| 1 | Differences found |
| 2 | Error (config validation, file not found, runtime failure) |

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

## CI/CD Integration

```yaml
# GitHub Actions example
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

Reconlify intentionally returns exit code **1** when differences are found.
This allows CI pipelines to decide whether differences should fail the build
or simply produce a warning.

Exit code **2** indicates a configuration or runtime error and should
normally fail the pipeline.

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

See [docs/PERF_TESTING.md](docs/PERF_TESTING.md) for details and baseline results.

## Documentation

- [YAML Config Schema](docs/YAML_SCHEMA_v1.md) — Full reference for all configuration options
- [Report Schema](docs/REPORT_SCHEMA_v1.md) — Complete specification of the JSON report format
- [User Guide](docs/RECONLIFY_CLI_USER_GUIDE_v1.md) — In-depth guide covering both engines and best practices
- [Product Requirements](docs/RECONLIFY_CLI_PRD_v1.md) — V1 scope and design rationale
- [Performance Testing](docs/PERF_TESTING.md) — Benchmark methodology and baseline results

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history.

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Testute AB.
