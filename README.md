# Reconify

Local-first, rule-based data reconciliation CLI.

Reconify compares structured (CSV/TSV) and unstructured (text) files using
declarative YAML configs, producing deterministic JSON reports with
machine-readable exit codes. All processing happens locally — no data leaves
your machine.

## Why Reconify?

| Need | Alternative | Reconify |
|------|------------|----------|
| Compare two CSV exports | Manual Excel diff | Key-based matching with tolerance, normalization, and per-column rules |
| Diff log files | `diff` / `comm` | Unordered-line multiset comparison with regex normalization |
| Validate ETL output in CI | Custom scripts | YAML config + JSON report + exit codes, no code to maintain |
| Audit filtered datasets | Enterprise recon tools | Local-only, open, deterministic, reproducible |

## Installation

Requires Python 3.11+.

```bash
pip install reconify-cli
# or
poetry install
```

## CLI Usage

```bash
reconify run config.yaml
reconify run config.yaml --out results.json
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No differences found |
| 1 | Differences found |
| 2 | Error (config validation, runtime failure) |

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--out PATH` | `report.json` | Output path for the JSON report |
| `--include-line-numbers` | on | Include original line numbers in text samples |
| `--max-line-numbers N` | 10 | Max line numbers per distinct line (unordered mode) |
| `--debug-report` | off | Include processed line numbers in text samples |

## Quick Start — Tabular

Create `recon.yaml`:

```yaml
type: tabular
source: source.csv
target: target.csv
keys:
  - id
```

Run:

```bash
reconify run recon.yaml
# Exit code 0 = match, 1 = differences, 2 = error
echo $?
```

The report is written to `report.json` with summary counts, detailed
metadata, and sample rows for every category of difference.

## Quick Start — Text

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
reconify run text_recon.yaml
```

In `unordered_lines` mode, line order is ignored — Reconify compares
occurrence counts of each distinct line across both files.

## Tabular Engine Features

**Key-based reconciliation** — Single or composite keys. Detects missing
rows on either side and cell-level value mismatches.

```yaml
keys:
  - account_id
  - region
```

**Column control** — Exclude columns from comparison, or restrict to a
specific set:

```yaml
ignore_columns: [updated_at, internal_id]
compare:
  include_columns: [name, amount, status]   # compare only these
  # or: exclude_columns: [notes]            # compare all except these
```

**Numeric tolerance** — Absolute tolerance per column. Values within range
are considered equal:

```yaml
tolerance:
  amount: 0.01
  balance: 0.001
```

**String rules** — Per-column normalization before comparison:

```yaml
string_rules:
  name:
    - trim
    - case_insensitive
  code:
    - regex_extract:
        pattern: "^([A-Z]+)-"
        group: 1
```

Supported rules: `trim`, `case_insensitive`, `contains`, `regex_extract`.

**Source-side normalization** — Create virtual columns on the source side
from existing columns, then compare against the target:

```yaml
normalization:
  full_name:
    - op: concat
      args: [first_name, " ", last_name]
    - op: trim
```

Supported ops: `map`, `concat`, `substr`, `add`, `sub`, `mul`, `div`,
`coalesce`, `date_format`, `upper`, `lower`, `trim`, `round`.

**Row filters** — Exclude specific key values or filter rows by rules:

```yaml
filters:
  exclude_keys:
    - { id: "999", region: "TEST" }
  row_filters:
    mode: exclude
    apply_to: both
    rules:
      - column: status
        op: equals
        value: deleted
```

**TSV support** — Set the delimiter to tab:

```yaml
csv:
  delimiter: "\t"
```

## Text Engine Features

**Two comparison modes:**

- `line_by_line` (default) — Positional comparison, line N vs line N.
- `unordered_lines` — Multiset comparison of line occurrence counts.

**Normalization options:**

```yaml
normalize:
  trim_lines: true
  collapse_whitespace: true
  case_insensitive: true
  ignore_blank_lines: true
  normalize_newlines: true    # default
```

**Regex rules** — Drop or transform lines before comparison:

```yaml
drop_lines_regex:
  - "^#"              # remove comments
  - "^\\s*$"          # remove blank lines
replace_regex:
  - pattern: "\\d{4}-\\d{2}-\\d{2}"
    replace: "DATE"
```

## Report Overview

Every run produces a JSON report (`report.json` by default) with a
consistent structure:

```json
{
  "type": "tabular",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "sha256...",
  "summary": {
    "source_rows": 1000,
    "target_rows": 998,
    "missing_in_target": 2,
    "missing_in_source": 0,
    "rows_with_mismatches": 5,
    "mismatched_cells": 8,
    "comparison_time_seconds": 0.42
  },
  "details": { "..." : "..." },
  "samples": { "..." : "..." }
}
```

**`summary`** — Aggregate counts. Zero differences = exit code 0.

**`details`** — Metadata: keys used, columns compared, filter breakdown,
per-column mismatch stats (`column_stats`).

**`samples`** — Concrete examples of differences. Tabular reports provide
four categories: `missing_in_target`, `missing_in_source`,
`value_mismatches`, `excluded`. Text reports provide a flat list
(`line_by_line`) or aggregated entries (`samples_agg` in `unordered_lines`
mode).

**`error`** — Present only on exit code 2. Includes machine-readable
`code`, human-readable `message`, and `details`.

## CI/CD Integration

```yaml
# GitHub Actions example
- name: Reconcile data
  run: |
    reconify run recon.yaml --out report.json
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

- [YAML Config Schema](docs/YAML_SCHEMA_v1.md) — Full reference for all
  configuration options.
- [Report Schema](docs/REPORT_SCHEMA_v1.md) — Complete specification of the
  JSON report format, field semantics, and audit notes.
- [User Guide](docs/USER_GUIDE_v1.md) — In-depth guide covering both
  engines, configuration patterns, and best practices.
- [Product Requirements](docs/PRD_v1.md) — V1 scope and design rationale.

## Development

```bash
poetry install
poetry run pytest              # unit + integration tests
poetry run pytest -m e2e       # end-to-end CLI tests
poetry run ruff check src tests
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release history.

## License

Released under the [MIT License](LICENSE). Copyright (c) 2026 Testute AB.
