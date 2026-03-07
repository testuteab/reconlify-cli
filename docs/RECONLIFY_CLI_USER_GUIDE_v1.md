# Reconlify User Guide — V1

## 1. Introduction

Reconlify is a local-first, rule-based data reconciliation CLI tool. It
compares two files — structured (CSV/TSV) or unstructured (text) — using a
declarative YAML configuration, and produces a deterministic JSON report.

All processing happens locally. No data is uploaded or transmitted.

Reconlify targets data engineers, QA engineers, ETL developers, migration
consultants, and CI/CD validation pipelines.

### Core Concepts

- **Config-driven.** Every comparison is defined by a YAML file. The same
  config always produces the same result on the same input.
- **Two engines.** The *tabular engine* compares CSV/TSV files row-by-row
  using key-based matching. The *text engine* compares text files line by
  line or as unordered multisets.
- **JSON report.** Every run produces a structured JSON report with summary
  counts, comparison metadata, and sample differences.
- **Exit codes.** `0` = no differences, `1` = differences found, `2` =
  error. These integrate directly into CI/CD pipelines.

---

## 2. Installation

Requires Python 3.11 or later.

```bash
pip install reconlify-cli
```

Or with Poetry (for development):

```bash
poetry install
```

Verify the installation:

```bash
reconlify run --help
```

---

## 3. Configuration Overview

Every Reconlify comparison starts with a YAML config file. The top-level
`type` field selects the engine:

```yaml
type: tabular   # or "text"
source: path/to/source_file
target: path/to/target_file
```

File paths are resolved relative to the working directory where `reconlify`
is invoked.

### Running a comparison

```bash
reconlify run config.yaml
reconlify run config.yaml --out results.json
```

The JSON report is written to `report.json` by default. Use `--out` to
specify a different path.

For the full YAML schema reference, see
[YAML_SCHEMA_v1.md](YAML_SCHEMA_v1.md). For the full report schema, see
[REPORT_SCHEMA_v1.md](REPORT_SCHEMA_v1.md).

---

# Part I — Tabular Engine

The tabular engine compares CSV and TSV files using key-based row matching.
It is powered by DuckDB for vectorized, in-memory SQL execution.

## 4. Key-Based Matching

Every tabular comparison requires one or more key columns. Keys identify
which rows correspond across the source and target files.

```yaml
type: tabular
source: accounts_source.csv
target: accounts_target.csv
keys:
  - account_id
```

Composite keys use multiple columns:

```yaml
keys:
  - account_id
  - region
```

The engine performs a full outer join on the key columns and classifies
each row into one of four categories:

| Category | Meaning |
|----------|---------|
| Missing in target | Key exists in source but not in target |
| Missing in source | Key exists in target but not in source |
| Value mismatch | Key exists on both sides, but one or more compared columns differ |
| Match | Key exists on both sides, all compared columns are equal |

Key matching is NULL-safe: two NULL key values are considered equal (using
SQL `IS NOT DISTINCT FROM` semantics).

**Duplicate keys are an error.** If any key combination appears more than
once on either side (after filtering), the engine returns exit code 2 with
error code `DUPLICATE_KEYS`.

## 5. Column Control

### Compared columns

By default, the engine compares all non-key columns that exist on *both*
sides (the intersection of source and target column names). You can narrow
this set with several options.

**`ignore_columns`** — Remove specific columns from comparison:

```yaml
ignore_columns:
  - updated_at
  - internal_notes
```

**`compare.include_columns`** — Compare *only* the listed columns
(whitelist):

```yaml
compare:
  include_columns:
    - name
    - amount
    - status
```

**`compare.exclude_columns`** — Compare all common columns *except* the
listed ones (blacklist):

```yaml
compare:
  exclude_columns:
    - debug_field
```

These filters are applied in order: first the common-column intersection is
computed, then `include_columns` narrows it, then `exclude_columns` removes
from it, then `ignore_columns` removes from it. In practice, you typically
use only one of these mechanisms.

### Global comparison settings

```yaml
compare:
  trim_whitespace: true          # default: true — TRIM() all values
  case_insensitive: false        # default: false — LOWER() all values
  normalize_nulls: ["", "NULL", "null"]  # default — treat these as NULL
```

`normalize_nulls` converts the listed string values to SQL NULL before
comparison. The defaults treat empty strings and the literal words "NULL"
and "null" as NULL.

## 6. Numeric Tolerance

Tolerance allows numeric columns to differ by a small absolute amount and
still be considered equal.

```yaml
tolerance:
  amount: 0.01       # abs(source - target) <= 0.01 → equal
  balance: 0.001
```

**Semantics:**

1. Both values are cast to DOUBLE.
2. If both casts succeed: `abs(source - target) <= tolerance` → equal.
3. If either cast fails (non-numeric data): falls back to exact string
   comparison (`IS NOT DISTINCT FROM`).

Tolerance values must be >= 0.

**Tip:** Use tolerance for floating-point rounding differences between
systems. A tolerance of `0.01` is appropriate when you expect values to
agree to two decimal places.

## 7. String Rules

String rules apply per-column normalization *in addition to* the global
`compare` settings. They modify how individual columns are compared.

```yaml
string_rules:
  name:
    - trim
    - case_insensitive
  product_code:
    - regex_extract:
        pattern: "^([A-Z]+)-\\d+"
        group: 1
```

### Available rules

| Rule | Effect |
|------|--------|
| `trim` | `TRIM()` the value before comparison |
| `case_insensitive` | `LOWER()` the value before comparison |
| `contains` | Match if either side contains the other (`src LIKE '%' \|\| tgt \|\| '%'` OR vice versa) instead of requiring exact equality |
| `regex_extract` | Apply `regexp_extract(value, pattern, group)` to both sides before comparison |

Rules are cumulative with global settings. If `compare.trim_whitespace` is
already `true` (the default), adding `trim` to a column's string_rules is
redundant but harmless.

**`regex_extract` parameters:**

- `pattern` (required): A regular expression with at least one capture
  group.
- `group` (optional, default 1): Which capture group to extract.

Both sides are extracted with the same pattern before comparison.

## 8. Column Mapping

Compare semantically equivalent datasets even when source and target use
different column names.

When source and target files represent the same data but use different
column headers, use `column_mapping` to declare the correspondence:

```yaml
type: tabular
source: trades_erp.csv
target: trades_ledger.csv

keys:
  - trade_id

column_mapping:
  trade_id: id
  amount: total_amount
  customer_name: client_name
```

Key design principle: the **source-side / logical column name** is the
canonical identifier. All config fields — `keys`, `compare`,
`tolerance`, `string_rules`, `ignore_columns` — use logical names.
`column_mapping` only affects how target columns are resolved.

If a column has no mapping, the target column defaults to the same name.

### Interactions with other features

- **Keys:** `keys: [trade_id]` joins `source.trade_id` against
  `target.id` when `column_mapping.trade_id = id`.
- **Tolerance / string_rules:** `tolerance.amount: 0.05` applies to
  `source.amount` vs `target.total_amount`.
- **Normalization:** a generated source column can map to a target column:

```yaml
normalization:
  full_name:
    - op: concat
      args: [first_name, " ", last_name]

column_mapping:
  full_name: customer_full_name
```

### Validation

- Mapped target columns must exist in the target file.
- No two logical columns may map to the same target column.
- Empty keys or values are rejected.

---

## 9. Source-Side Normalization

Normalization creates virtual columns on the source side by transforming
existing columns. If the target has a column with the same name as a
virtual column, that column is included in the comparison.

This is useful when source and target have different schemas but equivalent
data.

```yaml
normalization:
  full_name:
    - op: concat
      args: [first_name, " ", last_name]
    - op: trim

  amount_usd:
    - op: mul
      args: [amount, exchange_rate]
```

### Pipeline model

Each normalization entry is a linear pipeline of steps. Step 1 receives its
inputs from `args`. Steps 2+ receive the result of the previous step as
their implicit first operand.

```yaml
# Step 1: concat(first_name, " ", last_name) → "Alice Smith"
# Step 2: trim(previous_result)             → "Alice Smith"
full_name:
  - op: concat
    args: [first_name, " ", last_name]
  - op: trim
```

### Supported operations

| Op | Step 1 args | Description |
|----|-------------|-------------|
| `map` | col, val, repl, val, repl, ... | `CASE WHEN col=val THEN repl ...` |
| `concat` | col1, literal, col2, ... | String concatenation |
| `substr` | col, start [, length] | `SUBSTR()` |
| `add` | col1, col2 | Cast to DOUBLE, add |
| `sub` | col1, col2 | Cast to DOUBLE, subtract |
| `mul` | col1, col2 | Cast to DOUBLE, multiply |
| `div` | col1, col2 | Cast to DOUBLE, divide |
| `coalesce` | col1, col2, ... | First non-NULL value |
| `date_format` | col, from_fmt, to_fmt | Parse then reformat date |
| `upper` | col | `UPPER()` |
| `lower` | col | `LOWER()` |
| `trim` | col | `TRIM()` |
| `round` | col [, precision] | `ROUND()` with optional decimal places |

### Argument resolution

Arguments in `args` are resolved as follows:

- If the argument is a string that matches an existing source column name,
  it is treated as a column reference.
- If the argument is a number, it is used as a numeric literal.
- Otherwise, it is used as a string literal.

### Constraints

- Normalization is applied to the source side only.
- Each pipeline must contain at least one step.
- Arguments cannot reference other generated (normalization) columns.
- Pipelines are linear — no branching or nesting.

## 10. Filters

Filters remove rows from the comparison. Excluded rows are tracked in the
report with sample data for audit purposes.

### Exclude keys

Remove specific rows by their exact key values:

```yaml
filters:
  exclude_keys:
    - { id: "999", region: "TEST" }
    - { id: "000", region: "STAGING" }
```

Each entry must contain exactly all key columns. Matching rows are removed
from both source and target before comparison.

### Row filters

Remove rows based on column conditions:

```yaml
filters:
  row_filters:
    apply_to: both         # "both" (default), "source", or "target"
    mode: exclude          # "exclude" (default) or "include"
    rules:
      - column: status
        op: equals
        value: deleted
      - column: amount
        op: not_null
```

**`mode`** controls the semantics:

- `exclude` — Remove rows that match ALL rules (AND logic).
- `include` — Keep only rows that match ALL rules; remove the rest.

**`apply_to`** controls which sides are filtered:

- `both` — Apply to source and target.
- `source` — Apply only to source.
- `target` — Apply only to target.

### Available filter operators

| Operator | Required field | Semantics |
|----------|---------------|-----------|
| `equals` | `value` | Column equals the value |
| `not_equals` | `value` | Column does not equal the value |
| `in` | `values` (list) | Column value is in the list |
| `contains` | `value` | Column contains the substring |
| `regex` | `pattern` | Column matches the regex pattern |
| `is_null` | — | Column is NULL |
| `not_null` | — | Column is not NULL |

Each rule can optionally override the global comparison settings:

```yaml
rules:
  - column: name
    op: equals
    value: "test"
    case_insensitive: true     # override compare.case_insensitive
    trim_whitespace: true      # override compare.trim_whitespace
```

### Filter processing order

1. `exclude_keys` is applied first (removes matching rows).
2. `row_filters` is applied second (on the remaining rows).

Both steps happen before duplicate-key validation and comparison.

## 11. CSV Options

```yaml
csv:
  delimiter: ","       # default: "," — use "\t" for TSV files
  header: true         # default: true
  encoding: utf-8      # default: utf-8 (only UTF-8 is supported)
```

All values are read as strings (`all_varchar = true`). Numeric
comparisons happen via tolerance rules, which cast to DOUBLE internally.

## 12. Output Control

```yaml
output:
  include_row_samples: true      # default: true
  include_column_stats: true     # default: true
```

Setting `include_row_samples: false` produces empty sample lists (useful
for large-scale comparisons where you only need counts).

Setting `include_column_stats: false` produces an empty `column_stats`
object.

## 13. Tabular Report Structure

The tabular engine produces a report with this structure:

```json
{
  "type": "tabular",
  "version": "1.1",
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
  "details": {
    "format": "csv",
    "keys": ["id"],
    "compared_columns": ["name", "amount"],
    "read_rows_source": 1003,
    "read_rows_target": 998,
    "filters_applied": { "..." : "..." },
    "column_stats": {
      "name": { "mismatched_count": 3 },
      "amount": { "mismatched_count": 5 }
    }
  },
  "samples": {
    "missing_in_target": [],
    "missing_in_source": [],
    "value_mismatches": [],
    "excluded": []
  }
}
```

**Key relationships:**

```
read_rows_source - filters_applied.source_excluded_rows == source_rows
```

The `summary` counts reflect the dataset *after* filtering. The `details`
section lets you trace how many rows were read vs. how many participated
in the comparison.

## 14. Performance Notes

The tabular engine uses DuckDB for in-memory SQL execution. This provides:

- Vectorized execution for fast column comparisons.
- Column projection — only columns needed for keys, comparison, filtering,
  and normalization are carried through the pipeline. Extra columns on
  either side are dropped early.
- All data is read as strings (`all_varchar = true`) to avoid type
  inference issues.

For large files (1M+ rows), the engine is designed to complete within
roughly 60 seconds on a modern laptop.

---

# Part II — Text Engine

The text engine compares two text files line by line. It supports two modes
and extensive normalization options.

## 15. Comparison Modes

### line_by_line (default)

Compares lines positionally: line 1 vs line 1, line 2 vs line 2, etc.

If one file is longer, the extra lines are reported as differences with the
shorter side showing an empty string.

```yaml
type: text
source: expected.txt
target: actual.txt
mode: line_by_line
```

### unordered_lines

Ignores line order entirely. Compares the *multiset* of lines — how many
times each distinct line appears on each side.

```yaml
type: text
source: expected.log
target: actual.log
mode: unordered_lines
```

A line appearing 3 times in source and 1 time in target contributes 2 to
`different_lines`.

## 16. Text Normalization

Normalization is applied to each line through a 7-step pipeline in this
exact order:

1. **normalize_newlines** (default: `true`) — Normalize CRLF to LF.
2. **trim_lines** — Strip leading and trailing whitespace from each line.
3. **collapse_whitespace** — Replace consecutive whitespace with a single
   space.
4. **case_insensitive** — Convert to lowercase.
5. **replace_regex** — Apply regex substitution rules (sequentially).
6. **ignore_blank_lines** — Drop lines that are empty (`""`) at this point.
7. **drop_lines_regex** — Drop lines matching any pattern.

```yaml
normalize:
  normalize_newlines: true       # default: true
  trim_lines: false              # default: false
  collapse_whitespace: false     # default: false
  case_insensitive: false        # default: false
  ignore_blank_lines: false      # default: false
```

The pipeline order matters. For example, `trim_lines` and
`collapse_whitespace` are applied *before* `ignore_blank_lines`, so a line
containing only whitespace will be trimmed to `""` and then dropped if
`ignore_blank_lines` is enabled.

## 17. Regex Rules

### replace_regex

Substitute matching substrings before comparison. Rules are applied
sequentially in the order listed.

```yaml
replace_regex:
  - pattern: "\\d{4}-\\d{2}-\\d{2}"
    replace: "DATE"
  - pattern: "request_id=[a-f0-9-]+"
    replace: "request_id=<ID>"
```

This is useful for normalizing timestamps, UUIDs, or other variable content
that should not cause false positives.

### drop_lines_regex

Remove entire lines that match any of the listed patterns:

```yaml
drop_lines_regex:
  - "^#"              # comment lines
  - "^\\s*$"          # blank lines (before other normalization)
```

A line is dropped if it matches *any* pattern in the list.

## 18. Text Report Structure

### line_by_line report

```json
{
  "type": "text",
  "version": "1.1",
  "summary": {
    "total_lines_source": 150,
    "total_lines_target": 148,
    "different_lines": 3,
    "comparison_time_seconds": 0.001
  },
  "details": {
    "mode": "line_by_line",
    "read_lines_source": 155,
    "read_lines_target": 150,
    "ignored_blank_lines_source": 5,
    "ignored_blank_lines_target": 2,
    "rules_applied": {
      "drop_lines_count": 0,
      "replace_rules_count": 42
    }
  },
  "samples": [
    {
      "line_number_source": 10,
      "line_number_target": 10,
      "source": "processed source line",
      "target": "processed target line"
    }
  ]
}
```

- `total_lines_*` — Line count *after* processing (normalization, dropping,
  blank-line filtering). These are the lines that actually participate in
  the comparison.
- `read_lines_*` — Raw line count *before* any processing.
- `ignored_blank_lines_*` — Lines removed by `ignore_blank_lines`.
- `drop_lines_count` — Lines removed by `drop_lines_regex` (combined for
  both files).
- `replace_rules_count` — Total regex substitutions applied (combined for
  both files).
- `line_number_source` / `line_number_target` — Original 1-based line
  numbers in the raw file. `null` when one side is exhausted.

### unordered_lines report

In unordered mode, `samples` is always an empty list `[]`. Aggregated
mismatch data is in `samples_agg`:

```json
{
  "summary": { "..." : "..." },
  "details": {
    "mode": "unordered_lines",
    "unordered_stats": {
      "source_only_lines": 5,
      "target_only_lines": 3,
      "distinct_mismatched_lines": 4
    },
    "..." : "..."
  },
  "samples": [],
  "samples_agg": [
    {
      "line": "some line content",
      "source_count": 3,
      "target_count": 1,
      "source_line_numbers": [5, 12, 30],
      "target_line_numbers": [8],
      "source_line_numbers_truncated": false,
      "target_line_numbers_truncated": false
    }
  ]
}
```

**`unordered_stats`:**

- `source_only_lines` — Excess lines in source
  (`sum of max(src_count - tgt_count, 0)` per distinct line).
- `target_only_lines` — Excess lines in target.
- `distinct_mismatched_lines` — Number of unique line contents with
  differing counts.

Invariant: `source_only_lines + target_only_lines == different_lines`.

**`samples_agg`:**

- Sorted by largest count difference first, then by line content
  lexicographically.
- By default, all line numbers are stored (unlimited). Use
  `--max-line-numbers N` to cap to N entries per side when report size is a
  concern. The `*_truncated` flag indicates when line numbers were omitted.
- When `--no-include-line-numbers` is used, all four line-number fields are
  omitted.

`samples_agg` is present only in `unordered_lines` mode when differences
exist. It is omitted entirely when there are zero differences or when
an error occurs.

---

# Part III — Report Schema

This section covers the report structure shared by both engines. For the
full specification with audit notes and JSON examples, see
[REPORT_SCHEMA_v1.md](REPORT_SCHEMA_v1.md).

## 19. Root Object

```json
{
  "type": "tabular | text",
  "version": "1.1",
  "generated_at": "ISO-8601 UTC timestamp",
  "config_hash": "sha256 hex string",
  "summary": {},
  "details": {},
  "samples": [],
  "samples_agg": [],
  "error": {}
}
```

| Field | Presence | Description |
|-------|----------|-------------|
| `type` | Always | `"tabular"` or `"text"` — matches the config. |
| `version` | Always | Report schema version. Currently `"1.1"`. |
| `generated_at` | Always | ISO-8601 UTC timestamp. Not deterministic. |
| `config_hash` | Always | SHA-256 of the canonical config JSON. Identical configs produce the same hash. On error, falls back to hash of raw YAML or `""`. |
| `summary` | Always | Aggregate counts. Zeroed when `error` is present. |
| `details` | Always | Comparison metadata. Defaults when `error` is present. |
| `samples` | Always | Sample diff entries. Dict (tabular) or list (text). Empty on error. |
| `samples_agg` | Optional | Text `unordered_lines` mode only. Omitted when absent. |
| `error` | Optional | Present only on exit code 2. |

## 20. Summary Fields

### Tabular

| Field | Description |
|-------|-------------|
| `source_rows` | Rows in source *after* filtering |
| `target_rows` | Rows in target *after* filtering |
| `missing_in_target` | Keys in source not found in target |
| `missing_in_source` | Keys in target not found in source |
| `rows_with_mismatches` | Matched rows with at least one cell difference |
| `mismatched_cells` | Total cell-level mismatches |
| `comparison_time_seconds` | Wall-clock seconds |

### Text

| Field | Description |
|-------|-------------|
| `total_lines_source` | Lines in source *after* processing |
| `total_lines_target` | Lines in target *after* processing |
| `different_lines` | Number of line-level differences |
| `comparison_time_seconds` | Wall-clock seconds |

## 21. Details Fields

### Tabular

| Field | Description |
|-------|-------------|
| `format` | Always `"csv"` in V1 |
| `keys` | Key column names used for matching |
| `compared_columns` | Sorted list of columns compared (logical names) |
| `column_mapping` | Effective column mappings `{ logical: target }`. Omitted when empty. |
| `read_rows_source` | Raw rows read before filtering |
| `read_rows_target` | Raw rows read before filtering |
| `filters_applied` | Filter breakdown (exclude_keys + row_filters) |
| `column_stats` | Per-column `{ "mismatched_count": N }`. Empty `{}` when disabled. |

### Text

| Field | Description |
|-------|-------------|
| `mode` | `"line_by_line"` or `"unordered_lines"` |
| `read_lines_source` | Raw lines read before processing |
| `read_lines_target` | Raw lines read before processing |
| `ignored_blank_lines_source` | Lines removed by `ignore_blank_lines` |
| `ignored_blank_lines_target` | Lines removed by `ignore_blank_lines` |
| `rules_applied` | `drop_lines_count` and `replace_rules_count` (combined totals) |
| `unordered_stats` | Present only in `unordered_lines` mode |

## 22. Samples

### Tabular samples (dict with four categories)

**`missing_in_target`** / **`missing_in_source`** — Rows present on one
side only. Each entry has a key, the row data, and a line number from the
side where the row exists.

**`value_mismatches`** — Matched rows with differing values. Each entry
shows the key, line numbers on both sides, and only the columns that
differ (with source and target values).

**`excluded`** — Rows removed by filters. Each entry shows the side, key,
row data, line number, and the `reason` (`"exclude_keys"` or
`"row_filters"`).

### Text samples

**line_by_line:** A flat list of diff entries with source/target line
content and original line numbers.

**unordered_lines:** `samples` is always `[]`. Aggregated entries are in
`samples_agg` with occurrence counts and line number lists.

## 23. Error Object

Present only on exit code 2. When `error` is present, all `summary` fields
are zero and `samples` is empty.

```json
{
  "code": "CONFIG_VALIDATION_ERROR",
  "message": "Human-readable summary",
  "details": "Extended information or traceback"
}
```

**Error codes:**

| Code | Cause |
|------|-------|
| `CONFIG_VALIDATION_ERROR` | Invalid YAML or schema violation |
| `RUNTIME_ERROR` | File not found, I/O failure, unexpected exception |
| `DUPLICATE_KEYS` | Non-unique keys after filtering (tabular only) |
| `INVALID_ROW_FILTERS` | Row filter references a missing column (tabular only) |
| `INVALID_COLUMN_MAPPING` | Mapped target column missing or creates collision (tabular only) |

## 24. Exit Codes

| Code | Meaning | Report state |
|------|---------|--------------|
| 0 | No differences | `summary` has all-zero diff counts |
| 1 | Differences found | `summary` has non-zero diff counts |
| 2 | Error | `error` object present, `summary` zeroed |

---

# Part IV — Best Practices

## 25. Designing Configs

**Start minimal.** Begin with just `type`, `source`, `target`, and `keys`.
Run the comparison and review the report. Add tolerance, string rules, and
filters only as needed.

```yaml
# Start here
type: tabular
source: source.csv
target: target.csv
keys: [id]
```

**Use `ignore_columns` for volatile fields.** Columns like `updated_at`,
`created_by`, or `internal_id` often differ between systems but are not
meaningful differences. Exclude them early.

**Use `compare.include_columns` for focused comparisons.** When you only
care about a few columns, a whitelist is clearer than a blacklist.

**Store configs in version control.** Since configs are deterministic,
storing them alongside your data pipeline code ensures reproducibility.

## 26. Using Tolerance Safely

**Understand the semantics.** Tolerance uses *absolute* comparison:
`abs(source - target) <= tolerance`. There is no relative/percentage mode.

**Use tight tolerances.** A tolerance of `0.01` means values can differ by
up to one cent. Use the smallest tolerance that accounts for your expected
rounding differences.

**Non-numeric data falls back to exact comparison.** If a value cannot be
cast to DOUBLE on either side, the tolerance rule is ignored and the values
are compared as strings. This prevents false positives on non-numeric data
but means you should not rely on tolerance for mixed-type columns.

**Tolerance and string rules are independent.** Tolerance applies to the
numeric cast of the raw value. String rules (trim, case_insensitive) apply
to the string representation. They do not interact.

## 27. Debugging Mismatches

**Check `compared_columns` first.** The report's `details.compared_columns`
tells you exactly which columns were compared. If a column is missing,
check whether it exists on both sides or is being excluded by
`ignore_columns` / `compare.exclude_columns`.

**Review `column_stats`.** The `details.column_stats` object shows
per-column mismatch counts. This quickly identifies which columns are
causing the most differences.

**Inspect `value_mismatches` samples.** Each sample shows the exact source
and target values for the differing columns, making it easy to spot
patterns (e.g., trailing whitespace, case differences, rounding).

**Check filter effects.** Compare `read_rows_source` to `source_rows`. If
they differ, rows were filtered. Review `filters_applied` for the
breakdown, and check `excluded` samples to verify filters are behaving as
intended.

**For text comparisons**, use `--debug-report` to include processed line
numbers in samples. This shows the position in the normalized stream,
which helps when `drop_lines_regex` or `ignore_blank_lines` shift line
positions.

## 28. Integrating into CI

### Basic pattern

```bash
reconlify run recon.yaml --out report.json
exit_code=$?

if [ $exit_code -eq 2 ]; then
  echo "ERROR: Reconciliation failed"
  exit 1
elif [ $exit_code -eq 1 ]; then
  echo "WARNING: Differences found"
  # Optionally: fail the build, or just warn
fi
```

### Using config_hash for caching

The `config_hash` in the report is a SHA-256 of the canonical config. Use
it to detect whether a reconciliation config has changed between runs.

### Archiving reports

Always save the JSON report as a build artifact. Even when the comparison
passes (exit 0), the report provides audit evidence of what was compared,
how many rows were read, and what filters were applied.

### Multiple comparisons

Run multiple configs in sequence and aggregate exit codes:

```bash
max_exit=0
for config in configs/*.yaml; do
  reconlify run "$config" --out "reports/$(basename "$config" .yaml).json"
  code=$?
  if [ $code -gt $max_exit ]; then max_exit=$code; fi
done
exit $max_exit
```

---

## Appendix: Full Example Configs

### Tabular — comprehensive

```yaml
type: tabular
source: examples/data/source.csv
target: examples/data/target.csv
keys:
  - id
  - region

compare:
  exclude_columns:
    - internal_notes
  trim_whitespace: true
  case_insensitive: false
  normalize_nulls: ["", "NULL", "null"]

filters:
  exclude_keys:
    - { id: "999", region: "TEST" }
  row_filters:
    apply_to: both
    mode: exclude
    rules:
      - column: status
        op: equals
        value: deleted

ignore_columns:
  - updated_at

tolerance:
  amount: 0.01
  balance: 0.001

string_rules:
  name:
    - trim
    - case_insensitive

normalization:
  full_name:
    - op: concat
      args: ["first_name", " ", "last_name"]
    - op: trim
  amount_rounded:
    - op: round
      args: [amount, 2]

csv:
  delimiter: ","
  header: true
  encoding: utf-8

output:
  include_row_samples: true
  include_column_stats: true
```

### Text — comprehensive

```yaml
type: text
source: data/source.txt
target: data/target.txt
mode: unordered_lines
normalize:
  ignore_blank_lines: true
  trim_lines: true
  collapse_whitespace: true
  case_insensitive: false
  normalize_newlines: true
drop_lines_regex:
  - "^#"
  - "^\\s*$"
replace_regex:
  - pattern: "\\d{4}-\\d{2}-\\d{2}"
    replace: "DATE"
```
