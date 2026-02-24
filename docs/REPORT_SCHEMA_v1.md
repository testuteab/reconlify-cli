# Reconify Report Schema – V1

Output: report.json

Root:

{
  "type": "tabular" | "text",
  "version": "1.1",
  "generated_at": "ISO-8601 timestamp",
  "config_hash": "sha256 of canonical config JSON",
  "summary": {...},
  "details": {...},
  "samples": [...] | {...},
  "samples_agg": [...] | absent,
  "error": {...} | absent
}

---

# ERROR (optional)

Present only when exit code is 2 (config or runtime error).

error:

{
  "code": "CONFIG_VALIDATION_ERROR" | "RUNTIME_ERROR" | "DUPLICATE_KEYS" | "INVALID_ROW_FILTERS",
  "message": string,
  "details": string
}

When error is present:
- summary fields are zeroed out
- samples is empty
- details contain default values

---

# TABULAR REPORT

## summary

{
  "source_rows": int,
  "target_rows": int,
  "missing_in_target": int,
  "missing_in_source": int,
  "rows_with_mismatches": int,
  "mismatched_cells": int,
  "comparison_time_seconds": float
}

- `source_rows` / `target_rows`: rows **after** filtering (exclude_keys + row_filters).

## details

{
  "format": "csv",
  "keys": [string, ...],
  "compared_columns": [string, ...],
  "read_rows_source": int,
  "read_rows_target": int,
  "filters_applied": { ... },
  "column_stats": { ... }
}

- `read_rows_source` / `read_rows_target`: total rows read from the raw CSV
  files **before** any filtering.

Invariant:

  read_rows_source - filters_applied.source_excluded_rows == summary.source_rows
  read_rows_target - filters_applied.target_excluded_rows == summary.target_rows

### details.filters_applied

Totals (always present):

- `source_excluded_rows`: int — total excluded rows from ALL exclusion mechanisms
- `target_excluded_rows`: int — same for target

Breakdown (always present, default 0):

- `source_excluded_rows_exclude_keys`: int
- `target_excluded_rows_exclude_keys`: int
- `source_excluded_rows_row_filters`: int
- `target_excluded_rows_row_filters`: int

Invariant:
  source_excluded_rows = source_excluded_rows_exclude_keys + source_excluded_rows_row_filters
  target_excluded_rows = target_excluded_rows_exclude_keys + target_excluded_rows_row_filters

Exclude-keys info (always present):

- `exclude_keys_count`: int — number of exclude_keys entries in config

Row-filters info (ONLY present when enabled):

- `row_filters`: object | absent

Present only when config.filters.row_filters exists AND contains at least 1 rule.
When not enabled, the `row_filters` key is omitted entirely.

{
  "row_filters": {
    "count": int,
    "apply_to": "both" | "source" | "target",
    "mode": "exclude" | "include"
  }
}

Example (with row_filters enabled):

{
  "exclude_keys_count": 1,
  "source_excluded_rows": 3,
  "target_excluded_rows": 2,
  "source_excluded_rows_exclude_keys": 1,
  "target_excluded_rows_exclude_keys": 1,
  "source_excluded_rows_row_filters": 2,
  "target_excluded_rows_row_filters": 1,
  "row_filters": {
    "count": 2,
    "apply_to": "both",
    "mode": "exclude"
  }
}

Example (without row_filters):

{
  "exclude_keys_count": 0,
  "source_excluded_rows": 0,
  "target_excluded_rows": 0,
  "source_excluded_rows_exclude_keys": 0,
  "target_excluded_rows_exclude_keys": 0,
  "source_excluded_rows_row_filters": 0,
  "target_excluded_rows_row_filters": 0
}

### details.column_stats

Per-column mismatch counts for matched rows.
Present when output.include_column_stats is true (default) and there are compared columns.

{
  "column_name": {
    "mismatched_count": int
  }
}

## samples

Tabular samples is a dict with four categories:

{
  "missing_in_target": [ ... ],
  "missing_in_source": [ ... ],
  "value_mismatches": [ ... ],
  "excluded": [ ... ]
}

Each category is limited by sampling.sample_limit_per_type (or sampling.sample_limit).
When output.include_row_samples is false, all lists are empty.
Entries within each list are sorted by key values ascending for determinism.

### missing_in_target / missing_in_source

{
  "line_number_source": int,        (or line_number_target for missing_in_source)
  "key": {"column": value, ...},
  "row": {"column": value, ...}
}

### value_mismatches

{
  "line_number_source": int,
  "line_number_target": int,
  "key": {"column": value, ...},
  "columns": {
    "column_name": {
      "source": value,
      "target": value
    }
  }
}

### excluded

{
  "side": "source" | "target",
  "key": {"column": value, ...},
  "line_number_source": int,        (or line_number_target for target side)
  "row": {"column": value, ...},
  "reason": "exclude_keys" | "row_filters"
}

---

# TEXT REPORT

## summary

{
  "total_lines_source": int,
  "total_lines_target": int,
  "different_lines": int,
  "comparison_time_seconds": float
}

## details

{
  "mode": "line_by_line" | "unordered_lines",
  "rules_applied": {
    "drop_lines_count": int,
    "replace_rules_count": int
  },
  "unordered_stats": {...} | absent
}

## samples (line_by_line mode)

[
  {
    "line_number_source": int | null,
    "line_number_target": int | null,
    "source": string,
    "target": string,
    "processed_line_number_source": int | null | absent,
    "processed_line_number_target": int | null | absent
  }
]

### Line number semantics (v1.1)

- line_number_source / line_number_target: **original raw file line numbers**
  (1-based). These refer to the position in the raw file before any
  normalization, filtering, or dropping.
- When one side is shorter (missing at this processed index), the
  corresponding line_number is null.
- processed_line_number_source / processed_line_number_target: **debug-only
  fields** (1-based index in the processed stream after filtering). Present
  only when --debug-report is enabled. Null when the side is missing.

## samples (unordered_lines mode)

samples is set to [] for backward compatibility.

Aggregated mismatches are reported in root.samples_agg instead.

## samples_agg (optional, unordered_lines mode only)

Present only when mode is unordered_lines and there are mismatches.

[
  {
    "line": string,
    "source_count": int,
    "target_count": int,
    "source_line_numbers": [int, ...] | absent,
    "target_line_numbers": [int, ...] | absent,
    "source_line_numbers_truncated": bool | absent,
    "target_line_numbers_truncated": bool | absent
  }
]

### Line number arrays

Present when --include-line-numbers is enabled (default: true).

- source_line_numbers / target_line_numbers: lists of **original raw file
  line numbers** (1-based) where this processed line content occurred.
- Capped to --max-line-numbers entries per side (default: 10).
- source_line_numbers_truncated / target_line_numbers_truncated: true when
  the total occurrences exceed the stored line numbers (i.e. some line
  numbers were omitted due to the cap).
- When --no-include-line-numbers is passed, all four fields are absent.

Ordering: sorted by abs(source_count - target_count) descending,
then by line content lexicographically for determinism.

Limited to --sample-limit items (default 2000).

## unordered_stats (optional, unordered_lines mode only)

Present in details when mode is unordered_lines.

{
  "source_only_lines": int,
  "target_only_lines": int,
  "distinct_mismatched_lines": int
}

- source_only_lines: sum of max(source_count - target_count, 0) across all distinct lines
- target_only_lines: sum of max(target_count - source_count, 0) across all distinct lines
- distinct_mismatched_lines: count of distinct line strings where counts differ
