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
  "samples": [...],
  "samples_agg": [...] | absent,
  "error": {...} | absent
}

---

# ERROR (optional)

Present only when exit code is 2 (config or runtime error).

error:

{
  "code": "CONFIG_VALIDATION_ERROR" | "RUNTIME_ERROR",
  "message": string,
  "details": string
}

When error is present:
- summary fields are zeroed out
- samples is empty
- details contain default values

---

# TABULAR REPORT

summary:

{
  "total_rows_source": int,
  "total_rows_target": int,
  "matched_rows": int,
  "missing_in_source": int,
  "missing_in_target": int,
  "different_rows": int,
  "comparison_time_seconds": float
}

details:

{
  "column_stats": {
    "column_name": {
      "differences": int,
      "tolerance_applied": float | null
    }
  }
}

samples:

[
  {
    "key": {"column": value},
    "differences": {
      "column_name": {
        "source": value,
        "target": value
      }
    }
  }
]

---

# TEXT REPORT

summary:

{
  "total_lines_source": int,
  "total_lines_target": int,
  "different_lines": int,
  "comparison_time_seconds": float
}

details:

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
