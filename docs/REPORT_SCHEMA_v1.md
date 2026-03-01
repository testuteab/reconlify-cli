# Reconify Report Schema – V1

Output file: `report.json`

Exit codes:

| Code | Meaning                                    |
|------|--------------------------------------------|
| 0    | No differences found                       |
| 1    | Differences found                          |
| 2    | Error (config validation, runtime, etc.)   |

---

## Root object

```json
{
  "type": "tabular" | "text",
  "version": "1.1",
  "generated_at": "ISO-8601 timestamp",
  "config_hash": "sha256 hex string",
  "summary": { ... },
  "details": { ... },
  "samples": [ ... ] | { ... },
  "samples_agg": [ ... ],
  "error": { ... }
}
```

| Field          | Type              | Presence  | Description |
|----------------|-------------------|-----------|-------------|
| `type`         | string            | always    | `"tabular"` or `"text"` — matches the config `type`. |
| `version`      | string            | always    | Schema version. Currently `"1.1"`. |
| `generated_at` | string            | always    | ISO-8601 UTC timestamp of when the report was created. Not deterministic; do not use for equality checks. |
| `config_hash`  | string            | always    | **Best-effort** SHA-256 hex digest. When the config is successfully parsed, this is the hash of the canonical JSON-serialized Pydantic model — two runs with identical configs produce the same hash. On error reports where config was not fully parsed, this falls back to the hash of the raw YAML string, or `""` if the file could not be read at all. Do not rely on hash stability across Reconify versions. |
| `summary`      | object            | always    | Aggregate counts. Structure differs by `type`. Zeroed out when `error` is present. |
| `details`      | object            | always    | Metadata about what was compared and how. Structure differs by `type`. Contains defaults when `error` is present. |
| `samples`      | list or dict      | always    | Sample diff entries. Type-dependent: list for text, dict for tabular. Empty when `error` is present. |
| `samples_agg`  | list              | optional  | **Text only, unordered_lines mode only.** Aggregated mismatch samples. Omitted when absent (line_by_line mode, error reports, or no mismatches in unordered mode). |
| `error`        | object            | optional  | Present only on exit code 2. Omitted entirely when there is no error. |

---

## error (optional)

Present only when exit code is 2. When `error` is present, `summary` fields
are zeroed out, `samples` is empty, and `details` contains defaults.

```json
{
  "code": string,
  "message": string,
  "details": string
}
```

| Field     | Description |
|-----------|-------------|
| `code`    | Machine-readable error category. One of: `"CONFIG_VALIDATION_ERROR"` (invalid YAML or schema violation), `"RUNTIME_ERROR"` (file not found, I/O failure, unexpected exception), `"DUPLICATE_KEYS"` (tabular only — non-unique keys after filtering), `"INVALID_ROW_FILTERS"` (tabular only — row_filter references a column that does not exist in the CSV). |
| `message` | Human-readable summary of the error. |
| `details` | Extended information (e.g. full traceback or validation output). |

**Audit note:** When `error` is present, comparison did not complete. All
summary counts are zero and should not be interpreted as "no differences."
Check `error.code` to determine if the config needs fixing or if it is an
infrastructure problem.

---

# TABULAR REPORT

Produced when `type` is `"tabular"`.

## summary

```json
{
  "source_rows": int,
  "target_rows": int,
  "missing_in_target": int,
  "missing_in_source": int,
  "rows_with_mismatches": int,
  "mismatched_cells": int,
  "comparison_time_seconds": float
}
```

| Field                      | Description |
|----------------------------|-------------|
| `source_rows`              | Number of rows in the source file **after** all filtering (exclude_keys + row_filters). This is the row count that participates in the key-based diff. |
| `target_rows`              | Same as above, for the target file. |
| `missing_in_target`        | Rows whose key exists in (filtered) source but not in (filtered) target. **Audit:** these are records present in source that the target does not account for. |
| `missing_in_source`        | Rows whose key exists in (filtered) target but not in (filtered) source. **Audit:** these are records the target has that source does not. |
| `rows_with_mismatches`     | Rows that exist on both sides (matched by key) but have at least one compared column with a different value after normalization. |
| `mismatched_cells`         | Total number of individual cell-level mismatches across all mismatched rows. Always >= `rows_with_mismatches` (a row can have multiple mismatched cells). |
| `comparison_time_seconds`  | Wall-clock time of the comparison in seconds, rounded to 6 decimal places. |

**Invariant:** `missing_in_target + missing_in_source + rows_with_mismatches == 0` if and only if exit code is 0.

**Audit note:** To determine the total row count of the raw file before
filtering, see `details.read_rows_source` / `details.read_rows_target`. The
relationship is:
`read_rows_source - filters_applied.source_excluded_rows == source_rows`.

## details

```json
{
  "format": "csv",
  "keys": ["id"],
  "compared_columns": ["name", "value"],
  "read_rows_source": int,
  "read_rows_target": int,
  "filters_applied": { ... },
  "column_stats": { ... },
  "csv": { ... }
}
```

| Field               | Presence | Description |
|---------------------|----------|-------------|
| `format`            | always   | Always `"csv"` in V1. |
| `keys`              | always   | List of column names used as the composite primary key for matching rows between source and target. |
| `compared_columns`  | always   | Sorted list of non-key columns that were compared. Determined by the intersection of source and target columns, then narrowed by `compare.include_columns` / `compare.exclude_columns` if configured. |
| `read_rows_source`  | always   | Total rows read from the raw source CSV **before** any filtering. |
| `read_rows_target`  | always   | Total rows read from the raw target CSV **before** any filtering. |
| `filters_applied`   | always   | Breakdown of which rows were excluded and why. See below. |
| `column_stats`      | always   | Per-column mismatch counts. Always present; empty `{}` when `output.include_column_stats` is false or there are no compared columns. |
| `csv`               | optional | The **effective** CSV parsing settings used by the engine (after applying defaults). See below. Omitted in error reports where the engine did not run. |

**Invariants:**

```
read_rows_source - filters_applied.source_excluded_rows == summary.source_rows
read_rows_target - filters_applied.target_excluded_rows == summary.target_rows
```

**Audit note:** Compare `read_rows_source` to `source_rows`. If they differ,
rows were excluded by filtering. Check `filters_applied` for the breakdown.

### details.csv (optional)

The effective CSV parsing settings used by the engine. These reflect the
config values after applying defaults — if the user did not specify a
delimiter, this will show `","` (the default).

```json
{
  "delimiter": ",",
  "encoding": "utf-8",
  "header": true
}
```

| Field       | Description |
|-------------|-------------|
| `delimiter` | Column delimiter character used to parse both CSV files. Default `","`. |
| `encoding`  | Character encoding used to read both CSV files. Currently only `"utf-8"` is supported. |
| `header`    | Whether the first row of each CSV file is treated as a header row. Default `true`. |

**Audit note:** Check `details.csv` to confirm the engine parsed the files
with the expected settings. A wrong delimiter is a common cause of
"all rows mismatched" results.

### details.filters_applied

```json
{
  "exclude_keys_count": int,
  "source_excluded_rows": int,
  "target_excluded_rows": int,
  "source_excluded_rows_exclude_keys": int,
  "target_excluded_rows_exclude_keys": int,
  "source_excluded_rows_row_filters": int,
  "target_excluded_rows_row_filters": int,
  "row_filters": { ... }
}
```

**Always-present fields:**

| Field                                | Description |
|--------------------------------------|-------------|
| `exclude_keys_count`                 | Number of `exclude_keys` entries in the config. 0 when no keys are excluded. |
| `source_excluded_rows`               | **Total** rows excluded from source across all mechanisms. |
| `target_excluded_rows`               | **Total** rows excluded from target across all mechanisms. |
| `source_excluded_rows_exclude_keys`  | Rows excluded from source by the `exclude_keys` filter specifically. |
| `target_excluded_rows_exclude_keys`  | Rows excluded from target by the `exclude_keys` filter specifically. |
| `source_excluded_rows_row_filters`   | Rows excluded from source by `row_filters` specifically. 0 when row_filters is not configured or `apply_to` does not include source. |
| `target_excluded_rows_row_filters`   | Rows excluded from target by `row_filters` specifically. 0 when row_filters is not configured or `apply_to` does not include target. |

**Invariants:**

```
source_excluded_rows == source_excluded_rows_exclude_keys + source_excluded_rows_row_filters
target_excluded_rows == target_excluded_rows_exclude_keys + target_excluded_rows_row_filters
```

**Conditionally present field:**

| Field          | Presence | Description |
|----------------|----------|-------------|
| `row_filters`  | Only when config has `filters.row_filters` with >= 1 rule | Metadata about the row_filters configuration. Omitted entirely when row_filters is not enabled. |

```json
{
  "row_filters": {
    "count": int,
    "apply_to": "both" | "source" | "target",
    "mode": "exclude" | "include"
  }
}
```

- `count`: Number of filter rules configured.
- `apply_to`: Which sides the filters were applied to.
- `mode`: `"exclude"` removes matching rows; `"include"` keeps only matching rows.

**Audit note:** If `source_excluded_rows > 0`, auditors should review the
filter config to confirm the exclusions are intentional. The `excluded`
samples (see below) provide concrete examples of what was filtered out.

**Example (with row_filters enabled):**

```json
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
```

**Example (without row_filters):**

```json
{
  "exclude_keys_count": 0,
  "source_excluded_rows": 0,
  "target_excluded_rows": 0,
  "source_excluded_rows_exclude_keys": 0,
  "target_excluded_rows_exclude_keys": 0,
  "source_excluded_rows_row_filters": 0,
  "target_excluded_rows_row_filters": 0
}
```

### details.column_stats

Per-column mismatch counts for matched rows (rows present on both sides by
key). Always present in the JSON. Empty `{}` when
`output.include_column_stats` is false or when there are no compared columns.

```json
{
  "column_name": {
    "mismatched_count": int
  }
}
```

**Audit note:** Use this to identify which columns contribute the most
mismatches. The sum of all `mismatched_count` values equals
`summary.mismatched_cells`.

## samples

Tabular samples is a dict with four categories:

```json
{
  "missing_in_target": [ ... ],
  "missing_in_source": [ ... ],
  "value_mismatches": [ ... ],
  "excluded": [ ... ]
}
```

Each category is limited by `sampling.sample_limit_per_type` (falls back to
`sampling.sample_limit` when not set; default 200). When
`output.include_row_samples` is false, all four lists are empty. Entries
within each list are sorted by key values ascending for determinism.

### samples.missing_in_target / samples.missing_in_source

```json
{
  "line_number_source": int,
  "key": { "column": "value", ... },
  "row": { "column": "value", ... }
}
```

| Field                 | Description |
|-----------------------|-------------|
| `line_number_source`  | 1-based row number in the raw source CSV (for `missing_in_target`). |
| `line_number_target`  | 1-based row number in the raw target CSV (for `missing_in_source`). |
| `key`                 | Key column values that identify this row. |
| `row`                 | Non-key column values for the row. |

**Caveat:** `missing_in_target` entries have `line_number_source` (the row
exists in source). `missing_in_source` entries have `line_number_target`
instead (the row exists in target). The "other side" line number field is
absent because the row does not exist there.

### samples.value_mismatches

```json
{
  "line_number_source": int,
  "line_number_target": int,
  "key": { "column": "value", ... },
  "columns": {
    "column_name": {
      "source": "value",
      "target": "value"
    }
  }
}
```

| Field                 | Description |
|-----------------------|-------------|
| `line_number_source`  | 1-based row number in the raw source CSV. |
| `line_number_target`  | 1-based row number in the raw target CSV. |
| `key`                 | Key column values identifying this matched row. |
| `columns`             | Only the columns where values differ (after normalization). Each entry shows the raw (pre-normalization) source and target values. |

**Audit note:** The `columns` dict omits columns that matched. Only
genuinely different columns are listed, making it easy to see exactly what
changed.

### samples.excluded

```json
{
  "side": "source" | "target",
  "key": { "column": "value", ... },
  "line_number_source": int,
  "row": { "column": "value", ... },
  "reason": "exclude_keys" | "row_filters"
}
```

| Field                 | Description |
|-----------------------|-------------|
| `side`                | Which file the excluded row came from. |
| `key`                 | Key column values. |
| `line_number_source`  | 1-based line number (field is `line_number_source` when `side` is `"source"`, `line_number_target` when `side` is `"target"`). |
| `row`                 | Non-key column values. |
| `reason`              | Why the row was excluded: `"exclude_keys"` (matched an exclude_keys entry) or `"row_filters"` (matched/didn't match a row_filter rule). |

**Audit note:** Review excluded samples to confirm filters are not
inadvertently removing important data from the comparison.

---

## Full tabular example (no filters)

```json
{
  "type": "tabular",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "abc123...",
  "summary": {
    "source_rows": 3,
    "target_rows": 3,
    "missing_in_target": 0,
    "missing_in_source": 0,
    "rows_with_mismatches": 0,
    "mismatched_cells": 0,
    "comparison_time_seconds": 0.015
  },
  "details": {
    "format": "csv",
    "keys": ["id"],
    "compared_columns": ["name", "value"],
    "read_rows_source": 3,
    "read_rows_target": 3,
    "filters_applied": {
      "exclude_keys_count": 0,
      "source_excluded_rows": 0,
      "target_excluded_rows": 0,
      "source_excluded_rows_exclude_keys": 0,
      "target_excluded_rows_exclude_keys": 0,
      "source_excluded_rows_row_filters": 0,
      "target_excluded_rows_row_filters": 0
    },
    "column_stats": {
      "name": { "mismatched_count": 0 },
      "value": { "mismatched_count": 0 }
    },
    "csv": {
      "delimiter": ",",
      "encoding": "utf-8",
      "header": true
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

## Full tabular example (row_filters enabled, excluded samples)

```json
{
  "type": "tabular",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "def456...",
  "summary": {
    "source_rows": 2,
    "target_rows": 2,
    "missing_in_target": 0,
    "missing_in_source": 0,
    "rows_with_mismatches": 0,
    "mismatched_cells": 0,
    "comparison_time_seconds": 0.020
  },
  "details": {
    "format": "csv",
    "keys": ["id"],
    "compared_columns": ["name", "status", "value"],
    "read_rows_source": 4,
    "read_rows_target": 4,
    "filters_applied": {
      "exclude_keys_count": 0,
      "source_excluded_rows": 2,
      "target_excluded_rows": 2,
      "source_excluded_rows_exclude_keys": 0,
      "target_excluded_rows_exclude_keys": 0,
      "source_excluded_rows_row_filters": 2,
      "target_excluded_rows_row_filters": 2,
      "row_filters": {
        "count": 1,
        "apply_to": "both",
        "mode": "exclude"
      }
    },
    "column_stats": {
      "name": { "mismatched_count": 0 },
      "status": { "mismatched_count": 0 },
      "value": { "mismatched_count": 0 }
    },
    "csv": {
      "delimiter": ",",
      "encoding": "utf-8",
      "header": true
    }
  },
  "samples": {
    "missing_in_target": [],
    "missing_in_source": [],
    "value_mismatches": [],
    "excluded": [
      {
        "side": "source",
        "key": { "id": "2" },
        "line_number_source": 2,
        "row": { "status": "CANCELLED", "name": "Bob", "value": "200" },
        "reason": "row_filters"
      },
      {
        "side": "target",
        "key": { "id": "2" },
        "line_number_target": 2,
        "row": { "status": "CANCELLED", "name": "Bob", "value": "999" },
        "reason": "row_filters"
      }
    ]
  }
}
```

---

# TEXT REPORT

Produced when `type` is `"text"`.

## summary

```json
{
  "total_lines_source": int,
  "total_lines_target": int,
  "different_lines": int,
  "comparison_time_seconds": float
}
```

| Field                      | Description |
|----------------------------|-------------|
| `total_lines_source`       | Number of lines in the source file **after** processing (normalization, drop_lines_regex, ignore_blank_lines). Lines removed by dropping or blank-line filtering are not counted. |
| `total_lines_target`       | Same as above, for the target file. |
| `different_lines`          | Number of line-level differences. In `line_by_line` mode, this is the count of positional mismatches (including length differences). In `unordered_lines` mode, this is the sum of `abs(source_count - target_count)` across all distinct processed lines. |
| `comparison_time_seconds`  | Wall-clock time in seconds, rounded to 6 decimal places. |

**Audit note:** `total_lines_source` reflects post-processing counts, not
the raw file line count. If drop_lines_regex or ignore_blank_lines removed
lines, the raw file has more lines than reported here. Check
`details.rules_applied` for how many lines/substitutions were affected.

## details

```json
{
  "mode": "line_by_line" | "unordered_lines",
  "read_lines_source": int,
  "read_lines_target": int,
  "ignored_blank_lines_source": int,
  "ignored_blank_lines_target": int,
  "rules_applied": {
    "drop_lines_count": int,
    "replace_rules_count": int
  },
  "unordered_stats": { ... },
  "dropped_samples": [ ... ],
  "replacement_samples": [ ... ]
}
```

| Field                        | Presence | Description |
|------------------------------|----------|-------------|
| `mode`                       | always   | The comparison mode used. |
| `read_lines_source`          | always   | Total logical lines read from the raw source file **before** any processing (normalization, drop, ignore_blank_lines). See "line counting model" below. |
| `read_lines_target`          | always   | Total logical lines read from the raw target file **before** any processing. |

**Line counting model:** `read_lines_*` counts the number of logical lines
encountered by the file reader, where a "line" is text delimited by a newline
character. The `normalize_newlines` option affects how newline bytes are
interpreted (e.g. CRLF sequences may be translated to LF) but does **not**
change how many logical lines are counted — each newline-delimited segment is
one line regardless of the newline style. A file with N newline-delimited
lines will always report `read_lines_* == N`.
| `ignored_blank_lines_source` | always   | Number of lines removed from source by `ignore_blank_lines`. See "blank line definition" below. 0 when the option is disabled or the file has no blank lines. |
| `ignored_blank_lines_target` | always   | Same as above, for target. |

**Blank line definition:** The text engine applies a 7-step normalization
pipeline to each raw line in order: (1) normalize_newlines, (2) trim_lines,
(3) collapse_whitespace, (4) replace_regex, (5) ignore_blank_lines,
(6) drop_lines_regex, (7) case_insensitive. A line is counted in
`ignored_blank_lines_*` **only** when `normalize.ignore_blank_lines` is `true`
**and** the line is the empty string `""` at step 5 — that is, after trim,
collapse, and replace have already been applied. For example, a line
containing only whitespace will be counted here if `trim_lines` or
`collapse_whitespace` reduces it to `""`. This count does **not** include
lines removed by `drop_lines_regex` (step 6); those are counted separately in
`rules_applied.drop_lines_count`.
| `rules_applied`              | always   | Counts of rule application effects. See below. |
| `unordered_stats`            | only in `unordered_lines` mode | Aggregate breakdown of unordered mismatches. Omitted in `line_by_line` mode. Always present when mode is `unordered_lines`, regardless of whether differences exist. |
| `dropped_samples`            | only in `line_by_line` mode | List of audit samples for lines removed by `drop_lines_regex`. Capped at `--sample-limit` per side. Omitted in `unordered_lines` mode. See below. |
| `replacement_samples`        | only in `line_by_line` mode | List of audit samples for lines modified by `replace_regex`. Capped at `--sample-limit` per side. Omitted in `unordered_lines` mode. See below. |

**Invariant:**

```
read_lines_source == total_lines_source + ignored_blank_lines_source + (source's share of drop_lines_count)
```

**Audit note:** Compare `read_lines_source` to `total_lines_source`. If they
differ, lines were removed by blank-line filtering or drop_lines_regex. Check
`ignored_blank_lines_source` and `rules_applied.drop_lines_count` for the
breakdown. When `ignored_blank_lines_source == 0` and no drop rules are
configured, `read_lines_source == total_lines_source`.

### details.rules_applied

```json
{
  "drop_lines_count": int,
  "replace_rules_count": int
}
```

| Field                 | Description |
|-----------------------|-------------|
| `drop_lines_count`    | Total number of lines **actually dropped** by `drop_lines_regex` patterns across both source and target files combined. This is the count of lines removed, not the number of regex patterns configured. |
| `replace_rules_count` | Total number of regex substitutions **actually applied** by `replace_regex` rules across both source and target files combined. This is the count of individual replacements that fired, not the number of rules configured. A single rule can fire on many lines. |

**Caveat:** These are runtime effect counts, not config counts. If you need
to know how many rules were *configured*, check the original config.

### details.unordered_stats

Always present when `mode` is `"unordered_lines"`, regardless of whether
differences exist. Omitted entirely in `line_by_line` mode.

```json
{
  "source_only_lines": int,
  "target_only_lines": int,
  "distinct_mismatched_lines": int
}
```

| Field                      | Description |
|----------------------------|-------------|
| `source_only_lines`        | Sum of `max(source_count - target_count, 0)` across all distinct processed lines. Represents "excess" lines in source. |
| `target_only_lines`        | Sum of `max(target_count - source_count, 0)` across all distinct processed lines. Represents "excess" lines in target. |
| `distinct_mismatched_lines`| Count of distinct processed line strings where the source and target occurrence counts differ. |

**Invariant:** `source_only_lines + target_only_lines == summary.different_lines`.

**Audit note:** `distinct_mismatched_lines` tells you how many unique line
contents are involved in the mismatch. A high `different_lines` with a low
`distinct_mismatched_lines` means a few lines are repeated many times.

### details.dropped_samples (line_by_line mode only)

List of concrete line-level evidence of lines removed by `drop_lines_regex`.
Present only in `line_by_line` mode; omitted in `unordered_lines` mode.
Defaults to `[]` when no lines were dropped.

```json
[
  {
    "side": "source" | "target",
    "line_number": int,
    "raw": string,
    "processed": string
  }
]
```

| Field         | Description |
|---------------|-------------|
| `side`        | Which file the dropped line came from: `"source"` or `"target"`. |
| `line_number` | **Original raw file line number** (1-based) of the dropped line. |
| `raw`         | The original raw line content before any pipeline processing. |
| `processed`   | The line content at the point of drop — after trim, collapse, and replace, but **before** case folding. Shows exactly what the drop regex matched against. |

**Truncation:** Capped at `--sample-limit` entries per side (source samples
listed first, then target). When more lines are dropped than the limit, only
the first N encountered per side are stored.

**Audit note:** A line can appear in both `dropped_samples` and
`replacement_samples` if a replace rule fired on the line and then a drop
pattern matched. This is correct and useful — it shows the full
transformation chain.

### details.replacement_samples (line_by_line mode only)

List of concrete line-level evidence of lines modified by `replace_regex`.
Present only in `line_by_line` mode; omitted in `unordered_lines` mode.
Defaults to `[]` when no replacements fired.

```json
[
  {
    "side": "source" | "target",
    "line_number": int,
    "raw": string,
    "processed": string,
    "pattern": string | null,
    "replace": string | null
  }
]
```

| Field         | Description |
|---------------|-------------|
| `side`        | Which file the replaced line came from: `"source"` or `"target"`. |
| `line_number` | **Original raw file line number** (1-based) of the replaced line. |
| `raw`         | The original raw line content before any pipeline processing. |
| `processed`   | For kept lines: the final comparison value (after all pipeline steps including case folding). For dropped lines: the line content at the point of drop (pre-case-fold). |
| `pattern`     | The regex pattern string of the **first** replace rule that fired on this line. `null` if unavailable. |
| `replace`     | The replacement string of the **first** replace rule that fired on this line. `null` if unavailable. |

**Truncation:** Same as `dropped_samples` — capped per side at `--sample-limit`.

**Audit note:** `pattern` and `replace` show the first rule that matched.
If multiple replace rules fire on the same line, only the first is recorded.
Compare `raw` to `processed` to see the cumulative effect of all rules.

## samples (line_by_line mode)

A JSON list of diff entries. Limited to `--sample-limit` items (default
2000). When there are no differences, the list is empty.

```json
[
  {
    "line_number_source": int | null,
    "line_number_target": int | null,
    "raw_source": string,
    "raw_target": string,
    "processed_source": string,
    "processed_target": string,
    "source": string,
    "target": string,
    "processed_line_number_source": int | null,
    "processed_line_number_target": int | null
  }
]
```

| Field                            | Presence | Description |
|----------------------------------|----------|-------------|
| `line_number_source`             | always   | **Original raw file line number** (1-based) in the source file, before any normalization or dropping. `null` when source is exhausted (target is longer at this position). |
| `line_number_target`             | always   | Same as above, for target. `null` when target is exhausted (source is longer). |
| `raw_source`                     | always   | The **original raw line content** from source, before any pipeline processing (trim, collapse, replace, drop, case). Empty string `""` when source is exhausted. |
| `raw_target`                     | always   | Same as above, for target. |
| `processed_source`               | always   | The **processed line content** from source, after all pipeline steps (trim, collapse, replace, drop, case). This is the value used for comparison. Empty string `""` when source is exhausted. |
| `processed_target`               | always   | Same as above, for target. |
| `source`                         | always   | **Deprecated alias** of `processed_source`. Retained for backward compatibility — new consumers should use `processed_source`. |
| `target`                         | always   | **Deprecated alias** of `processed_target`. Retained for backward compatibility — new consumers should use `processed_target`. |
| `processed_line_number_source`   | only with `--debug-report` | 1-based index in the processed stream (after filtering/dropping). Debug-only field, omitted by default. `null` when source is exhausted. |
| `processed_line_number_target`   | only with `--debug-report` | Same as above, for target. |

**Audit note:** Use `raw_source` / `raw_target` to see the original file
content at each differing position, and `processed_source` / `processed_target`
to see the values after the normalization pipeline. When `replace_regex` or
other normalization is active, comparing raw vs processed helps auditors
understand exactly what the pipeline changed.

Use `line_number_source` / `line_number_target` to locate
differences in the original files. When one side is `null`, it means that
side has fewer lines at this position — look at the non-null side to see the
extra content.

## samples (unordered_lines mode)

In `unordered_lines` mode, `samples` is always set to `[]`. Aggregated
mismatch data is reported in `samples_agg` instead (see below).

## samples_agg (optional, unordered_lines mode only)

Present only when `mode` is `"unordered_lines"` **and** there are
mismatches. Omitted entirely in `line_by_line` mode, on error, or when there
are zero differences.

```json
[
  {
    "line": string,
    "source_count": int,
    "target_count": int,
    "source_line_numbers": [int, ...],
    "target_line_numbers": [int, ...],
    "source_line_numbers_truncated": bool,
    "target_line_numbers_truncated": bool
  }
]
```

| Field                            | Presence | Description |
|----------------------------------|----------|-------------|
| `line`                           | always   | The processed line content (after normalization/replacement). |
| `source_count`                   | always   | Number of times this line appears in the processed source. |
| `target_count`                   | always   | Number of times this line appears in the processed target. |
| `source_line_numbers`            | only with `--include-line-numbers` (default: on) | List of **original raw file line numbers** (1-based) where this processed line content occurred in source. Capped to `--max-line-numbers` entries (default: 10). Empty list `[]` when `source_count` is 0. |
| `target_line_numbers`            | only with `--include-line-numbers` | Same as above, for target. |
| `source_line_numbers_truncated`  | only with `--include-line-numbers` | `true` when the total occurrences in source exceed the stored line numbers (i.e. some line numbers were omitted due to the `--max-line-numbers` cap). `false` otherwise. |
| `target_line_numbers_truncated`  | only with `--include-line-numbers` | Same as above, for target. |

When `--no-include-line-numbers` is passed, all four line-number fields are
absent from every entry.

**Ordering:** Sorted by `abs(source_count - target_count)` descending, then
by `line` content lexicographically ascending for determinism.

**Truncation:** Limited to `--sample-limit` items (default 2000). The
top-N entries with the largest absolute count differences are kept.

**Audit note:** Entries at the top have the largest frequency imbalance.
Check `source_count` vs `target_count` to understand whether lines were
added, removed, or changed. When `source_count > 0` and `target_count > 0`
but counts differ, some occurrences of this line were added or removed.

---

## Full text example (line_by_line, with diffs)

```json
{
  "type": "text",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "aaa111...",
  "summary": {
    "total_lines_source": 3,
    "total_lines_target": 1,
    "different_lines": 2,
    "comparison_time_seconds": 0.0002
  },
  "details": {
    "mode": "line_by_line",
    "read_lines_source": 3,
    "read_lines_target": 1,
    "ignored_blank_lines_source": 0,
    "ignored_blank_lines_target": 0,
    "rules_applied": {
      "drop_lines_count": 0,
      "replace_rules_count": 0
    },
    "dropped_samples": [],
    "replacement_samples": []
  },
  "samples": [
    {
      "line_number_source": 2,
      "line_number_target": null,
      "raw_source": "bbb",
      "raw_target": "",
      "processed_source": "bbb",
      "processed_target": "",
      "source": "bbb",
      "target": ""
    },
    {
      "line_number_source": 3,
      "line_number_target": null,
      "raw_source": "ccc",
      "raw_target": "",
      "processed_source": "ccc",
      "processed_target": "",
      "source": "ccc",
      "target": ""
    }
  ]
}
```

## Full text example (line_by_line, with drop and replace rules)

```json
{
  "type": "text",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "ccc333...",
  "summary": {
    "total_lines_source": 2,
    "total_lines_target": 2,
    "different_lines": 0,
    "comparison_time_seconds": 0.0003
  },
  "details": {
    "mode": "line_by_line",
    "read_lines_source": 3,
    "read_lines_target": 3,
    "ignored_blank_lines_source": 0,
    "ignored_blank_lines_target": 0,
    "rules_applied": {
      "drop_lines_count": 2,
      "replace_rules_count": 4
    },
    "dropped_samples": [
      {
        "side": "source",
        "line_number": 1,
        "raw": "# header comment",
        "processed": "# header comment"
      },
      {
        "side": "target",
        "line_number": 1,
        "raw": "# target comment",
        "processed": "# target comment"
      }
    ],
    "replacement_samples": [
      {
        "side": "source",
        "line_number": 2,
        "raw": "alpha 2024-01-15 value",
        "processed": "alpha DATE value",
        "pattern": "\\d{4}-\\d{2}-\\d{2}",
        "replace": "DATE"
      },
      {
        "side": "target",
        "line_number": 2,
        "raw": "alpha 2025-12-01 value",
        "processed": "alpha DATE value",
        "pattern": "\\d{4}-\\d{2}-\\d{2}",
        "replace": "DATE"
      }
    ]
  },
  "samples": []
}
```

## Full text example (unordered_lines, with diffs)

```json
{
  "type": "text",
  "version": "1.1",
  "generated_at": "2026-01-15T12:00:00+00:00",
  "config_hash": "bbb222...",
  "summary": {
    "total_lines_source": 4,
    "total_lines_target": 4,
    "different_lines": 4,
    "comparison_time_seconds": 0.0005
  },
  "details": {
    "mode": "unordered_lines",
    "read_lines_source": 4,
    "read_lines_target": 4,
    "ignored_blank_lines_source": 0,
    "ignored_blank_lines_target": 0,
    "rules_applied": {
      "drop_lines_count": 0,
      "replace_rules_count": 0
    },
    "unordered_stats": {
      "source_only_lines": 2,
      "target_only_lines": 2,
      "distinct_mismatched_lines": 2
    }
  },
  "samples": [],
  "samples_agg": [
    {
      "line": "alpha",
      "source_count": 3,
      "target_count": 1,
      "source_line_numbers": [1, 2, 3],
      "target_line_numbers": [1],
      "source_line_numbers_truncated": false,
      "target_line_numbers_truncated": false
    },
    {
      "line": "gamma",
      "source_count": 0,
      "target_count": 2,
      "source_line_numbers": [],
      "target_line_numbers": [3, 4],
      "source_line_numbers_truncated": false,
      "target_line_numbers_truncated": false
    }
  ]
}
```

---

# Cross-engine audit model

Both engines follow a common **read → filter/process → compare** pipeline.
The report fields at each stage let auditors trace exactly how much data was
read, how much was excluded or transformed, and how much participated in the
final comparison.

### Tabular

```
details.read_rows_source          (raw rows read from CSV)
  − filters_applied.source_excluded_rows   (rows removed by exclude_keys + row_filters)
  ─────────────────────────────────
  = summary.source_rows            (rows participating in key-based diff)
```

Same relationship holds for target (`read_rows_target`, `target_excluded_rows`,
`target_rows`). The `filters_applied` sub-object provides a full breakdown of
which mechanism excluded how many rows.

### Text

```
details.read_lines_source         (raw lines read from file)
  − details.ignored_blank_lines_source     (lines removed by ignore_blank_lines)
  − (source's share of rules_applied.drop_lines_count)  (lines removed by drop_lines_regex)
  ─────────────────────────────────
  = summary.total_lines_source     (lines participating in comparison)
```

Same relationship holds for target. Note that `drop_lines_count` is a combined
total across both files; per-file drop counts are not broken out separately.

### Common principle

In both engines, **`summary` counts represent the dataset actually compared** —
they exclude anything removed by filtering or processing rules. To understand
the full picture of what was read vs. what was compared, compare the `details`
read counts to the `summary` counts. Any gap is accounted for by the
filtering/processing fields in `details`.

---

# Field presence summary

## Fields that differ between engines

| Field / Section             | Tabular          | Text                |
|-----------------------------|------------------|---------------------|
| `summary` fields            | source_rows, target_rows, missing_in_target, missing_in_source, rows_with_mismatches, mismatched_cells | total_lines_source, total_lines_target, different_lines |
| `details` fields            | format, keys, compared_columns, read_rows_source/target, filters_applied, column_stats | mode, read_lines_source/target, ignored_blank_lines_source/target, rules_applied, unordered_stats, dropped_samples, replacement_samples |
| `samples` type              | dict (4 category lists) | list (flat) |
| `samples_agg`               | never present    | unordered_lines mode only |

## Mode-dependent fields (text engine)

| Field                  | line_by_line     | unordered_lines       |
|------------------------|------------------|-----------------------|
| `samples`              | list of diffs    | always `[]`           |
| `samples_agg`          | absent           | present when diffs > 0 |
| `unordered_stats`      | absent           | always present        |
| `dropped_samples`      | always present (may be `[]`) | absent     |
| `replacement_samples`  | always present (may be `[]`) | absent     |
| `processed_line_number_*` | with `--debug-report` | N/A            |

## CLI-flag-dependent fields (text engine)

| CLI flag                       | Effect on report |
|--------------------------------|------------------|
| `--sample-limit N`             | Caps `samples` list (line_by_line) or `samples_agg` list (unordered) to N entries. |
| `--include-line-numbers` (default) | Includes `source_line_numbers`, `target_line_numbers`, and truncated flags in `samples_agg`. |
| `--no-include-line-numbers`    | Omits all four line-number fields from `samples_agg` entries. |
| `--max-line-numbers N`         | Caps stored line numbers per side per distinct line to N. Sets truncated flag when exceeded. |
| `--debug-report`               | Adds `processed_line_number_source/target` to line_by_line samples. |

## Config-dependent fields (tabular engine)

| Config option                       | Effect on report |
|-------------------------------------|------------------|
| `output.include_row_samples: false` | All four sample category lists are empty. |
| `output.include_column_stats: false`| `column_stats` is `{}`. |
| `filters.row_filters` with rules   | `row_filters` sub-object appears in `filters_applied`. |
| `filters.row_filters` absent/empty  | `row_filters` key is omitted from `filters_applied`. |

---

# Changelog

**This revision (v1.1 doc update, revision 8):**

- **Audit samples for dropped and replaced lines (text engine, line_by_line
  mode):** Two new lists in `details`: `dropped_samples` and
  `replacement_samples`. These provide concrete line-level evidence of which
  lines were removed by `drop_lines_regex` and which were modified by
  `replace_regex`, respectively. Each sample includes the original raw line,
  the processed content at the point of action, the original file line number,
  and which side (source/target) it came from. Replacement samples also
  include the first matching pattern and replacement string. Both lists are
  capped at `--sample-limit` per side and default to `[]`. Present only in
  `line_by_line` mode; omitted in `unordered_lines` mode. Backward
  compatible — older consumers that do not expect these fields will ignore
  them.

**Previous revision (v1.1 doc update, revision 7):**

- **`details.csv` in tabular reports:** Tabular reports now include an
  optional `details.csv` object containing the effective CSV parsing settings
  (`delimiter`, `encoding`, `header`) used by the engine. This helps auditors
  confirm the files were parsed with the expected settings. The field is
  omitted in error reports where the engine did not run. Backward compatible —
  older consumers that do not expect this field will ignore it.

**Previous revision (v1.1 doc update, revision 6):**

- **Pipeline ordering fix — drop_lines_regex before case folding:** The
  normalization pipeline now evaluates `drop_lines_regex` (step 6) **before**
  `case_insensitive` lowering (step 7). Previously, lowercasing ran first,
  which silently broke drop patterns containing uppercase literals (e.g.
  `\[HEARTBEAT\]` would not match the already-lowercased `[heartbeat]`).
  The corrected full order is: trim → collapse → replace_regex →
  ignore_blank → drop_lines_regex → case_insensitive.

**Previous revision (v1.1 doc update, revision 5):**

- **Raw + processed line content in line_by_line samples:** Each sample entry
  now includes `raw_source`, `raw_target`, `processed_source`, and
  `processed_target` fields. `raw_source` / `raw_target` contain the original
  file content before any pipeline processing; `processed_source` /
  `processed_target` contain the content after all normalization steps. The
  existing `source` / `target` fields are retained as backward-compatible
  aliases of `processed_source` / `processed_target`.
- **Pipeline ordering fix — replace_regex before case folding:** The
  normalization pipeline now applies `replace_regex` (step 4) **before**
  `case_insensitive` lowering. Previously `case_insensitive` ran first, which
  silently broke regex patterns containing uppercase literals (e.g. `T` and
  `Z` in ISO-8601 timestamps). The order at this revision was: trim →
  collapse → replace_regex → case_insensitive → ignore_blank → drop_lines.

**Previous revision (v1.1 doc update, revision 4):**

- **NULL-safe key matching:** Tabular engine now uses `IS NOT DISTINCT FROM`
  for key joins, correctly matching rows where key columns contain NULL values.
  Missing-row detection now checks `_reconify_line_number IS NULL` on the
  outer-joined side — a stable indicator that is always non-null for real rows —
  instead of checking key columns which are ambiguous when keys can be NULL.
- **`column_stats` always in `details`:** `details.column_stats` is now always
  present in tabular reports (including error reports). When
  `output.include_column_stats` is false or there are no compared columns, it
  is an empty dict `{}`. Previously it was placed at the report root and only
  present when enabled.
- **Encoding validation:** The tabular engine now validates that
  `csv.encoding` is `"utf-8"` (the only encoding DuckDB supports) and returns
  a clear `RUNTIME_ERROR` for unsupported encodings.
- **Resource cleanup:** DuckDB connection is now closed via `try/finally` on
  all return paths, including early-return error cases (INVALID_ROW_FILTERS,
  DUPLICATE_KEYS).

**Previous revision (v1.1 doc update, revision 3):**

- Clarified `ignored_blank_lines_*` semantics: defined exactly when a line is
  considered "blank" (empty string `""` at step 6 of the 7-step pipeline,
  after trim/collapse/case/replace), and clarified it does not include
  `drop_lines_regex` drops.
- Clarified `read_lines_*` line counting model: counts logical lines as
  encountered by the reader; `normalize_newlines` affects content normalization
  but not line count.
- Added "Cross-engine audit model" section showing the common
  read → filter/process → compare pipeline for both tabular and text engines,
  with concrete field arithmetic.
- Emphasized that `summary` counts in both engines represent the dataset
  actually compared, not the raw input.

**Previous revision (v1.1 doc update, revision 2):**

- Added `read_lines_source`, `read_lines_target`, `ignored_blank_lines_source`,
  `ignored_blank_lines_target` to text engine `details`. These provide raw file
  line counts and blank-line filtering counts for audit traceability, mirroring
  the tabular engine's `read_rows_source/target` pattern.
- Updated `config_hash` description to clarify best-effort semantics: canonical
  JSON hash when config is parsed, raw YAML hash on validation errors, empty
  string when file is unreadable.
- Clarified `unordered_stats` presence: always present in `unordered_lines`
  mode regardless of whether differences exist.
- Updated all text JSON examples to include the new detail fields.
- Added invariant and audit note for `read_lines` vs `total_lines` relationship.

**Previous revision (v1.1 doc update, revision 1):**

- Added per-field semantics tables with descriptions for every field in both
  engines.
- Added audit interpretation notes explaining how to reason about each field.
- Documented caveats: `missing_in_target` samples use `line_number_source`
  (not target), `missing_in_source` uses `line_number_target`; excluded
  samples use the line number field matching their `side`.
- Fixed `details.column_stats` description: it is always present in JSON
  (empty `{}` when disabled), not conditionally absent as previously stated.
- Clarified `rules_applied.drop_lines_count` and `replace_rules_count` are
  runtime effect counts (lines dropped / substitutions fired), not counts
  of configured rules.
- Clarified `total_lines_source` / `total_lines_target` are post-processing
  counts (after drop/blank-line filtering), not raw file line counts.
- Added invariant: `source_only_lines + target_only_lines == different_lines`
  for unordered mode.
- Added four full JSON examples: text line_by_line, text unordered_lines,
  tabular (no filters), tabular (with row_filters and excluded samples).
- Added field presence summary tables: engine differences, mode-dependent
  fields, CLI-flag-dependent fields, config-dependent fields.
- Documented `samples_agg` truncation behavior (`--sample-limit` keeps
  top-N by absolute count difference).
