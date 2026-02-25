# Reconify YAML Schema – V1

Root field:

type: "tabular" | "text"

---

# TABULAR MODE

type: tabular

source: string
target: string

keys:
  - column_name          # required, at least 1

## compare (optional)

compare:
  include_columns: [col1, col2]     # only compare these columns (default: all common)
  exclude_columns: [col3]           # exclude these from comparison
  trim_whitespace: bool             # default true
  case_insensitive: bool            # default false
  normalize_nulls: [str]            # default ["", "NULL", "null"]

## filters (optional)

filters:
  exclude_keys:
    - {key_col: value}              # must have exactly all key columns
  row_filters:
    apply_to: both | source | target  # default "both"
    mode: exclude | include           # default "exclude"
    rules:
      - column: string
        op: equals | not_equals | in | contains | regex | is_null | not_null
        value: string                 # for equals, not_equals, contains
        values: [str]                 # for in
        pattern: string               # for regex
        case_insensitive: bool        # per-rule override (inherits from compare)
        trim_whitespace: bool         # per-rule override (inherits from compare)

## ignore_columns (optional)

ignore_columns:
  - column_name                     # drop these from compared_columns

## tolerance (optional)

tolerance:
  column_name: float                # absolute tolerance, must be >= 0

Tolerance semantics:
- If both values can be cast to DOUBLE: match when abs(src - tgt) <= tolerance
- Otherwise: fall back to string comparison (IS NOT DISTINCT FROM)

## string_rules (optional, per-column)

string_rules:
  column_name:
    - case_insensitive              # LOWER() before comparison
    - trim                          # TRIM() before comparison
    - contains                      # bidirectional LIKE instead of equality
    - regex_extract:                # extract regex group before comparison
        pattern: string             # regex pattern (required, min_length=1)
        group: int                  # capture group number (default 1, >= 0)

String rules are applied per-column in addition to global compare settings.
- `trim` and `case_insensitive` modify the normalized expression
- `contains` changes the equality check to: src LIKE '%'||tgt||'%' OR tgt LIKE '%'||src||'%'
- `regex_extract` applies regexp_extract() to both sides before comparison

## normalization (optional, source-side only)

normalization:
  new_column_name:
    - op: string
      args: list (optional)

Creates virtual columns on the source side. These columns are included in
the comparison if the target has a column with the same name.

Pipeline convention:
- Step 1: args supply all inputs (column refs resolved by name, else string literals)
- Step 2+: previous result is implicitly the first operand

Supported op values:

| Op          | Args (step 1)            | Description                          |
|-------------|--------------------------|--------------------------------------|
| map         | col, val, repl, ...      | CASE WHEN col=val THEN repl ...      |
| concat      | col1, " ", col2, ...     | String concatenation (||)            |
| substr      | col, start [, length]    | SUBSTR()                             |
| add         | col1, col2               | TRY_CAST to DOUBLE, +                |
| sub         | col1, col2               | TRY_CAST to DOUBLE, -                |
| mul         | col1, col2               | TRY_CAST to DOUBLE, *                |
| div         | col1, col2               | TRY_CAST to DOUBLE, /                |
| coalesce    | col1, col2, ...          | COALESCE()                           |
| date_format | col, from_fmt, to_fmt    | strptime then strftime               |
| upper       | col                      | UPPER()                              |
| lower       | col                      | LOWER()                              |
| trim        | col                      | TRIM()                               |
| round       | col [, precision]        | ROUND(TRY_CAST to DOUBLE, precision) |

Constraints:
- Pipeline must not be empty
- Args may reference only original source columns or string/numeric literals
- Generated columns cannot reference other generated columns

## csv (optional)

csv:
  delimiter: string          # default ","; use "\t" for TSV files
  header: bool               # default true
  encoding: string           # default "utf-8" (only UTF-8 supported)

## sampling (optional)

sampling:
  sample_limit: int          # default 200
  sample_limit_per_type: int # default null (uses sample_limit)

## output (optional)

output:
  include_row_samples: bool     # default true
  include_column_stats: bool    # default true

---

# TEXT MODE

type: text

source: string
target: string

mode: line_by_line | unordered_lines (default: line_by_line)

normalize:
  ignore_blank_lines: bool (default false)
  trim_lines: bool (default false)
  collapse_whitespace: bool (default false)
  case_insensitive: bool (default false)
  normalize_newlines: bool (default true)

drop_lines_regex:
  - string (regex pattern)

replace_regex:
  - pattern: string
    replace: string
