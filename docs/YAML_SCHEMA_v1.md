# Reconify YAML Schema – V1

Root field:

type: "tabular" | "text"

---

# TABULAR MODE

type: tabular

source: string
target: string

key:
  - column_name

ignore_columns:
  - column_name

tolerance:
  column_name: float

string_rules:
  column_name:
    - case_insensitive
    - trim
    - contains
    - regex_extract

normalization:
  new_column_name:
    - op: string
      args: list (optional)

Supported op values:

- map
- concat
- substr
- add
- sub
- mul
- div
- coalesce
- date_format
- upper
- lower
- trim
- round

Constraints:

- normalization must be a list (pipeline)
- no nested ops
- args may reference only original source columns or literals
- generated columns cannot be referenced
- no arbitrary expressions

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