# Reconify-cli – V1 Product Requirements Document

## 1. Product Overview

Reconify is a local-first, rule-based data reconciliation CLI tool designed for technical professionals.

All processing happens locally.
No source data is uploaded.

V1 is CLI-only and targets technical users.

---

## 2. Target Users

- Data engineers
- QA engineers
- ETL developers
- Migration consultants
- CI/CD validation pipelines

---

## 3. Core Value Proposition

- High performance (1M rows within ~60 seconds)
- Deterministic results
- YAML-driven reproducible configurations
- Machine-readable JSON report
- Local-only processing

---

## 4. Scope – V1

### 4.1 Supported Formats

Tabular:
- CSV
- TSV

Text:
- Any text file (line-based comparison)

---

# 5. TABULAR ENGINE

### 5.1 Key-based reconciliation

- Single or multi-column keys
- Detect:
  - missing in source
  - missing in target
  - matched rows
  - modified rows

---

### 5.2 Column Control

- ignore_columns
- tolerance per column
- string rules per column

---

### 5.3 Tolerance

Absolute tolerance only.

If:

abs(source - target) <= tolerance

→ considered equal

---

### 5.4 String Rules

Supported:

- case_insensitive
- trim
- contains
- regex_extract

Rules are applied after normalization.

---

### 5.5 Normalization (Source-side only)

Normalization generates virtual columns.

Constraints:

- Applied only to source
- Linear pipeline only
- No nested transforms
- No arbitrary expressions
- No referencing generated columns
- No cross-column dependency on generated columns

Supported operations:

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

---

### 5.6 Performance Requirements

- 1M rows within ~60 seconds on modern laptop
- Column projection mandatory
- Vectorized execution (DuckDB recommended)
- Sample-limited reporting (default max 2000 diff rows)

---

# 6. TEXT ENGINE

### 6.1 Modes

- line_by_line (default)
- unordered_lines

---

### 6.2 Normalization

- ignore_blank_lines
- trim_lines
- collapse_whitespace
- case_insensitive
- normalize_newlines

---

### 6.3 Filtering Rules

- drop_lines_regex
- replace_regex

Drop rules remove lines before comparison.
Replace rules substitute matching substrings before comparison.

---

# 7. CLI

Commands:

reconify run config.yaml
reconify run config.yaml --out report.json

Exit codes:

0 = no differences
1 = differences found
2 = configuration or runtime error

---

# 8. Reporting

Output file: report.json

Default:
- Summary
- Column statistics
- Up to 2000 sample diffs

Full diff export optional in future versions.

---

# 9. Out of Scope (V1)

- Database connections
- Parquet
- JSON structural diff
- Nested transform
- Expression language
- UI
- SaaS account system

---

# 10. Non-Functional

- Deterministic
- Reproducible
- Cross-platform
- Safe (no arbitrary code execution)