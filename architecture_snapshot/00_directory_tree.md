# Architecture Snapshot — Directory Tree & Project Config

Generated: 2026-02-25

---

## Directory tree

```
reconify-cli/
├── .gitignore
├── CLAUDE_CONTEXT.md
├── Makefile
├── README.md
├── docs/
│   ├── PRD_v1.md
│   ├── REPORT_SCHEMA_v1.md
│   └── YAML_SCHEMA_v1.md
├── examples/
│   ├── tabular_full.yaml
│   ├── tabular_min.yaml
│   ├── text_full.yaml
│   └── text_min.yaml
├── poetry.lock
├── pyproject.toml
├── src/
│   └── reconify/
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py
│       ├── config_loader.py
│       ├── models.py
│       ├── report.py
│       ├── tabular_engine.py
│       └── text_engine.py
└── tests/
    ├── e2e/
    │   ├── README.md
    │   ├── cases/
    │   │   ├── tabular_compare_exclude_columns_exit0/
    │   │   ├── tabular_compare_include_columns_exit0/
    │   │   ├── tabular_compare_normalize_trim_case_exit0/
    │   │   ├── tabular_csv_delimiter_semicolon_exit0/
    │   │   ├── tabular_csv_header_false_exit0/
    │   │   ├── tabular_duplicate_keys_error/
    │   │   ├── tabular_exact_match/
    │   │   ├── tabular_exclude_keys/
    │   │   ├── tabular_invalid_exclude_keys/
    │   │   ├── tabular_invalid_row_filters_exit2/
    │   │   ├── tabular_missing_rows/
    │   │   ├── tabular_row_filters_apply_to_source_exit1/
    │   │   ├── tabular_row_filters_apply_to_target_exit1/
    │   │   ├── tabular_row_filters_exclude_both_exit0/
    │   │   ├── tabular_row_filters_include_mode_exit0/
    │   │   ├── tabular_row_filters_is_null_not_null_exit0/
    │   │   ├── tabular_row_filters_op_contains_exit0/
    │   │   ├── tabular_row_filters_op_in_exit0/
    │   │   ├── tabular_row_filters_op_not_equals_exit0/
    │   │   ├── tabular_row_filters_op_regex_exit0/
    │   │   ├── tabular_sampling_and_output_flags/
    │   │   ├── tabular_value_mismatch/
    │   │   ├── text_case_insensitive_true_exit0/
    │   │   ├── text_collapse_whitespace_true_exit0/
    │   │   ├── text_ignore_blank_lines/
    │   │   ├── text_line_by_line_debug_processed_numbers/
    │   │   ├── text_line_by_line_identical/
    │   │   ├── text_line_by_line_missing_lines/
    │   │   ├── text_line_by_line_original_line_numbers/
    │   │   ├── text_normalize_newlines_false/
    │   │   ├── text_normalize_newlines_true/
    │   │   ├── text_replace_and_drop_rules/
    │   │   ├── text_runtime_error_missing_file_exit2/
    │   │   ├── text_sample_limit_truncation_line_by_line/
    │   │   ├── text_sample_limit_truncation_unordered_agg/
    │   │   ├── text_trim_lines_false_unordered/
    │   │   ├── text_trim_lines_true_line_by_line_exit0/
    │   │   ├── text_unordered_counts_and_agg/
    │   │   ├── text_unordered_include_line_numbers_false/
    │   │   ├── text_unordered_large_mismatches/
    │   │   └── text_unordered_max_line_numbers_cap/
    │   ├── conftest.py
    │   ├── test_tabular_e2e.py
    │   └── test_text_e2e.py
    ├── test_cli_smoke.py
    ├── test_models.py
    ├── test_report.py
    ├── test_tabular_engine.py
    └── test_text_engine.py
```

---

## pyproject.toml

```toml
[tool.poetry]
name = "reconify-cli"
version = "0.1.0"
description = "Local-first, rule-based data reconciliation CLI tool"
authors = []
packages = [{ include = "reconify", from = "src" }]

[tool.poetry.scripts]
reconify = "reconify.cli:app"

[tool.poetry.dependencies]
python = "^3.11"
typer = ">=0.9,<1"
pydantic = "^2"
pyyaml = "^6"
duckdb = ">=0.9,<2"

[tool.poetry.group.dev.dependencies]
pytest = "^8"
ruff = ">=0.4,<1"

[tool.ruff]
target-version = "py311"
line-length = 100
src = ["src"]

[tool.ruff.lint]
select = ["E", "F", "I", "W", "UP", "B", "SIM", "RUF"]
ignore = ["B008"]  # Allow function calls in argument defaults (Typer pattern)

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["e2e: End-to-end tests (invoke CLI as subprocess)"]

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

---

## Current version

```
version = "0.1.0"
```

Report schema version: `"1.1"`
