# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Removed

- **Sample limits removed (breaking).** The `--sample-limit` CLI flag,
  `sampling.sample_limit`, and `sampling.sample_limit_per_type` config
  options have been removed. All diff samples, audit samples, and
  aggregated entries are now included in reports without truncation.
- **`TabularSampling` config section removed (breaking).** The `sampling:`
  block in tabular YAML configs is no longer recognized. Existing configs
  that include `sampling:` will fail validation — remove the section to fix.

## [1.0.0] - 2026-02-25

### Added

- Tabular engine: CSV/TSV reconciliation with key-based row matching (single or composite keys).
- Column control via `ignore_columns`, `compare.include_columns`, and `compare.exclude_columns`.
- Per-column numeric tolerance with absolute threshold and non-numeric fallback.
- Per-column string rules: `trim`, `case_insensitive`, `contains`, `regex_extract`.
- Source-side normalization pipelines with 13 operations (`concat`, `round`, `map`, etc.).
- Row filters: `exclude_keys` for specific key values and `row_filters` with `include`/`exclude` modes, `apply_to`, and seven filter operators.
- TSV support via configurable `csv.delimiter`.
- Text engine with `line_by_line` and `unordered_lines` comparison modes.
- Text normalization options: `trim_lines`, `collapse_whitespace`, `case_insensitive`, `ignore_blank_lines`, `normalize_newlines`.
- Text regex rules: `drop_lines_regex` and `replace_regex`.
- Deterministic JSON report output (schema v1.1) with `config_hash`, summary counts, detailed metadata, and sample diffs.
- Per-column mismatch statistics (`column_stats`) in tabular reports.
- Aggregated samples (`samples_agg`) for unordered text mode.
- YAML-based configuration with Pydantic v2 validation.
- CLI with `reconify run` command, `--out`, `--include-line-numbers`, `--max-line-numbers`, and `--debug-report` options.
- Exit codes: 0 (no differences), 1 (differences found), 2 (error).
- E2E test suite runnable via `make e2e`.
- Documentation: YAML schema reference, report schema reference, user guide, and PRD.

### Changed

- Report schema documentation aligned to implementation (v1.1).

### Notes

- This is the first public 1.0 release of Reconify.
- The CLI interface and report schema are intended to be stable, but minor breaking changes may occur before 1.1 as real-world usage informs refinements.
