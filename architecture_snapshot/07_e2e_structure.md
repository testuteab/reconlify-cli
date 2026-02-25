# Architecture Snapshot — E2E Test Structure

---

## E2E cases (tree format)

```
tests/e2e/cases/
├── tabular_compare_exclude_columns_exit0/
├── tabular_compare_include_columns_exit0/
├── tabular_compare_normalize_trim_case_exit0/
├── tabular_csv_delimiter_semicolon_exit0/
├── tabular_csv_header_false_exit0/
├── tabular_duplicate_keys_error/
├── tabular_exact_match/
├── tabular_exclude_keys/
├── tabular_invalid_exclude_keys/
├── tabular_invalid_row_filters_exit2/
├── tabular_missing_rows/
├── tabular_row_filters_apply_to_source_exit1/
├── tabular_row_filters_apply_to_target_exit1/
├── tabular_row_filters_exclude_both_exit0/
├── tabular_row_filters_include_mode_exit0/
├── tabular_row_filters_is_null_not_null_exit0/
├── tabular_row_filters_op_contains_exit0/
├── tabular_row_filters_op_in_exit0/
├── tabular_row_filters_op_not_equals_exit0/
├── tabular_row_filters_op_regex_exit0/
├── tabular_sampling_and_output_flags/
├── tabular_value_mismatch/
├── text_case_insensitive_true_exit0/
├── text_collapse_whitespace_true_exit0/
├── text_ignore_blank_lines/
├── text_line_by_line_debug_processed_numbers/
├── text_line_by_line_identical/
├── text_line_by_line_missing_lines/
├── text_line_by_line_original_line_numbers/
├── text_normalize_newlines_false/
├── text_normalize_newlines_true/
├── text_replace_and_drop_rules/
├── text_runtime_error_missing_file_exit2/
├── text_sample_limit_truncation_line_by_line/
├── text_sample_limit_truncation_unordered_agg/
├── text_trim_lines_false_unordered/
├── text_trim_lines_true_line_by_line_exit0/
├── text_unordered_counts_and_agg/
├── text_unordered_include_line_numbers_false/
├── text_unordered_large_mismatches/
└── text_unordered_max_line_numbers_cap/
```

22 tabular cases, 19 text cases = **41 E2E cases total**.
