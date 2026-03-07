"""Tabular (CSV) comparison engine for Reconlify V1 using DuckDB."""

from __future__ import annotations

import re
import time
from typing import Any

import duckdb

from reconlify.models import NormOp, NormStep, RowFilterOp, RowFilterRule, TabularConfig


def compare_tabular(config: TabularConfig) -> tuple[dict[str, Any], int]:
    """Compare two CSV files according to the given TabularConfig.

    Returns (report_dict, exit_code) where:
      0 = no differences
      1 = differences found
      2 = error (duplicate keys, file errors, etc.)
    """
    start = time.monotonic()

    try:
        return _run_comparison(config, start)
    except (FileNotFoundError, OSError) as exc:
        elapsed = time.monotonic() - start
        return _error_result(config, elapsed, "RUNTIME_ERROR", f"Failed to read file: {exc}")
    except duckdb.IOException as exc:
        elapsed = time.monotonic() - start
        return _error_result(config, elapsed, "RUNTIME_ERROR", f"Failed to read file: {exc}")


def _error_result(
    config: TabularConfig,
    elapsed: float,
    code: str,
    message: str,
) -> tuple[dict[str, Any], int]:
    return {
        "summary": {
            "source_rows": 0,
            "target_rows": 0,
            "missing_in_target": 0,
            "missing_in_source": 0,
            "rows_with_mismatches": 0,
            "mismatched_cells": 0,
            "comparison_time_seconds": round(elapsed, 6),
        },
        "details": {
            "format": config.format,
            "keys": list(config.keys),
            "compared_columns": [],
            "read_rows_source": 0,
            "read_rows_target": 0,
            "filters_applied": _empty_filters_applied(config),
            "column_stats": {},
        },
        "samples": {
            "missing_in_target": [],
            "missing_in_source": [],
            "value_mismatches": [],
            "excluded": [],
        },
        "error": {
            "code": code,
            "message": message,
            "details": message,
        },
    }, 2


def _run_comparison(config: TabularConfig, start: float) -> tuple[dict[str, Any], int]:
    enc = config.csv.encoding.lower().replace("-", "").replace("_", "")
    if enc != "utf8":
        elapsed = time.monotonic() - start
        return _error_result(
            config,
            elapsed,
            "RUNTIME_ERROR",
            f"Unsupported encoding: {config.csv.encoding!r}. Only UTF-8 is supported.",
        )

    con = duckdb.connect(":memory:")
    try:
        return _do_comparison(config, start, con)
    finally:
        con.close()


def _do_comparison(
    config: TabularConfig, start: float, con: duckdb.DuckDBPyConnection
) -> tuple[dict[str, Any], int]:
    # ---------------------------------------------------------------
    # 1) READ CSV WITH LINE NUMBERS
    # ---------------------------------------------------------------
    for side, path in (("source", config.source), ("target", config.target)):
        con.execute(
            f"""
            CREATE TABLE {side}_raw AS
            SELECT
                row_number() OVER () AS _reconlify_line_number,
                *
            FROM read_csv_auto(
                ?,
                delim = ?,
                header = ?,
                all_varchar = true
            )
            """,
            [path, config.csv.delimiter, config.csv.header],
        )

    source_total = con.execute("SELECT count(*) FROM source_raw").fetchone()[0]
    target_total = con.execute("SELECT count(*) FROM target_raw").fetchone()[0]

    # ---------------------------------------------------------------
    # 1b) PROJECTION - keep only columns needed downstream
    # ---------------------------------------------------------------
    keys = config.keys
    col_map = config.column_mapping
    reverse_map = {v: k for k, v in col_map.items()}

    src_raw_cols = set(_get_column_names(con, "source_raw"))
    tgt_raw_cols = set(_get_column_names(con, "target_raw"))

    # Validate column_mapping target columns exist
    for logical, tgt_physical in col_map.items():
        if tgt_physical not in tgt_raw_cols:
            elapsed = time.monotonic() - start
            return _error_result(
                config,
                elapsed,
                "INVALID_COLUMN_MAPPING",
                f"column_mapping[{logical!r}]: target column {tgt_physical!r} "
                f"does not exist in target file",
            )

    key_set = set(keys)

    # Matchable logical columns: source columns whose target equivalent exists
    matchable_logical: set[str] = set()
    for src_col in src_raw_cols - {"_reconlify_line_number"}:
        tgt_col = col_map.get(src_col, src_col)
        if tgt_col in tgt_raw_cols:
            matchable_logical.add(src_col)
    common_cols = matchable_logical

    # Normalization: input columns (source-side) + output columns
    norm_input_cols: set[str] = set()
    norm_output_cols = set(config.normalization.keys())
    for _col_name, pipeline in config.normalization.items():
        for step in pipeline:
            for arg in step.args:
                if isinstance(arg, str) and arg in src_raw_cols:
                    norm_input_cols.add(arg)

    # Row filter referenced columns
    rf_cols: set[str] = set()
    if config.filters.row_filters and config.filters.row_filters.rules:
        for rule in config.filters.row_filters.rules:
            rf_cols.add(rule.column)

    src_needed = (
        {"_reconlify_line_number"}
        | key_set
        | common_cols
        | norm_input_cols
        | (rf_cols & src_raw_cols)
    )

    # Target needed columns (physical names)
    tgt_needed_physical: set[str] = {"_reconlify_line_number"}
    for c in key_set | common_cols:
        tgt_needed_physical.add(col_map.get(c, c))
    for c in norm_output_cols:
        tgt_phys = col_map.get(c, c)
        if tgt_phys in tgt_raw_cols:
            tgt_needed_physical.add(tgt_phys)
    for c in rf_cols:
        tgt_phys = col_map.get(c, c)
        if tgt_phys in tgt_raw_cols:
            tgt_needed_physical.add(tgt_phys)

    # Source projection
    src_proj_cols = ", ".join(f'"{c}"' for c in sorted(src_needed))
    con.execute(f"CREATE TABLE source_proj AS SELECT {src_proj_cols} FROM source_raw")

    # Target projection (alias mapped columns to logical names)
    tgt_proj_parts = []
    tgt_result_names = []
    for c in sorted(tgt_needed_physical):
        logical = reverse_map.get(c, c)
        tgt_result_names.append(logical)
        if logical != c:
            tgt_proj_parts.append(f'"{c}" AS "{logical}"')
        else:
            tgt_proj_parts.append(f'"{c}"')

    # Check for aliasing collisions
    if len(tgt_result_names) != len(set(tgt_result_names)):
        seen_names: set[str] = set()
        dupe_names: set[str] = set()
        for n in tgt_result_names:
            if n in seen_names:
                dupe_names.add(n)
            seen_names.add(n)
        elapsed = time.monotonic() - start
        return _error_result(
            config,
            elapsed,
            "INVALID_COLUMN_MAPPING",
            f"column_mapping creates column name collisions in target: {sorted(dupe_names)}",
        )

    con.execute(
        f"CREATE TABLE target_proj AS SELECT {', '.join(tgt_proj_parts)} FROM target_raw"
    )

    # ---------------------------------------------------------------
    # 2) APPLY exclude_keys FILTER
    # ---------------------------------------------------------------
    source_excluded_rows = 0
    target_excluded_rows = 0

    if config.filters.exclude_keys:
        _create_excluded_keys_table(con, config)

        for side in ("source", "target"):
            anti_join_cond = " AND ".join(f'{side}_proj."{k}" = _excluded_keys."{k}"' for k in keys)
            con.execute(
                f"""
                CREATE TABLE {side}_after_ek AS
                SELECT s.*
                FROM {side}_proj s
                WHERE NOT EXISTS (
                    SELECT 1 FROM _excluded_keys
                    WHERE {anti_join_cond.replace(f"{side}_proj", "s")}
                )
                """
            )

        source_excluded_rows = (
            source_total - con.execute("SELECT count(*) FROM source_after_ek").fetchone()[0]
        )
        target_excluded_rows = (
            target_total - con.execute("SELECT count(*) FROM target_after_ek").fetchone()[0]
        )
    else:
        con.execute("CREATE TABLE source_after_ek AS SELECT * FROM source_proj")
        con.execute("CREATE TABLE target_after_ek AS SELECT * FROM target_proj")

    # ---------------------------------------------------------------
    # 2b) APPLY row_filters
    # ---------------------------------------------------------------
    src_excluded_rf = 0
    tgt_excluded_rf = 0
    rf_cfg = config.filters.row_filters

    if rf_cfg and rf_cfg.rules:
        # Validate columns exist
        available_cols = set(_get_column_names(con, "source_after_ek"))
        available_cols.discard("_reconlify_line_number")
        missing_cols = []
        for rule in rf_cfg.rules:
            if rule.column not in available_cols:
                missing_cols.append(rule.column)
        if missing_cols:
            elapsed = time.monotonic() - start
            return _error_result(
                config,
                elapsed,
                "INVALID_ROW_FILTERS",
                f"Row filter references missing columns: {missing_cols}",
            )

        predicate, params = _build_row_filter_predicate(rf_cfg.rules, config)

        apply_source = rf_cfg.apply_to in ("both", "source")
        apply_target = rf_cfg.apply_to in ("both", "target")
        is_exclude = rf_cfg.mode == "exclude"

        for side, should_apply in (
            ("source", apply_source),
            ("target", apply_target),
        ):
            if should_apply:
                where = f"NOT ({predicate})" if is_exclude else predicate
                con.execute(
                    f"""
                    CREATE TABLE {side}_filtered AS
                    SELECT * FROM {side}_after_ek
                    WHERE {where}
                    """,
                    params,
                )
            else:
                con.execute(f"CREATE TABLE {side}_filtered AS SELECT * FROM {side}_after_ek")

        if apply_source:
            before = con.execute("SELECT count(*) FROM source_after_ek").fetchone()[0]
            after = con.execute("SELECT count(*) FROM source_filtered").fetchone()[0]
            src_excluded_rf = before - after

        if apply_target:
            before = con.execute("SELECT count(*) FROM target_after_ek").fetchone()[0]
            after = con.execute("SELECT count(*) FROM target_filtered").fetchone()[0]
            tgt_excluded_rf = before - after
    else:
        con.execute("CREATE TABLE source_filtered AS SELECT * FROM source_after_ek")
        con.execute("CREATE TABLE target_filtered AS SELECT * FROM target_after_ek")

    # ---------------------------------------------------------------
    # 3) VALIDATE DUPLICATE KEYS
    # ---------------------------------------------------------------
    for side in ("source", "target"):
        key_cols = ", ".join(f'"{k}"' for k in keys)
        dup_result = con.execute(
            f"""
            SELECT {key_cols}, count(*) as cnt
            FROM {side}_filtered
            GROUP BY {key_cols}
            HAVING cnt > 1
            LIMIT 5
            """
        ).fetchall()
        if dup_result:
            elapsed = time.monotonic() - start
            dup_examples = [str(row[:-1]) for row in dup_result]
            return _error_result(
                config,
                elapsed,
                "DUPLICATE_KEYS",
                f"Duplicate keys found in {side}: {', '.join(dup_examples)}",
            )

    # ---------------------------------------------------------------
    # 3.5) NORMALIZATION VIRTUAL COLUMNS (source-side only)
    # ---------------------------------------------------------------
    src_compare_table = "source_filtered"
    tgt_compare_table = "target_filtered"

    if config.normalization:
        src_cols_for_norm = set(_get_column_names(con, "source_filtered"))
        src_cols_for_norm.discard("_reconlify_line_number")

        norm_col_exprs = []
        for col_name, pipeline in config.normalization.items():
            expr = _build_normalization_pipeline(pipeline, src_cols_for_norm)
            norm_col_exprs.append(f'({expr}) AS "{col_name}"')

        extra = ", " + ", ".join(norm_col_exprs)
        con.execute(
            f"CREATE TABLE source_normed AS SELECT source_filtered.*{extra} FROM source_filtered"
        )
        src_compare_table = "source_normed"

    # ---------------------------------------------------------------
    # 4) DETERMINE COMPARED COLUMNS
    # ---------------------------------------------------------------
    source_cols = _get_column_names(con, src_compare_table)
    target_cols = _get_column_names(con, tgt_compare_table)

    # Column lists for projection tables (excludes normalization virtual cols).
    # Used for excluded-key / row-filter samples that query pre-normalization tables.
    source_proj_cols = _get_column_names(con, "source_proj")
    target_proj_cols = _get_column_names(con, "target_proj")

    common_cols = set(source_cols) & set(target_cols)
    common_cols.discard("_reconlify_line_number")
    for k in keys:
        common_cols.discard(k)

    compared_columns = sorted(common_cols)

    if config.compare.include_columns is not None:
        include_set = set(config.compare.include_columns)
        compared_columns = sorted(c for c in compared_columns if c in include_set)

    if config.compare.exclude_columns is not None:
        exclude_set = set(config.compare.exclude_columns)
        compared_columns = [c for c in compared_columns if c not in exclude_set]

    if config.ignore_columns:
        ignore_set = set(config.ignore_columns)
        compared_columns = [c for c in compared_columns if c not in ignore_set]

    # ---------------------------------------------------------------
    # 5) BUILD PER-COLUMN NORMALIZATION + EQUALITY PREDICATES
    # ---------------------------------------------------------------
    norm_exprs_source = {}
    norm_exprs_target = {}
    eq_predicates = {}
    for col in compared_columns:
        norm_exprs_source[col] = _build_col_norm_expr(f's."{col}"', col, config)
        norm_exprs_target[col] = _build_col_norm_expr(f't."{col}"', col, config)
        eq_predicates[col] = _build_eq_predicate(
            norm_exprs_source[col], norm_exprs_target[col], col, config
        )

    # ---------------------------------------------------------------
    # 6) COMPUTE DIFFERENCES
    # ---------------------------------------------------------------
    key_join_cond = " AND ".join(f's."{k}" IS NOT DISTINCT FROM t."{k}"' for k in keys)
    key_is_null_t = "t._reconlify_line_number IS NULL"
    key_is_null_s = "s._reconlify_line_number IS NULL"

    # A) missing_in_target
    missing_in_target_count = con.execute(
        f"""
        SELECT count(*)
        FROM {src_compare_table} s
        LEFT JOIN {tgt_compare_table} t ON {key_join_cond}
        WHERE {key_is_null_t}
        """
    ).fetchone()[0]

    # B) missing_in_source
    missing_in_source_count = con.execute(
        f"""
        SELECT count(*)
        FROM {tgt_compare_table} t
        LEFT JOIN {src_compare_table} s ON {key_join_cond}
        WHERE {key_is_null_s}
        """
    ).fetchone()[0]

    # C) value_mismatches
    if compared_columns:
        mismatch_conditions = " OR ".join(f"NOT ({eq_predicates[c]})" for c in compared_columns)

        cell_expr = _cell_count_expr(compared_columns, eq_predicates)
        mismatch_query = f"""
        SELECT count(*) as row_count,
               COALESCE({cell_expr}, 0) as cell_count
        FROM {src_compare_table} s
        INNER JOIN {tgt_compare_table} t ON {key_join_cond}
        WHERE {mismatch_conditions}
        """
        mismatch_result = con.execute(mismatch_query).fetchone()
        rows_with_mismatches = mismatch_result[0]
        mismatched_cells = mismatch_result[1]
    else:
        rows_with_mismatches = 0
        mismatched_cells = 0

    # ---------------------------------------------------------------
    # 7-8) COLLECT SAMPLES
    # ---------------------------------------------------------------
    include_samples = config.output.include_row_samples
    key_order = ", ".join(f's."{k}" ASC' for k in keys)
    key_order_t = ", ".join(f't."{k}" ASC' for k in keys)

    samples_missing_target: list[dict] = []
    samples_missing_source: list[dict] = []
    samples_mismatches: list[dict] = []
    samples_excluded: list[dict] = []

    if include_samples:
        # Missing in target samples
        s_all_cols = ", ".join(f's."{c}"' for c in source_cols if c != "_reconlify_line_number")
        samples_missing_target = _fetch_missing_samples(
            con,
            f"""
            SELECT s._reconlify_line_number as line_number,
                   {", ".join(f's."{k}"' for k in keys)}
                   {"," + s_all_cols if s_all_cols else ""}
            FROM {src_compare_table} s
            LEFT JOIN {tgt_compare_table} t ON {key_join_cond}
            WHERE {key_is_null_t}
            ORDER BY {key_order}
            """,
            keys,
            source_cols,
            line_number_field="line_number_source",
        )

        # Missing in source samples
        t_all_cols = ", ".join(f't."{c}"' for c in target_cols if c != "_reconlify_line_number")
        samples_missing_source = _fetch_missing_samples(
            con,
            f"""
            SELECT t._reconlify_line_number as line_number,
                   {", ".join(f't."{k}"' for k in keys)}
                   {"," + t_all_cols if t_all_cols else ""}
            FROM {tgt_compare_table} t
            LEFT JOIN {src_compare_table} s ON {key_join_cond}
            WHERE {key_is_null_s}
            ORDER BY {key_order_t}
            """,
            keys,
            target_cols,
            line_number_field="line_number_target",
        )

        # Value mismatch samples
        if compared_columns and rows_with_mismatches > 0:
            samples_mismatches = _fetch_mismatch_samples(
                con,
                config,
                keys,
                compared_columns,
                norm_exprs_source,
                norm_exprs_target,
                eq_predicates,
                key_join_cond,
                src_compare_table,
                tgt_compare_table,
            )

        # Excluded samples (exclude_keys)
        if config.filters.exclude_keys:
            samples_excluded = _fetch_excluded_key_samples(
                con,
                config,
                keys,
                source_proj_cols,
                target_proj_cols,
            )

        # Excluded samples (row_filters)
        if rf_cfg and rf_cfg.rules and (src_excluded_rf > 0 or tgt_excluded_rf > 0):
            rf_excluded = _fetch_row_filter_excluded_samples(
                con,
                config,
                keys,
                source_proj_cols,
                target_proj_cols,
                predicate,
                params,
                rf_cfg,
            )
            samples_excluded.extend(rf_excluded)
            # Re-sort combined excluded samples by keys ASC
            samples_excluded.sort(key=lambda e: tuple(str(e["key"].get(k, "")) for k in keys))

    # ---------------------------------------------------------------
    # 10) BUILD REPORT
    # ---------------------------------------------------------------
    elapsed = time.monotonic() - start

    has_diffs = (
        missing_in_target_count > 0 or missing_in_source_count > 0 or rows_with_mismatches > 0
    )
    exit_code = 1 if has_diffs else 0

    source_post_filter = source_total - source_excluded_rows - src_excluded_rf
    target_post_filter = target_total - target_excluded_rows - tgt_excluded_rf

    report: dict[str, Any] = {
        "summary": {
            "source_rows": source_post_filter,
            "target_rows": target_post_filter,
            "missing_in_target": missing_in_target_count,
            "missing_in_source": missing_in_source_count,
            "rows_with_mismatches": rows_with_mismatches,
            "mismatched_cells": mismatched_cells,
            "comparison_time_seconds": round(elapsed, 6),
        },
        "details": {
            "format": config.format,
            "keys": list(keys),
            "compared_columns": compared_columns,
            "column_mapping": {k: v for k, v in col_map.items() if k != v},
            "read_rows_source": source_total,
            "read_rows_target": target_total,
            "filters_applied": _build_filters_applied(
                config,
                source_excluded_rows,
                target_excluded_rows,
                src_excluded_rf,
                tgt_excluded_rf,
            ),
        },
        "samples": {
            "missing_in_target": samples_missing_target,
            "missing_in_source": samples_missing_source,
            "value_mismatches": samples_mismatches,
            "excluded": samples_excluded,
        },
    }

    # Column stats: always present in details, empty {} when disabled
    if config.output.include_column_stats and compared_columns:
        report["details"]["column_stats"] = _compute_column_stats(
            con,
            compared_columns,
            eq_predicates,
            key_join_cond,
            src_compare_table,
            tgt_compare_table,
        )
    else:
        report["details"]["column_stats"] = {}

    return report, exit_code


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _create_excluded_keys_table(con: duckdb.DuckDBPyConnection, config: TabularConfig) -> None:
    """Create a temp table from exclude_keys entries."""
    keys = config.keys
    col_defs = ", ".join(f'"{k}" VARCHAR' for k in keys)
    con.execute(f"CREATE TABLE _excluded_keys ({col_defs})")

    for entry in config.filters.exclude_keys:
        placeholders = ", ".join("?" for _ in keys)
        values = [str(entry[k]) for k in keys]
        con.execute(f"INSERT INTO _excluded_keys VALUES ({placeholders})", values)


def _get_column_names(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    """Get column names for a table, preserving order."""
    result = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [row[1] for row in result]


def _build_norm_expr(col_expr: str, config: TabularConfig) -> str:
    """Build a normalized SQL expression for a column (global rules only)."""
    expr = col_expr

    # Apply normalize_nulls: NULLIF for each value
    for null_val in config.compare.normalize_nulls:
        expr = f"NULLIF({expr}, '{_escape_sql_str(null_val)}')"

    # Apply trim_whitespace
    if config.compare.trim_whitespace:
        expr = f"TRIM({expr})"

    # Apply case_insensitive
    if config.compare.case_insensitive:
        expr = f"LOWER({expr})"

    return expr


def _build_col_norm_expr(col_expr: str, col_name: str, config: TabularConfig) -> str:
    """Build a normalized SQL expression for a column, including per-column string_rules."""
    expr = col_expr

    # A) Global normalize_nulls
    for null_val in config.compare.normalize_nulls:
        expr = f"NULLIF({expr}, '{_escape_sql_str(null_val)}')"

    # B) Parse per-column string rules
    rules = config.string_rules.get(col_name, [])
    simple_rules: set[str] = set()
    regex_params = None
    for rule in rules:
        if isinstance(rule, str):
            simple_rules.add(rule)
        else:
            regex_params = rule.regex_extract

    # C) Trim: global OR per-column
    if config.compare.trim_whitespace or "trim" in simple_rules:
        expr = f"TRIM({expr})"

    # D) Case insensitive: global OR per-column
    if config.compare.case_insensitive or "case_insensitive" in simple_rules:
        expr = f"LOWER({expr})"

    # E) regex_extract (applied after trim/lower)
    if regex_params is not None:
        pat = _escape_sql_str(regex_params.pattern)
        expr = f"regexp_extract({expr}, '{pat}', {regex_params.group})"

    return expr


def _build_eq_predicate(src_expr: str, tgt_expr: str, col_name: str, config: TabularConfig) -> str:
    """Build a SQL equality predicate for a column pair.

    Returns a SQL expression that evaluates to TRUE when values are considered equal.
    """
    # Tolerance check (numeric with string fallback)
    if col_name in config.tolerance:
        tol = config.tolerance[col_name]
        return (
            f"(CASE"
            f" WHEN TRY_CAST({src_expr} AS DOUBLE) IS NOT NULL"
            f" AND TRY_CAST({tgt_expr} AS DOUBLE) IS NOT NULL"
            f" THEN abs(TRY_CAST({src_expr} AS DOUBLE) - TRY_CAST({tgt_expr} AS DOUBLE)) <= {tol}"
            f" ELSE {src_expr} IS NOT DISTINCT FROM {tgt_expr}"
            f" END)"
        )

    # Contains check (bidirectional LIKE)
    rules = config.string_rules.get(col_name, [])
    for rule in rules:
        if rule == "contains":
            return (
                f"COALESCE("
                f"({src_expr} IS NULL AND {tgt_expr} IS NULL)"
                f" OR {src_expr} LIKE '%' || {tgt_expr} || '%'"
                f" OR {tgt_expr} LIKE '%' || {src_expr} || '%'"
                f", FALSE)"
            )

    # Default: IS NOT DISTINCT FROM
    return f"({src_expr} IS NOT DISTINCT FROM {tgt_expr})"


def _normalize_col_value(val: Any, col_name: str, config: TabularConfig) -> Any:
    """Apply per-column normalization to a Python value."""
    if val is None:
        return None
    s = str(val)
    for null_val in config.compare.normalize_nulls:
        if s == null_val:
            return None

    rules = config.string_rules.get(col_name, [])
    simple_rules: set[str] = set()
    regex_params = None
    for rule in rules:
        if isinstance(rule, str):
            simple_rules.add(rule)
        else:
            regex_params = rule.regex_extract

    if config.compare.trim_whitespace or "trim" in simple_rules:
        s = s.strip()
    if config.compare.case_insensitive or "case_insensitive" in simple_rules:
        s = s.lower()
    if regex_params is not None:
        m = re.search(regex_params.pattern, s)
        s = m.group(regex_params.group) if m else ""

    return s


def _are_values_equal(src_val: Any, tgt_val: Any, col_name: str, config: TabularConfig) -> bool:
    """Python-side equality check for mismatch sample filtering."""
    src_norm = _normalize_col_value(src_val, col_name, config)
    tgt_norm = _normalize_col_value(tgt_val, col_name, config)

    # Tolerance check
    if col_name in config.tolerance:
        if src_norm is not None and tgt_norm is not None:
            try:
                src_f = float(src_norm)
                tgt_f = float(tgt_norm)
                return abs(src_f - tgt_f) <= config.tolerance[col_name]
            except (ValueError, TypeError):
                pass
        return src_norm == tgt_norm

    # Contains check
    rules = config.string_rules.get(col_name, [])
    for rule in rules:
        if rule == "contains":
            if src_norm is None and tgt_norm is None:
                return True
            if src_norm is None or tgt_norm is None:
                return False
            return str(src_norm) in str(tgt_norm) or str(tgt_norm) in str(src_norm)

    return src_norm == tgt_norm


def _cell_count_expr(
    compared_columns: list[str],
    eq_predicates: dict[str, str],
) -> str:
    """Build SQL expression that counts mismatched cells per row, then sums."""
    parts = []
    for c in compared_columns:
        parts.append(f"CASE WHEN NOT ({eq_predicates[c]}) THEN 1 ELSE 0 END")
    return "SUM(" + " + ".join(parts) + ")"


def _fetch_missing_samples(
    con: duckdb.DuckDBPyConnection,
    query: str,
    keys: list[str],
    all_cols: list[str],
    *,
    line_number_field: str,
) -> list[dict]:
    """Fetch missing row samples from a query result."""
    rows = con.execute(query).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc]
    samples = []
    for row in rows:
        row_dict = dict(zip(col_names, row, strict=False))
        entry: dict[str, Any] = {
            line_number_field: row_dict.pop("line_number"),
        }
        key_dict = {k: row_dict.get(k) for k in keys}
        entry["key"] = key_dict
        # Row data (exclude keys and line number)
        row_data = {}
        for c in all_cols:
            if c != "_reconlify_line_number" and c not in keys and c in row_dict:
                row_data[c] = row_dict[c]
        entry["row"] = row_data
        samples.append(entry)
    return samples


def _fetch_mismatch_samples(
    con: duckdb.DuckDBPyConnection,
    config: TabularConfig,
    keys: list[str],
    compared_columns: list[str],
    norm_source: dict[str, str],
    norm_target: dict[str, str],
    eq_predicates: dict[str, str],
    key_join_cond: str,
    src_compare_table: str,
    tgt_compare_table: str,
) -> list[dict]:
    """Fetch value mismatch samples."""
    mismatch_cond = " OR ".join(f"NOT ({eq_predicates[c]})" for c in compared_columns)
    key_select = ", ".join(f's."{k}"' for k in keys)
    key_order = ", ".join(f's."{k}" ASC' for k in keys)

    # Build select for source and target values of compared columns
    col_selects = []
    for c in compared_columns:
        col_selects.append(f's."{c}" AS "source_{c}"')
        col_selects.append(f't."{c}" AS "target_{c}"')

    query = f"""
    SELECT s._reconlify_line_number AS source_line,
           t._reconlify_line_number AS target_line,
           {key_select},
           {", ".join(col_selects)}
    FROM {src_compare_table} s
    INNER JOIN {tgt_compare_table} t ON {key_join_cond}
    WHERE {mismatch_cond}
    ORDER BY {key_order}
    """
    rows = con.execute(query).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc]

    samples = []
    for row in rows:
        row_dict = dict(zip(col_names, row, strict=False))
        entry: dict[str, Any] = {
            "line_number_source": row_dict["source_line"],
            "line_number_target": row_dict["target_line"],
            "key": {k: row_dict[k] for k in keys},
        }
        columns: dict[str, dict] = {}
        for c in compared_columns:
            src_val = row_dict.get(f"source_{c}")
            tgt_val = row_dict.get(f"target_{c}")
            if not _are_values_equal(src_val, tgt_val, c, config):
                columns[c] = {"source": src_val, "target": tgt_val}
        entry["columns"] = columns
        samples.append(entry)
    return samples


def _normalize_value(val: Any, config: TabularConfig) -> Any:
    """Apply normalization to a Python value for comparison (global rules only)."""
    if val is None:
        return None
    s = str(val)
    for null_val in config.compare.normalize_nulls:
        if s == null_val:
            return None
    if config.compare.trim_whitespace:
        s = s.strip()
    if config.compare.case_insensitive:
        s = s.lower()
    return s


def _fetch_excluded_key_samples(
    con: duckdb.DuckDBPyConnection,
    config: TabularConfig,
    keys: list[str],
    source_cols: list[str],
    target_cols: list[str],
) -> list[dict]:
    """Fetch samples of rows that were excluded by exclude_keys."""
    samples: list[dict] = []

    join_cond_s = " AND ".join(f's."{k}" = _excluded_keys."{k}"' for k in keys)
    join_cond_t = " AND ".join(f't."{k}" = _excluded_keys."{k}"' for k in keys)

    # Source excluded rows
    s_data_cols = ", ".join(
        f's."{c}"' for c in source_cols if c != "_reconlify_line_number" and c not in keys
    )
    s_key_select = ", ".join(f's."{k}"' for k in keys)
    s_order = ", ".join(f's."{k}" ASC' for k in keys)
    src_query = f"""
    SELECT s._reconlify_line_number AS line_number,
           {s_key_select}
           {"," + s_data_cols if s_data_cols else ""}
    FROM source_proj s
    INNER JOIN _excluded_keys ON {join_cond_s}
    ORDER BY {s_order}
    """
    rows = con.execute(src_query).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc]
    for row in rows:
        row_dict = dict(zip(col_names, row, strict=False))
        entry: dict[str, Any] = {
            "side": "source",
            "key": {k: row_dict.get(k) for k in keys},
            "line_number_source": row_dict["line_number"],
            "row": {
                c: row_dict[c]
                for c in source_cols
                if c != "_reconlify_line_number" and c not in keys and c in row_dict
            },
            "reason": "exclude_keys",
        }
        samples.append(entry)

    # Target excluded rows
    t_data_cols = ", ".join(
        f't."{c}"' for c in target_cols if c != "_reconlify_line_number" and c not in keys
    )
    t_key_select = ", ".join(f't."{k}"' for k in keys)
    t_order = ", ".join(f't."{k}" ASC' for k in keys)
    tgt_query = f"""
    SELECT t._reconlify_line_number AS line_number,
           {t_key_select}
           {"," + t_data_cols if t_data_cols else ""}
    FROM target_proj t
    INNER JOIN _excluded_keys ON {join_cond_t}
    ORDER BY {t_order}
    """
    rows = con.execute(tgt_query).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc]
    for row in rows:
        row_dict = dict(zip(col_names, row, strict=False))
        entry = {
            "side": "target",
            "key": {k: row_dict.get(k) for k in keys},
            "line_number_target": row_dict["line_number"],
            "row": {
                c: row_dict[c]
                for c in target_cols
                if c != "_reconlify_line_number" and c not in keys and c in row_dict
            },
            "reason": "exclude_keys",
        }
        samples.append(entry)

    # Sort by keys ASC
    samples.sort(key=lambda e: tuple(str(e["key"].get(k, "")) for k in keys))
    return samples


def _compute_column_stats(
    con: duckdb.DuckDBPyConnection,
    compared_columns: list[str],
    eq_predicates: dict[str, str],
    key_join_cond: str,
    src_compare_table: str,
    tgt_compare_table: str,
) -> dict[str, Any]:
    """Compute per-column mismatch counts for matched rows."""
    stats: dict[str, Any] = {}
    for c in compared_columns:
        count = con.execute(
            f"""
            SELECT count(*)
            FROM {src_compare_table} s
            INNER JOIN {tgt_compare_table} t ON {key_join_cond}
            WHERE NOT ({eq_predicates[c]})
            """
        ).fetchone()[0]
        stats[c] = {"mismatched_count": count}
    return stats


# ---------------------------------------------------------------------------
# Normalization pipeline helpers
# ---------------------------------------------------------------------------


def _resolve_norm_arg(arg: Any, source_cols: set[str]) -> str:
    """Resolve a normalization argument to a SQL expression.

    Column names become quoted identifiers; everything else becomes a string literal.
    """
    if isinstance(arg, str) and arg in source_cols:
        return f'"{arg}"'
    if isinstance(arg, (int, float)):
        return str(arg)
    return f"'{_escape_sql_str(str(arg))}'"


def _build_norm_step_sql(step: NormStep, prev_expr: str | None, source_cols: set[str]) -> str:
    """Build SQL for a single NormStep in a normalization pipeline.

    *prev_expr* is the result of the previous step (None for the first step).
    For the first step, arguments supply all inputs.  For subsequent steps,
    *prev_expr* is implicitly the first operand.
    """
    args = step.args

    if step.op == NormOp.map:
        if prev_expr:
            base_expr = prev_expr
            pair_args = args
        else:
            base_expr = _resolve_norm_arg(args[0], source_cols)
            pair_args = args[1:]
        cases = []
        for i in range(0, len(pair_args), 2):
            val = _resolve_norm_arg(pair_args[i], source_cols)
            repl = _resolve_norm_arg(pair_args[i + 1], source_cols)
            cases.append(f"WHEN {base_expr} = {val} THEN {repl}")
        return f"CASE {' '.join(cases)} ELSE {base_expr} END"

    if step.op == NormOp.concat:
        if prev_expr:
            parts = [prev_expr] + [_resolve_norm_arg(a, source_cols) for a in args]
        else:
            parts = [_resolve_norm_arg(a, source_cols) for a in args]
        return "(" + " || ".join(parts) + ")"

    if step.op == NormOp.substr:
        if prev_expr:
            base, start_idx = prev_expr, args[0]
            length = args[1] if len(args) > 1 else None
        else:
            base = _resolve_norm_arg(args[0], source_cols)
            start_idx = args[1]
            length = args[2] if len(args) > 2 else None
        if length is not None:
            return f"SUBSTR({base}, {start_idx}, {length})"
        return f"SUBSTR({base}, {start_idx})"

    if step.op in (NormOp.add, NormOp.sub, NormOp.mul, NormOp.div):
        op_map = {NormOp.add: "+", NormOp.sub: "-", NormOp.mul: "*", NormOp.div: "/"}
        op_str = op_map[step.op]
        if prev_expr:
            a = f"TRY_CAST({prev_expr} AS DOUBLE)"
            b = f"TRY_CAST({_resolve_norm_arg(args[0], source_cols)} AS DOUBLE)"
        else:
            a = f"TRY_CAST({_resolve_norm_arg(args[0], source_cols)} AS DOUBLE)"
            b = f"TRY_CAST({_resolve_norm_arg(args[1], source_cols)} AS DOUBLE)"
        return f"CAST(({a} {op_str} {b}) AS VARCHAR)"

    if step.op == NormOp.coalesce:
        if prev_expr:
            parts = [prev_expr] + [_resolve_norm_arg(a, source_cols) for a in args]
        else:
            parts = [_resolve_norm_arg(a, source_cols) for a in args]
        return f"COALESCE({', '.join(parts)})"

    if step.op == NormOp.date_format:
        if prev_expr:
            base, from_fmt, to_fmt = prev_expr, args[0], args[1]
        else:
            base = _resolve_norm_arg(args[0], source_cols)
            from_fmt, to_fmt = args[1], args[2]
        return (
            f"strftime(strptime({base}, "
            f"'{_escape_sql_str(str(from_fmt))}'), "
            f"'{_escape_sql_str(str(to_fmt))}')"
        )

    if step.op == NormOp.upper:
        base = prev_expr if prev_expr else _resolve_norm_arg(args[0], source_cols)
        return f"UPPER({base})"

    if step.op == NormOp.lower:
        base = prev_expr if prev_expr else _resolve_norm_arg(args[0], source_cols)
        return f"LOWER({base})"

    if step.op == NormOp.trim:
        base = prev_expr if prev_expr else _resolve_norm_arg(args[0], source_cols)
        return f"TRIM({base})"

    if step.op == NormOp.round:
        if prev_expr:
            base = prev_expr
            precision = int(args[0]) if args else 0
        else:
            base = _resolve_norm_arg(args[0], source_cols)
            precision = int(args[1]) if len(args) > 1 else 0
        return f"CAST(ROUND(TRY_CAST({base} AS DOUBLE), {precision}) AS VARCHAR)"

    raise ValueError(f"Unknown NormOp: {step.op}")


def _build_normalization_pipeline(pipeline: list[NormStep], source_cols: set[str]) -> str:
    """Chain a list of NormSteps into a single SQL expression."""
    prev_expr: str | None = None
    for step in pipeline:
        prev_expr = _build_norm_step_sql(step, prev_expr, source_cols)
    return prev_expr


# ---------------------------------------------------------------------------
# Row filters helpers
# ---------------------------------------------------------------------------


def _empty_filters_applied(config: TabularConfig) -> dict[str, Any]:
    """Return zeroed-out filters_applied dict."""
    rf = config.filters.row_filters
    result: dict[str, Any] = {
        "exclude_keys_count": len(config.filters.exclude_keys),
        "source_excluded_rows": 0,
        "target_excluded_rows": 0,
        "source_excluded_rows_exclude_keys": 0,
        "target_excluded_rows_exclude_keys": 0,
        "source_excluded_rows_row_filters": 0,
        "target_excluded_rows_row_filters": 0,
    }
    if rf and rf.rules:
        result["row_filters"] = {
            "count": len(rf.rules),
            "apply_to": rf.apply_to,
            "mode": rf.mode,
        }
    return result


def _build_filters_applied(
    config: TabularConfig,
    source_excluded_ek: int,
    target_excluded_ek: int,
    source_excluded_rf: int,
    target_excluded_rf: int,
) -> dict[str, Any]:
    """Build the filters_applied dict for the report."""
    rf = config.filters.row_filters
    result: dict[str, Any] = {
        "exclude_keys_count": len(config.filters.exclude_keys),
        "source_excluded_rows": source_excluded_ek + source_excluded_rf,
        "target_excluded_rows": target_excluded_ek + target_excluded_rf,
        "source_excluded_rows_exclude_keys": source_excluded_ek,
        "target_excluded_rows_exclude_keys": target_excluded_ek,
        "source_excluded_rows_row_filters": source_excluded_rf,
        "target_excluded_rows_row_filters": target_excluded_rf,
    }
    if rf and rf.rules:
        result["row_filters"] = {
            "count": len(rf.rules),
            "apply_to": rf.apply_to,
            "mode": rf.mode,
        }
    return result


def _build_rule_expr(
    rule: RowFilterRule,
    config: TabularConfig,
) -> tuple[str, list[Any]]:
    """Convert a single RowFilterRule to (sql_fragment, params).

    Uses DuckDB parameter binding (?) to avoid SQL injection.
    """
    col = f'"{rule.column}"'
    params: list[Any] = []

    # is_null / not_null don't need CAST or normalization
    if rule.op == RowFilterOp.is_null:
        return f"{col} IS NULL", []
    if rule.op == RowFilterOp.not_null:
        return f"{col} IS NOT NULL", []

    # String-ish ops: build normalized expression
    expr = f"CAST({col} AS VARCHAR)"

    # Apply normalize_nulls from config.compare
    for null_val in config.compare.normalize_nulls:
        expr = f"NULLIF({expr}, '{_escape_sql_str(null_val)}')"

    # Per-rule trim (inherits from compare if not set)
    trim = rule.trim_whitespace
    if trim is None:
        trim = config.compare.trim_whitespace
    if trim:
        expr = f"TRIM({expr})"

    # Per-rule case_insensitive (inherits from compare if not set)
    ci = rule.case_insensitive
    if ci is None:
        ci = config.compare.case_insensitive
    if ci:
        expr = f"LOWER({expr})"

    if rule.op == RowFilterOp.equals:
        val = _coerce_param(rule.value, ci)
        params.append(val)
        return f"{expr} = ?", params

    if rule.op == RowFilterOp.not_equals:
        val = _coerce_param(rule.value, ci)
        params.append(val)
        return f"{expr} <> ?", params

    if rule.op == RowFilterOp.in_:
        placeholders = ", ".join("?" for _ in rule.values)
        for v in rule.values:
            params.append(_coerce_param(v, ci))
        return f"{expr} IN ({placeholders})", params

    if rule.op == RowFilterOp.contains:
        val = _coerce_param(rule.value, ci)
        params.append(val)
        return f"{expr} LIKE '%' || ? || '%'", params

    # regex
    pat = rule.pattern
    if ci:
        pat = f"(?i){pat}"
    params.append(pat)
    return f"regexp_matches({expr}, ?)", params


def _coerce_param(value: Any, case_insensitive: bool) -> str:
    """Convert a filter value to a string param, lowering if needed."""
    s = str(value)
    if case_insensitive:
        s = s.lower()
    return s


def _escape_sql_str(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


def _build_row_filter_predicate(
    rules: list[RowFilterRule],
    config: TabularConfig,
) -> tuple[str, list[Any]]:
    """Build a combined AND predicate from a list of rules.

    Returns (sql_predicate, flat_params_list).
    """
    fragments: list[str] = []
    all_params: list[Any] = []

    for rule in rules:
        frag, params = _build_rule_expr(rule, config)
        fragments.append(f"({frag})")
        all_params.extend(params)

    predicate = " AND ".join(fragments) if fragments else "TRUE"
    return predicate, all_params


def _fetch_row_filter_excluded_samples(
    con: duckdb.DuckDBPyConnection,
    config: TabularConfig,
    keys: list[str],
    source_cols: list[str],
    target_cols: list[str],
    predicate: str,
    params: list[Any],
    rf_cfg: Any,
) -> list[dict]:
    """Fetch samples of rows excluded by row_filters."""
    samples: list[dict] = []
    is_exclude = rf_cfg.mode == "exclude"
    apply_source = rf_cfg.apply_to in ("both", "source")
    apply_target = rf_cfg.apply_to in ("both", "target")

    # For exclude mode: excluded rows are those matching the predicate
    # For include mode: excluded rows are those NOT matching the predicate
    where = predicate if is_exclude else f"NOT ({predicate})"

    if apply_source:
        samples.extend(
            _fetch_rf_side_samples(
                con,
                "source_after_ek",
                "source",
                keys,
                source_cols,
                where,
                params,
            )
        )

    if apply_target:
        samples.extend(
            _fetch_rf_side_samples(
                con,
                "target_after_ek",
                "target",
                keys,
                target_cols,
                where,
                params,
            )
        )

    samples.sort(key=lambda e: tuple(str(e["key"].get(k, "")) for k in keys))
    return samples


def _fetch_rf_side_samples(
    con: duckdb.DuckDBPyConnection,
    table: str,
    side: str,
    keys: list[str],
    cols: list[str],
    where: str,
    params: list[Any],
) -> list[dict]:
    """Fetch row_filter excluded samples for one side."""
    data_cols = ", ".join(
        f'r."{c}"' for c in cols if c != "_reconlify_line_number" and c not in keys
    )
    key_select = ", ".join(f'r."{k}"' for k in keys)
    key_order = ", ".join(f'r."{k}" ASC' for k in keys)

    query = f"""
    SELECT r._reconlify_line_number AS line_number,
           {key_select}
           {"," + data_cols if data_cols else ""}
    FROM {table} r
    WHERE {where}
    ORDER BY {key_order}
    """
    rows = con.execute(query, params).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc]

    ln_field = "line_number_source" if side == "source" else "line_number_target"
    samples: list[dict] = []
    for row in rows:
        row_dict = dict(zip(col_names, row, strict=False))
        entry: dict[str, Any] = {
            "side": side,
            "key": {k: row_dict.get(k) for k in keys},
            ln_field: row_dict["line_number"],
            "row": {
                c: row_dict[c]
                for c in cols
                if c != "_reconlify_line_number" and c not in keys and c in row_dict
            },
            "reason": "row_filters",
        }
        samples.append(entry)
    return samples
