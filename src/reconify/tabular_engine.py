"""Tabular (CSV) comparison engine for Reconify V1 using DuckDB."""

from __future__ import annotations

import time
from typing import Any

import duckdb

from reconify.models import TabularConfig


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
            "filters_applied": {
                "exclude_keys_count": len(config.filters.exclude_keys),
                "source_excluded_rows": 0,
                "target_excluded_rows": 0,
            },
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
    con = duckdb.connect(":memory:")

    # ---------------------------------------------------------------
    # 1) READ CSV WITH LINE NUMBERS
    # ---------------------------------------------------------------
    for side, path in (("source", config.source), ("target", config.target)):
        con.execute(
            f"""
            CREATE TABLE {side}_raw AS
            SELECT
                row_number() OVER () AS _reconify_line_number,
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
    # 2) APPLY exclude_keys FILTER
    # ---------------------------------------------------------------
    source_excluded_rows = 0
    target_excluded_rows = 0
    keys = config.keys

    if config.filters.exclude_keys:
        _create_excluded_keys_table(con, config)

        for side in ("source", "target"):
            anti_join_cond = " AND ".join(f'{side}_raw."{k}" = _excluded_keys."{k}"' for k in keys)
            con.execute(
                f"""
                CREATE TABLE {side}_filtered AS
                SELECT s.*
                FROM {side}_raw s
                WHERE NOT EXISTS (
                    SELECT 1 FROM _excluded_keys
                    WHERE {anti_join_cond.replace(f"{side}_raw", "s")}
                )
                """
            )

        source_excluded_rows = (
            source_total - con.execute("SELECT count(*) FROM source_filtered").fetchone()[0]
        )
        target_excluded_rows = (
            target_total - con.execute("SELECT count(*) FROM target_filtered").fetchone()[0]
        )
    else:
        con.execute("CREATE TABLE source_filtered AS SELECT * FROM source_raw")
        con.execute("CREATE TABLE target_filtered AS SELECT * FROM target_raw")

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
    # 4) DETERMINE COMPARED COLUMNS
    # ---------------------------------------------------------------
    source_cols = _get_column_names(con, "source_filtered")
    target_cols = _get_column_names(con, "target_filtered")

    common_cols = set(source_cols) & set(target_cols)
    common_cols.discard("_reconify_line_number")
    for k in keys:
        common_cols.discard(k)

    compared_columns = sorted(common_cols)

    if config.compare.include_columns is not None:
        include_set = set(config.compare.include_columns)
        compared_columns = sorted(c for c in compared_columns if c in include_set)

    if config.compare.exclude_columns is not None:
        exclude_set = set(config.compare.exclude_columns)
        compared_columns = [c for c in compared_columns if c not in exclude_set]

    # ---------------------------------------------------------------
    # 5) NORMALIZATION LOGIC (build SQL expressions)
    # ---------------------------------------------------------------
    norm_exprs_source = {}
    norm_exprs_target = {}
    for col in compared_columns:
        norm_exprs_source[col] = _build_norm_expr(f's."{col}"', config)
        norm_exprs_target[col] = _build_norm_expr(f't."{col}"', config)

    # ---------------------------------------------------------------
    # 6) COMPUTE DIFFERENCES
    # ---------------------------------------------------------------
    key_join_cond = " AND ".join(f's."{k}" = t."{k}"' for k in keys)
    key_is_null_t = " AND ".join(f't."{k}" IS NULL' for k in keys)
    key_is_null_s = " AND ".join(f's."{k}" IS NULL' for k in keys)

    # A) missing_in_target
    missing_in_target_count = con.execute(
        f"""
        SELECT count(*)
        FROM source_filtered s
        LEFT JOIN target_filtered t ON {key_join_cond}
        WHERE {key_is_null_t}
        """
    ).fetchone()[0]

    # B) missing_in_source
    missing_in_source_count = con.execute(
        f"""
        SELECT count(*)
        FROM target_filtered t
        LEFT JOIN source_filtered s ON {key_join_cond}
        WHERE {key_is_null_s}
        """
    ).fetchone()[0]

    # C) value_mismatches
    if compared_columns:
        mismatch_conditions = " OR ".join(
            f"({norm_exprs_source[c]} IS DISTINCT FROM {norm_exprs_target[c]})"
            for c in compared_columns
        )

        cell_expr = _cell_count_expr(compared_columns, norm_exprs_source, norm_exprs_target)
        mismatch_query = f"""
        SELECT count(*) as row_count,
               COALESCE({cell_expr}, 0) as cell_count
        FROM source_filtered s
        INNER JOIN target_filtered t ON {key_join_cond}
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
    sample_limit = config.sampling.sample_limit
    per_type_limit = config.sampling.sample_limit_per_type or sample_limit
    include_samples = config.output.include_row_samples
    key_order = ", ".join(f's."{k}" ASC' for k in keys)
    key_order_t = ", ".join(f't."{k}" ASC' for k in keys)

    samples_missing_target: list[dict] = []
    samples_missing_source: list[dict] = []
    samples_mismatches: list[dict] = []
    samples_excluded: list[dict] = []

    if include_samples:
        # Missing in target samples
        s_all_cols = ", ".join(f's."{c}"' for c in source_cols if c != "_reconify_line_number")
        samples_missing_target = _fetch_missing_samples(
            con,
            f"""
            SELECT s._reconify_line_number as line_number,
                   {", ".join(f's."{k}"' for k in keys)}
                   {"," + s_all_cols if s_all_cols else ""}
            FROM source_filtered s
            LEFT JOIN target_filtered t ON {key_join_cond}
            WHERE {key_is_null_t}
            ORDER BY {key_order}
            LIMIT {per_type_limit}
            """,
            keys,
            source_cols,
            line_number_field="line_number_source",
        )

        # Missing in source samples
        t_all_cols = ", ".join(f't."{c}"' for c in target_cols if c != "_reconify_line_number")
        samples_missing_source = _fetch_missing_samples(
            con,
            f"""
            SELECT t._reconify_line_number as line_number,
                   {", ".join(f't."{k}"' for k in keys)}
                   {"," + t_all_cols if t_all_cols else ""}
            FROM target_filtered t
            LEFT JOIN source_filtered s ON {key_join_cond}
            WHERE {key_is_null_s}
            ORDER BY {key_order_t}
            LIMIT {per_type_limit}
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
                key_join_cond,
                per_type_limit,
            )

        # Excluded samples
        if config.filters.exclude_keys:
            samples_excluded = _fetch_excluded_samples(
                con,
                config,
                keys,
                source_cols,
                target_cols,
                key_join_cond,
                per_type_limit,
            )

    # ---------------------------------------------------------------
    # 10) BUILD REPORT
    # ---------------------------------------------------------------
    elapsed = time.monotonic() - start

    has_diffs = (
        missing_in_target_count > 0 or missing_in_source_count > 0 or rows_with_mismatches > 0
    )
    exit_code = 1 if has_diffs else 0

    report: dict[str, Any] = {
        "summary": {
            "source_rows": source_total,
            "target_rows": target_total,
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
            "filters_applied": {
                "exclude_keys_count": len(config.filters.exclude_keys),
                "source_excluded_rows": source_excluded_rows,
                "target_excluded_rows": target_excluded_rows,
            },
        },
        "samples": {
            "missing_in_target": samples_missing_target,
            "missing_in_source": samples_missing_source,
            "value_mismatches": samples_mismatches,
            "excluded": samples_excluded,
        },
    }

    # Optional column stats
    if config.output.include_column_stats and compared_columns:
        report["column_stats"] = _compute_column_stats(
            con,
            keys,
            compared_columns,
            norm_exprs_source,
            norm_exprs_target,
            key_join_cond,
        )

    con.close()
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
    """Build a normalized SQL expression for a column."""
    expr = col_expr

    # Apply normalize_nulls: NULLIF for each value
    for null_val in config.compare.normalize_nulls:
        expr = f"NULLIF({expr}, '{null_val}')"

    # Apply trim_whitespace
    if config.compare.trim_whitespace:
        expr = f"TRIM({expr})"

    # Apply case_insensitive
    if config.compare.case_insensitive:
        expr = f"LOWER({expr})"

    return expr


def _cell_count_expr(
    compared_columns: list[str],
    norm_source: dict[str, str],
    norm_target: dict[str, str],
) -> str:
    """Build SQL expression that counts mismatched cells per row, then sums."""
    parts = []
    for c in compared_columns:
        parts.append(
            f"CASE WHEN ({norm_source[c]} IS DISTINCT FROM {norm_target[c]}) THEN 1 ELSE 0 END"
        )
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
            if c != "_reconify_line_number" and c not in keys and c in row_dict:
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
    key_join_cond: str,
    limit: int,
) -> list[dict]:
    """Fetch value mismatch samples."""
    mismatch_cond = " OR ".join(
        f"({norm_source[c]} IS DISTINCT FROM {norm_target[c]})" for c in compared_columns
    )
    key_select = ", ".join(f's."{k}"' for k in keys)
    key_order = ", ".join(f's."{k}" ASC' for k in keys)

    # Build select for source and target values of compared columns
    col_selects = []
    for c in compared_columns:
        col_selects.append(f's."{c}" AS "source_{c}"')
        col_selects.append(f't."{c}" AS "target_{c}"')

    query = f"""
    SELECT s._reconify_line_number AS source_line,
           t._reconify_line_number AS target_line,
           {key_select},
           {", ".join(col_selects)}
    FROM source_filtered s
    INNER JOIN target_filtered t ON {key_join_cond}
    WHERE {mismatch_cond}
    ORDER BY {key_order}
    LIMIT {limit}
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
            # Apply normalization to check if truly different
            src_norm = _normalize_value(src_val, config)
            tgt_norm = _normalize_value(tgt_val, config)
            if src_norm != tgt_norm:
                columns[c] = {"source": src_val, "target": tgt_val}
        entry["columns"] = columns
        samples.append(entry)
    return samples


def _normalize_value(val: Any, config: TabularConfig) -> Any:
    """Apply normalization to a Python value for comparison."""
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


def _fetch_excluded_samples(
    con: duckdb.DuckDBPyConnection,
    config: TabularConfig,
    keys: list[str],
    source_cols: list[str],
    target_cols: list[str],
    key_join_cond: str,
    limit: int,
) -> list[dict]:
    """Fetch samples of rows that were excluded by exclude_keys."""
    samples: list[dict] = []
    per_side = limit

    join_cond_s = " AND ".join(f's."{k}" = _excluded_keys."{k}"' for k in keys)
    join_cond_t = " AND ".join(f't."{k}" = _excluded_keys."{k}"' for k in keys)

    # Source excluded rows
    s_data_cols = ", ".join(
        f's."{c}"' for c in source_cols if c != "_reconify_line_number" and c not in keys
    )
    s_key_select = ", ".join(f's."{k}"' for k in keys)
    s_order = ", ".join(f's."{k}" ASC' for k in keys)
    src_query = f"""
    SELECT s._reconify_line_number AS line_number,
           {s_key_select}
           {"," + s_data_cols if s_data_cols else ""}
    FROM source_raw s
    INNER JOIN _excluded_keys ON {join_cond_s}
    ORDER BY {s_order}
    LIMIT {per_side}
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
                if c != "_reconify_line_number" and c not in keys and c in row_dict
            },
            "reason": "exclude_keys",
        }
        samples.append(entry)

    # Target excluded rows
    t_data_cols = ", ".join(
        f't."{c}"' for c in target_cols if c != "_reconify_line_number" and c not in keys
    )
    t_key_select = ", ".join(f't."{k}"' for k in keys)
    t_order = ", ".join(f't."{k}" ASC' for k in keys)
    tgt_query = f"""
    SELECT t._reconify_line_number AS line_number,
           {t_key_select}
           {"," + t_data_cols if t_data_cols else ""}
    FROM target_raw t
    INNER JOIN _excluded_keys ON {join_cond_t}
    ORDER BY {t_order}
    LIMIT {per_side}
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
                if c != "_reconify_line_number" and c not in keys and c in row_dict
            },
            "reason": "exclude_keys",
        }
        samples.append(entry)

    # Sort by keys ASC
    samples.sort(key=lambda e: tuple(str(e["key"].get(k, "")) for k in keys))
    return samples


def _compute_column_stats(
    con: duckdb.DuckDBPyConnection,
    keys: list[str],
    compared_columns: list[str],
    norm_source: dict[str, str],
    norm_target: dict[str, str],
    key_join_cond: str,
) -> dict[str, Any]:
    """Compute per-column mismatch counts for matched rows."""
    stats: dict[str, Any] = {}
    for c in compared_columns:
        count = con.execute(
            f"""
            SELECT count(*)
            FROM source_filtered s
            INNER JOIN target_filtered t ON {key_join_cond}
            WHERE {norm_source[c]} IS DISTINCT FROM {norm_target[c]}
            """
        ).fetchone()[0]
        stats[c] = {"mismatched_count": count}
    return stats
