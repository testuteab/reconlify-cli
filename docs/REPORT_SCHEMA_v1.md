# Reconify Report Schema – V1

Output: report.json

Root:

{
  "type": "tabular" | "text",
  "version": "1.0",
  "generated_at": "ISO-8601 timestamp",
  "config_hash": "sha256 of canonical config JSON",
  "summary": {...},
  "details": {...},
  "samples": [...]
}

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
    }
}

samples:

[
  {
    "line_number_source": int | null,
    "line_number_target": int | null,
    "source": string,
    "target": string
  }
]