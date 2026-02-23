"""Text comparison engine for Reconify V1."""

from __future__ import annotations

import re
import time
from collections import Counter
from typing import Any

from reconify.models import TextConfig, TextMode


def _process_lines(
    path: str,
    config: TextConfig,
) -> tuple[list[str], int, int]:
    """Read and process lines from a file according to config rules.

    Returns (processed_lines, drop_count, replace_count).
    """
    norm = config.normalize

    with open(path, newline="") as f:
        raw = f.read()

    # 1) Normalize newlines
    if norm.normalize_newlines:
        raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    lines = raw.split("\n")
    # Remove trailing empty element from final newline
    if lines and lines[-1] == "":
        lines.pop()

    drop_count = 0
    replace_count = 0
    result: list[str] = []

    # Compile regexes once
    replace_rules = [(re.compile(r.pattern), r.replace) for r in config.replace_regex]
    drop_patterns = [re.compile(p) for p in config.drop_lines_regex]

    for line in lines:
        # 2) trim_lines
        if norm.trim_lines:
            line = line.strip()

        # 3) collapse_whitespace
        if norm.collapse_whitespace:
            line = re.sub(r"\s+", " ", line).strip()

        # 4) case_insensitive
        if norm.case_insensitive:
            line = line.lower()

        # 5) replace_regex rules sequentially
        for pattern, replacement in replace_rules:
            line, n = pattern.subn(replacement, line)
            replace_count += n

        # 6) ignore_blank_lines
        if norm.ignore_blank_lines and line == "":
            continue

        # 7) drop_lines_regex
        dropped = False
        for dp in drop_patterns:
            if dp.search(line):
                drop_count += 1
                dropped = True
                break
        if dropped:
            continue

        result.append(line)

    return result, drop_count, replace_count


def _compare_line_by_line(
    source_lines: list[str],
    target_lines: list[str],
    sample_limit: int,
) -> tuple[int, list[dict[str, Any]]]:
    """Line-by-line comparison by index.

    Returns (different_lines, samples).
    """
    max_len = max(len(source_lines), len(target_lines))
    different = 0
    samples: list[dict[str, Any]] = []

    for i in range(max_len):
        src = source_lines[i] if i < len(source_lines) else ""
        tgt = target_lines[i] if i < len(target_lines) else ""

        is_missing = i >= len(source_lines) or i >= len(target_lines)

        if is_missing or src != tgt:
            different += 1
            if len(samples) < sample_limit:
                samples.append(
                    {
                        "line_number_source": i + 1,
                        "line_number_target": i + 1,
                        "source": src,
                        "target": tgt,
                    }
                )

    return different, samples


def _compare_unordered(
    source_lines: list[str],
    target_lines: list[str],
    sample_limit: int,
) -> tuple[int, list[dict[str, Any]], dict[str, int]]:
    """Unordered multiset comparison using Counter.

    Returns (different_lines, samples_agg, unordered_stats).
    """
    source_counts: Counter[str] = Counter(source_lines)
    target_counts: Counter[str] = Counter(target_lines)

    all_keys = set(source_counts) | set(target_counts)

    different = 0
    source_only = 0
    target_only = 0
    distinct_mismatched = 0
    mismatches: list[tuple[int, str, int, int]] = []  # (abs_diff, line, sc, tc)

    for key in all_keys:
        sc = source_counts.get(key, 0)
        tc = target_counts.get(key, 0)
        diff = abs(sc - tc)
        if diff == 0:
            continue
        different += diff
        distinct_mismatched += 1
        source_only += max(sc - tc, 0)
        target_only += max(tc - sc, 0)
        mismatches.append((diff, key, sc, tc))

    # Sort by abs(diff) DESC, then by line lexicographically for determinism
    mismatches.sort(key=lambda x: (-x[0], x[1]))

    samples_agg = [
        {"line": line, "source_count": sc, "target_count": tc}
        for _, line, sc, tc in mismatches[:sample_limit]
    ]

    unordered_stats = {
        "source_only_lines": source_only,
        "target_only_lines": target_only,
        "distinct_mismatched_lines": distinct_mismatched,
    }

    return different, samples_agg, unordered_stats


def compare_text(
    config: TextConfig,
    sample_limit: int = 2000,
) -> tuple[dict[str, Any], int]:
    """Compare two text files according to the given TextConfig.

    Returns (report_dict, exit_code) where report_dict matches
    REPORT_SCHEMA_v1 for type="text".

    Exit codes: 0 = no differences, 1 = differences found, 2 = error.
    """
    start = time.monotonic()

    try:
        source_lines, src_drop, src_replace = _process_lines(config.source, config)
        target_lines, tgt_drop, tgt_replace = _process_lines(config.target, config)
    except (FileNotFoundError, OSError) as exc:
        elapsed = time.monotonic() - start
        return {
            "summary": {
                "total_lines_source": 0,
                "total_lines_target": 0,
                "different_lines": 0,
                "comparison_time_seconds": round(elapsed, 6),
            },
            "details": {
                "mode": config.mode.value,
                "rules_applied": {
                    "drop_lines_count": 0,
                    "replace_rules_count": 0,
                },
            },
            "samples": [],
            "error": {
                "code": "RUNTIME_ERROR",
                "message": f"Failed to read file: {exc}",
                "details": str(exc),
            },
        }, 2

    total_drop = src_drop + tgt_drop
    total_replace = src_replace + tgt_replace

    if config.mode == TextMode.line_by_line:
        different, samples = _compare_line_by_line(source_lines, target_lines, sample_limit)
    else:
        different, samples_agg, unordered_stats = _compare_unordered(
            source_lines, target_lines, sample_limit
        )

    elapsed = time.monotonic() - start
    exit_code = 0 if different == 0 else 1

    details: dict[str, Any] = {
        "mode": config.mode.value,
        "rules_applied": {
            "drop_lines_count": total_drop,
            "replace_rules_count": total_replace,
        },
    }

    report_dict: dict[str, Any] = {
        "summary": {
            "total_lines_source": len(source_lines),
            "total_lines_target": len(target_lines),
            "different_lines": different,
            "comparison_time_seconds": round(elapsed, 6),
        },
        "details": details,
    }

    if config.mode == TextMode.line_by_line:
        report_dict["samples"] = samples
    else:
        details["unordered_stats"] = unordered_stats
        report_dict["samples"] = []
        report_dict["samples_agg"] = samples_agg

    return report_dict, exit_code
