"""Text comparison engine for Reconify V1."""

from __future__ import annotations

import re
import time
from collections import Counter
from itertools import zip_longest
from typing import Any

from reconify.models import TextConfig, TextMode

_SENTINEL = object()
_WS_RE = re.compile(r"\s+")


def _compile_rules(
    config: TextConfig,
) -> tuple[list[tuple[re.Pattern[str], str]], list[re.Pattern[str]]]:
    """Compile regex patterns from config once."""
    replace_rules = [(re.compile(r.pattern), r.replace) for r in config.replace_regex]
    drop_patterns = [re.compile(p) for p in config.drop_lines_regex]
    return replace_rules, drop_patterns


def _apply_pipeline(
    line: str,
    norm: Any,
    replace_rules: list[tuple[re.Pattern[str], str]],
    drop_patterns: list[re.Pattern[str]],
) -> tuple[str | None, int, bool, str, list[tuple[str, str, int]]]:
    """Apply the 7-step processing pipeline to a single line.

    Returns (processed_line_or_None, replace_count, was_dropped,
             dropped_content, rules_matched).
    None for processed_line means the line should be skipped (blank or dropped).
    dropped_content is the line content at the point of drop (pre-case-fold).
    rules_matched is a list of (pattern_str, replacement_str, match_count) for
    each rule that fired at least once on this line.
    """
    replace_count = 0
    rules_matched: list[tuple[str, str, int]] = []

    # 2) trim_lines
    if norm.trim_lines:
        line = line.strip()

    # 3) collapse_whitespace
    if norm.collapse_whitespace:
        line = _WS_RE.sub(" ", line).strip()

    # 4) replace_regex rules sequentially (before case folding so
    #    patterns can match original casing, e.g. "T" and "Z" in
    #    ISO-8601 timestamps)
    for pattern, replacement in replace_rules:
        line, n = pattern.subn(replacement, line)
        replace_count += n
        if n > 0:
            rules_matched.append((pattern.pattern, replacement, n))

    # 5) ignore_blank_lines (before drop/case so blank detection uses
    #    the post-replace content, same as before)
    if norm.ignore_blank_lines and line == "":
        return None, replace_count, False, "", rules_matched

    # 6) drop_lines_regex (before case folding so patterns match the
    #    original casing, e.g. [HEARTBEAT] still matches when
    #    case_insensitive=true)
    for dp in drop_patterns:
        if dp.search(line):
            return None, replace_count, True, line, rules_matched

    # 7) case_insensitive (last transform — only reached for kept lines)
    if norm.case_insensitive:
        line = line.lower()

    return line, replace_count, False, "", rules_matched


class _LineStream:
    """Streaming line processor yielding (processed_line, orig_line_num, processed_line_num, raw_line).

    After exhaustion, drop_count, replace_count, and total_lines are available.
    """

    __slots__ = (
        "_config",
        "_drop_patterns",
        "_path",
        "_replace_rules",
        "_side",
        "blank_lines_ignored",
        "drop_count",
        "dropped_samples",
        "raw_lines_count",
        "replace_count",
        "replacement_lines_affected",
        "replacement_samples",
        "total_lines",
    )

    def __init__(
        self,
        path: str,
        config: TextConfig,
        replace_rules: list[tuple[re.Pattern[str], str]],
        drop_patterns: list[re.Pattern[str]],
        side: str = "source",
    ) -> None:
        self._path = path
        self._config = config
        self._replace_rules = replace_rules
        self._drop_patterns = drop_patterns
        self._side = side
        self.blank_lines_ignored = 0
        self.drop_count = 0
        self.dropped_samples: list[dict[str, Any]] = []
        self.raw_lines_count = 0
        self.replace_count = 0
        self.replacement_lines_affected = 0
        self.replacement_samples: list[dict[str, Any]] = []
        self.total_lines = 0

    def __iter__(self):
        norm = self._config.normalize

        for raw_idx, raw_line in enumerate(self._raw_lines()):
            orig_line_num = raw_idx + 1  # 1-based

            result, rc, dropped, dropped_content, rules_matched = _apply_pipeline(
                raw_line,
                norm,
                self._replace_rules,
                self._drop_patterns,
            )
            self.replace_count += rc

            # Capture replacement samples (line may also be dropped)
            if rc > 0:
                self.replacement_lines_affected += 1
                self.replacement_samples.append(
                    {
                        "side": self._side,
                        "line_number": orig_line_num,
                        "raw": raw_line,
                        "processed": dropped_content if dropped else (result or ""),
                        "rules": [
                            {"pattern": pat, "replace": repl, "matches": n}
                            for pat, repl, n in rules_matched
                        ],
                    }
                )

            if dropped:
                self.drop_count += 1
                # Capture dropped samples
                self.dropped_samples.append(
                    {
                        "side": self._side,
                        "line_number": orig_line_num,
                        "raw": raw_line,
                        "processed": dropped_content,
                    }
                )
            elif result is None:
                self.blank_lines_ignored += 1
            if result is None:
                continue
            self.total_lines += 1
            yield result, orig_line_num, self.total_lines, raw_line

    def _raw_lines(self):
        """Yield raw lines from the file, streaming in both modes."""
        norm = self._config.normalize
        if norm.normalize_newlines:
            # Universal newline mode — Python translates \r\n, \r to \n
            with open(self._path) as f:
                for line in f:
                    if line.endswith("\n"):
                        line = line[:-1]
                    self.raw_lines_count += 1
                    yield line
        else:
            # Preserve original newlines — strip only trailing \n so
            # CRLF lines keep their \r while LF lines do not.
            with open(self._path, newline="") as f:
                for line in f:
                    if line.endswith("\n"):
                        line = line[:-1]
                    self.raw_lines_count += 1
                    yield line


def _compare_line_by_line(
    source_stream: _LineStream,
    target_stream: _LineStream,
    *,
    debug_report: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    """Stream line-by-line comparison using zip_longest.

    Returns (different_lines, samples).
    """
    different = 0
    samples: list[dict[str, Any]] = []

    for src_item, tgt_item in zip_longest(
        source_stream,
        target_stream,
        fillvalue=_SENTINEL,
    ):
        src_missing = src_item is _SENTINEL
        tgt_missing = tgt_item is _SENTINEL

        # Tuple layout: (processed_line, orig_line_num, processed_line_num, raw_line)
        src_processed = "" if src_missing else src_item[0]
        tgt_processed = "" if tgt_missing else tgt_item[0]
        src_orig = None if src_missing else src_item[1]
        tgt_orig = None if tgt_missing else tgt_item[1]
        src_raw = "" if src_missing else src_item[3]
        tgt_raw = "" if tgt_missing else tgt_item[3]

        if src_missing or tgt_missing or src_processed != tgt_processed:
            different += 1
            entry: dict[str, Any] = {
                "line_number_source": src_orig,
                "line_number_target": tgt_orig,
                "raw_source": src_raw,
                "raw_target": tgt_raw,
                "processed_source": src_processed,
                "processed_target": tgt_processed,
                "source": src_processed,
                "target": tgt_processed,
            }
            if debug_report:
                entry["processed_line_number_source"] = None if src_missing else src_item[2]
                entry["processed_line_number_target"] = None if tgt_missing else tgt_item[2]
            samples.append(entry)

    return different, samples


def _compare_unordered(
    source_stream: _LineStream,
    target_stream: _LineStream,
    *,
    include_line_numbers: bool = True,
    max_line_numbers: int = 0,
) -> tuple[int, list[dict[str, Any]], dict[str, int]]:
    """Unordered multiset comparison: streaming Counter build + full collection.

    Returns (different_lines, samples_agg, unordered_stats).

    *max_line_numbers*: 0 means unlimited; positive integer caps per line.
    """
    unlimited = max_line_numbers <= 0
    source_counts: Counter[str] = Counter()
    target_counts: Counter[str] = Counter()
    src_index: dict[str, list[int]] = {}
    tgt_index: dict[str, list[int]] = {}

    # Build counters and line indexes by streaming
    # Tuple layout: (processed_line, orig_line_num, processed_line_num, raw_line)
    for line, orig_num, _, _raw in source_stream:
        source_counts[line] += 1
        if include_line_numbers:
            lst = src_index.setdefault(line, [])
            if unlimited or len(lst) < max_line_numbers:
                lst.append(orig_num)

    for line, orig_num, _, _raw in target_stream:
        target_counts[line] += 1
        if include_line_numbers:
            lst = tgt_index.setdefault(line, [])
            if unlimited or len(lst) < max_line_numbers:
                lst.append(orig_num)

    # --- Compute stats and collect ALL mismatched lines in two passes. ---
    different = 0
    source_only = 0
    target_only = 0
    distinct_mismatched = 0
    mismatched: list[tuple[int, str, int, int]] = []  # (diff, line, sc, tc)

    # Pass 1: keys present in source_counts.
    for key, sc in source_counts.items():
        tc = target_counts.get(key, 0)
        diff = abs(sc - tc)
        if diff == 0:
            continue
        different += diff
        distinct_mismatched += 1
        source_only += max(sc - tc, 0)
        target_only += max(tc - sc, 0)
        mismatched.append((diff, key, sc, tc))

    # Pass 2: keys only in target_counts (source_count = 0).
    for key, tc in target_counts.items():
        if key in source_counts:
            continue  # already handled in pass 1
        different += tc
        distinct_mismatched += 1
        target_only += tc
        mismatched.append((tc, key, 0, tc))

    # Sort for deterministic output: abs_diff DESC, line ASC.
    mismatched.sort(key=lambda e: (-e[0], e[1]))

    samples_agg: list[dict[str, Any]] = []
    for _diff, line, sc, tc in mismatched:
        entry: dict[str, Any] = {
            "line": line,
            "source_count": sc,
            "target_count": tc,
        }
        if include_line_numbers:
            src_nums = src_index.get(line, [])
            tgt_nums = tgt_index.get(line, [])
            entry["source_line_numbers"] = src_nums
            entry["target_line_numbers"] = tgt_nums
            entry["source_line_numbers_truncated"] = sc > len(src_nums)
            entry["target_line_numbers_truncated"] = tc > len(tgt_nums)
        samples_agg.append(entry)

    unordered_stats = {
        "source_only_lines": source_only,
        "target_only_lines": target_only,
        "distinct_mismatched_lines": distinct_mismatched,
    }

    return different, samples_agg, unordered_stats


def compare_text(
    config: TextConfig,
    *,
    include_line_numbers: bool = True,
    max_line_numbers: int = 0,
    debug_report: bool = False,
) -> tuple[dict[str, Any], int]:
    """Compare two text files according to the given TextConfig.

    Returns (report_dict, exit_code) where report_dict matches
    REPORT_SCHEMA_v1 for type="text".

    Exit codes: 0 = no differences, 1 = differences found, 2 = error.
    """
    start = time.monotonic()

    try:
        replace_rules, drop_patterns = _compile_rules(config)
        src_stream = _LineStream(
            config.source,
            config,
            replace_rules,
            drop_patterns,
            side="source",
        )
        tgt_stream = _LineStream(
            config.target,
            config,
            replace_rules,
            drop_patterns,
            side="target",
        )

        if config.mode == TextMode.line_by_line:
            different, samples = _compare_line_by_line(
                src_stream,
                tgt_stream,
                debug_report=debug_report,
            )
        else:
            different, samples_agg, unordered_stats = _compare_unordered(
                src_stream,
                tgt_stream,
                include_line_numbers=include_line_numbers,
                max_line_numbers=max_line_numbers,
            )
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
                "read_lines_source": 0,
                "read_lines_target": 0,
                "ignored_blank_lines_source": 0,
                "ignored_blank_lines_target": 0,
                "rules_applied": {
                    "drop_lines_count": 0,
                    "replace_rules_count": len(config.replace_regex),
                    "replacement_lines_affected": 0,
                    "replacement_applications": 0,
                },
            },
            "samples": [],
            "error": {
                "code": "RUNTIME_ERROR",
                "message": f"Failed to read file: {exc}",
                "details": str(exc),
            },
        }, 2

    total_drop = src_stream.drop_count + tgt_stream.drop_count
    total_replace = src_stream.replace_count + tgt_stream.replace_count
    total_lines_affected = (
        src_stream.replacement_lines_affected + tgt_stream.replacement_lines_affected
    )

    elapsed = time.monotonic() - start
    exit_code = 0 if different == 0 else 1

    details: dict[str, Any] = {
        "mode": config.mode.value,
        "read_lines_source": src_stream.raw_lines_count,
        "read_lines_target": tgt_stream.raw_lines_count,
        "ignored_blank_lines_source": src_stream.blank_lines_ignored,
        "ignored_blank_lines_target": tgt_stream.blank_lines_ignored,
        "rules_applied": {
            "drop_lines_count": total_drop,
            "replace_rules_count": len(config.replace_regex),
            "replacement_lines_affected": total_lines_affected,
            "replacement_applications": total_replace,
        },
        "normalize": config.normalize.model_dump(),
    }

    report_dict: dict[str, Any] = {
        "summary": {
            "total_lines_source": src_stream.total_lines,
            "total_lines_target": tgt_stream.total_lines,
            "different_lines": different,
            "comparison_time_seconds": round(elapsed, 6),
        },
        "details": details,
    }

    details["dropped_samples"] = src_stream.dropped_samples + tgt_stream.dropped_samples
    details["replacement_samples"] = (
        src_stream.replacement_samples + tgt_stream.replacement_samples
    )

    if config.mode == TextMode.line_by_line:
        report_dict["samples"] = samples
    else:
        details["unordered_stats"] = unordered_stats
        report_dict["samples"] = []
        report_dict["samples_agg"] = samples_agg

        # Warn when unlimited line numbers produces very large arrays
        _LINE_NUMBER_WARN_THRESHOLD = 5000
        if include_line_numbers and max_line_numbers <= 0:
            large = [
                e["line"][:80]
                for e in samples_agg
                if len(e.get("source_line_numbers", [])) > _LINE_NUMBER_WARN_THRESHOLD
                or len(e.get("target_line_numbers", [])) > _LINE_NUMBER_WARN_THRESHOLD
            ]
            if large:
                report_dict.setdefault("warnings", []).append(
                    f"Large line_numbers arrays: {len(large)} distinct line(s) "
                    f"exceed {_LINE_NUMBER_WARN_THRESHOLD} entries. "
                    f"Use --max-line-numbers to cap if report size is a concern."
                )

    return report_dict, exit_code
