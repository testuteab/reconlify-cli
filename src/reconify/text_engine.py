"""Text comparison engine for Reconify V1."""

from __future__ import annotations

import heapq
import re
import time
from collections import Counter
from itertools import zip_longest
from typing import Any

from reconify.models import TextConfig, TextMode

_SENTINEL = object()


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
) -> tuple[str | None, int, bool]:
    """Apply the 7-step processing pipeline to a single line.

    Returns (processed_line_or_None, replace_count, was_dropped).
    None means the line should be skipped (blank or dropped).
    """
    replace_count = 0

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
        return None, replace_count, False

    # 7) drop_lines_regex
    for dp in drop_patterns:
        if dp.search(line):
            return None, replace_count, True

    return line, replace_count, False


class _LineStream:
    """Streaming line processor yielding (line, orig_line_num, processed_line_num).

    After exhaustion, drop_count, replace_count, and total_lines are available.
    """

    __slots__ = (
        "_config",
        "_drop_patterns",
        "_path",
        "_replace_rules",
        "blank_lines_ignored",
        "drop_count",
        "raw_lines_count",
        "replace_count",
        "total_lines",
    )

    def __init__(
        self,
        path: str,
        config: TextConfig,
        replace_rules: list[tuple[re.Pattern[str], str]],
        drop_patterns: list[re.Pattern[str]],
    ) -> None:
        self._path = path
        self._config = config
        self._replace_rules = replace_rules
        self._drop_patterns = drop_patterns
        self.blank_lines_ignored = 0
        self.drop_count = 0
        self.raw_lines_count = 0
        self.replace_count = 0
        self.total_lines = 0

    def __iter__(self):
        norm = self._config.normalize

        for raw_idx, raw_line in enumerate(self._raw_lines()):
            orig_line_num = raw_idx + 1  # 1-based

            result, rc, dropped = _apply_pipeline(
                raw_line,
                norm,
                self._replace_rules,
                self._drop_patterns,
            )
            self.replace_count += rc
            if dropped:
                self.drop_count += 1
            elif result is None:
                self.blank_lines_ignored += 1
            if result is None:
                continue
            self.total_lines += 1
            yield result, orig_line_num, self.total_lines

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
    sample_limit: int,
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

        src_line = "" if src_missing else src_item[0]
        tgt_line = "" if tgt_missing else tgt_item[0]
        src_orig = None if src_missing else src_item[1]
        tgt_orig = None if tgt_missing else tgt_item[1]

        if src_missing or tgt_missing or src_line != tgt_line:
            different += 1
            if len(samples) < sample_limit:
                entry: dict[str, Any] = {
                    "line_number_source": src_orig,
                    "line_number_target": tgt_orig,
                    "source": src_line,
                    "target": tgt_line,
                }
                if debug_report:
                    entry["processed_line_number_source"] = None if src_missing else src_item[2]
                    entry["processed_line_number_target"] = None if tgt_missing else tgt_item[2]
                samples.append(entry)

    return different, samples


class _ReverseLexKey:
    """Wrapper that reverses lexicographic ordering of a string.

    Used as a heap key so that the *worst* item (largest line content for
    a given diff) sits at the heap root and can be evicted in O(log k).

    The desired output order is abs_diff DESC, line ASC.  In a min-heap
    the root is the smallest element — the one we want to evict first.
    The "worst kept" item has the smallest diff; among equal diffs, the
    largest line (last alphabetically).  Storing (diff, _ReverseLexKey)
    makes the heap root hold exactly that worst item:
      - smallest diff naturally floats to root
      - reversed lex means the largest line compares as smallest
    """

    __slots__ = ("_s",)

    def __init__(self, s: str) -> None:
        self._s = s

    def __lt__(self, other: _ReverseLexKey) -> bool:
        return self._s > other._s

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ReverseLexKey):
            return NotImplemented
        return self._s == other._s


def _compare_unordered(
    source_stream: _LineStream,
    target_stream: _LineStream,
    sample_limit: int,
    *,
    include_line_numbers: bool = True,
    max_line_numbers: int = 10,
) -> tuple[int, list[dict[str, Any]], dict[str, int]]:
    """Unordered multiset comparison: streaming Counter build + bounded top-N.

    Returns (different_lines, samples_agg, unordered_stats).
    """
    source_counts: Counter[str] = Counter()
    target_counts: Counter[str] = Counter()
    src_index: dict[str, list[int]] = {}
    tgt_index: dict[str, list[int]] = {}

    # Build counters and line indexes by streaming
    for line, orig_num, _ in source_stream:
        source_counts[line] += 1
        if include_line_numbers:
            lst = src_index.setdefault(line, [])
            if len(lst) < max_line_numbers:
                lst.append(orig_num)

    for line, orig_num, _ in target_stream:
        target_counts[line] += 1
        if include_line_numbers:
            lst = tgt_index.setdefault(line, [])
            if len(lst) < max_line_numbers:
                lst.append(orig_num)

    # --- Compute stats and collect top-N samples in two passes. ---
    # Two passes over the Counter dicts avoids building a union set of all
    # distinct keys, which would duplicate every key string and spike memory
    # when files contain millions of distinct lines.
    different = 0
    source_only = 0
    target_only = 0
    distinct_mismatched = 0

    # Bounded min-heap for top-N samples.  Each element is a 5-tuple:
    #   (diff, _ReverseLexKey(line), line, source_count, target_count)
    #
    # The heap root (index 0) is the WORST kept item — smallest diff,
    # and among equal diffs the largest line (via reversed lex ordering).
    # This is exactly the item we want to evict when a better candidate
    # arrives.  heapreplace swaps the root in O(log k).
    heap: list[tuple[int, _ReverseLexKey, str, int, int]] = []

    def _is_better(
        diff: int,
        line: str,
        worst: tuple[int, _ReverseLexKey, str, int, int],
    ) -> bool:
        """Check if (diff, line) is strictly better than the worst heap entry.

        Better = higher diff, or same diff with lexicographically smaller line.
        The worst entry stores (diff, _, raw_line, ...) at indices 0 and 2.
        """
        if diff != worst[0]:
            return diff > worst[0]
        return line < worst[2]

    def _heap_push(diff: int, line: str, sc: int, tc: int) -> None:
        if sample_limit <= 0:
            return
        if len(heap) < sample_limit:
            heapq.heappush(heap, (diff, _ReverseLexKey(line), line, sc, tc))
        elif _is_better(diff, line, heap[0]):
            heapq.heapreplace(heap, (diff, _ReverseLexKey(line), line, sc, tc))

    # Pass 1: keys present in source_counts.
    # Covers keys that appear in both sides AND keys only in source.
    for key, sc in source_counts.items():
        tc = target_counts.get(key, 0)
        diff = abs(sc - tc)
        if diff == 0:
            continue
        different += diff
        distinct_mismatched += 1
        source_only += max(sc - tc, 0)
        target_only += max(tc - sc, 0)
        _heap_push(diff, key, sc, tc)

    # Pass 2: keys only in target_counts (source_count = 0).
    for key, tc in target_counts.items():
        if key in source_counts:
            continue  # already handled in pass 1
        different += tc
        distinct_mismatched += 1
        target_only += tc
        _heap_push(tc, key, 0, tc)

    # Sort heap contents for deterministic output: abs_diff DESC, line ASC.
    heap.sort(key=lambda e: (-e[0], e[2]))

    samples_agg: list[dict[str, Any]] = []
    for _diff, _rkey, line, sc, tc in heap:
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
    sample_limit: int = 2000,
    *,
    include_line_numbers: bool = True,
    max_line_numbers: int = 10,
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
        )
        tgt_stream = _LineStream(
            config.target,
            config,
            replace_rules,
            drop_patterns,
        )

        if config.mode == TextMode.line_by_line:
            different, samples = _compare_line_by_line(
                src_stream,
                tgt_stream,
                sample_limit,
                debug_report=debug_report,
            )
        else:
            different, samples_agg, unordered_stats = _compare_unordered(
                src_stream,
                tgt_stream,
                sample_limit,
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

    total_drop = src_stream.drop_count + tgt_stream.drop_count
    total_replace = src_stream.replace_count + tgt_stream.replace_count

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
            "replace_rules_count": total_replace,
        },
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

    if config.mode == TextMode.line_by_line:
        report_dict["samples"] = samples
    else:
        details["unordered_stats"] = unordered_stats
        report_dict["samples"] = []
        report_dict["samples_agg"] = samples_agg

    return report_dict, exit_code
