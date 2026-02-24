"""Pydantic v2 models for Reconify V1 config and report schemas."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# Shared enums
# ---------------------------------------------------------------------------


class StringRule(StrEnum):
    case_insensitive = "case_insensitive"
    trim = "trim"
    contains = "contains"
    regex_extract = "regex_extract"


class NormOp(StrEnum):
    map = "map"
    concat = "concat"
    substr = "substr"
    add = "add"
    sub = "sub"
    mul = "mul"
    div = "div"
    coalesce = "coalesce"
    date_format = "date_format"
    upper = "upper"
    lower = "lower"
    trim = "trim"
    round = "round"


# ---------------------------------------------------------------------------
# Tabular config
# ---------------------------------------------------------------------------


class NormStep(BaseModel):
    op: NormOp
    args: list[Any] = Field(default_factory=list)


class TabularCompare(BaseModel):
    include_columns: list[str] | None = None
    exclude_columns: list[str] | None = None
    trim_whitespace: bool = True
    case_insensitive: bool = False
    normalize_nulls: list[str] = Field(default_factory=lambda: ["", "NULL", "null"])


class TabularFilters(BaseModel):
    exclude_keys: list[dict[str, Any]] = Field(default_factory=list)


class TabularCsvOptions(BaseModel):
    delimiter: str = ","
    header: bool = True
    encoding: str = "utf-8"


class TabularSampling(BaseModel):
    sample_limit: int = 200
    sample_limit_per_type: int | None = None


class TabularOutput(BaseModel):
    include_row_samples: bool = True
    include_column_stats: bool = True


class TabularConfig(BaseModel):
    type: Literal["tabular"]
    format: Literal["csv"] = "csv"
    source: str
    target: str
    keys: Annotated[list[str], Field(min_length=1)]
    compare: TabularCompare = Field(default_factory=TabularCompare)
    filters: TabularFilters = Field(default_factory=TabularFilters)
    csv: TabularCsvOptions = Field(default_factory=TabularCsvOptions)
    sampling: TabularSampling = Field(default_factory=TabularSampling)
    output: TabularOutput = Field(default_factory=TabularOutput)

    @model_validator(mode="after")
    def _check_exclude_keys(self) -> TabularConfig:
        """Every exclude_keys entry must contain exactly all key columns."""
        key_set = set(self.keys)
        for i, entry in enumerate(self.filters.exclude_keys):
            entry_keys = set(entry.keys())
            if entry_keys != key_set:
                raise ValueError(
                    f"filters.exclude_keys[{i}] has keys {sorted(entry_keys)} "
                    f"but expected exactly {sorted(key_set)}"
                )
        return self


# ---------------------------------------------------------------------------
# Text config
# ---------------------------------------------------------------------------


class TextMode(StrEnum):
    line_by_line = "line_by_line"
    unordered_lines = "unordered_lines"


class TextNormalize(BaseModel):
    ignore_blank_lines: bool = False
    trim_lines: bool = False
    collapse_whitespace: bool = False
    case_insensitive: bool = False
    normalize_newlines: bool = True


class ReplaceRegex(BaseModel):
    pattern: str
    replace: str


class TextConfig(BaseModel):
    type: Literal["text"]
    source: str
    target: str
    mode: TextMode = TextMode.line_by_line
    normalize: TextNormalize = Field(default_factory=TextNormalize)
    drop_lines_regex: list[str] = Field(default_factory=list)
    replace_regex: list[ReplaceRegex] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Discriminated union
# ---------------------------------------------------------------------------

ReconConfig = Annotated[TabularConfig | TextConfig, Field(discriminator="type")]


# ---------------------------------------------------------------------------
# Report schemas
# ---------------------------------------------------------------------------


class TabularSummary(BaseModel):
    source_rows: int = 0
    target_rows: int = 0
    missing_in_target: int = 0
    missing_in_source: int = 0
    rows_with_mismatches: int = 0
    mismatched_cells: int = 0
    comparison_time_seconds: float = 0.0


class TabularFiltersApplied(BaseModel):
    exclude_keys_count: int = 0
    source_excluded_rows: int = 0
    target_excluded_rows: int = 0


class TabularDetails(BaseModel):
    format: str = "csv"
    keys: list[str] = Field(default_factory=list)
    compared_columns: list[str] = Field(default_factory=list)
    filters_applied: TabularFiltersApplied = Field(default_factory=TabularFiltersApplied)
    column_stats: dict[str, Any] = Field(default_factory=dict)


class TextRulesApplied(BaseModel):
    drop_lines_count: int = 0
    replace_rules_count: int = 0


class UnorderedStats(BaseModel):
    source_only_lines: int = 0
    target_only_lines: int = 0
    distinct_mismatched_lines: int = 0


class TextSummary(BaseModel):
    total_lines_source: int = 0
    total_lines_target: int = 0
    different_lines: int = 0
    comparison_time_seconds: float = 0.0


class TextDetails(BaseModel):
    mode: str = TextMode.line_by_line.value
    rules_applied: TextRulesApplied = Field(default_factory=TextRulesApplied)
    unordered_stats: UnorderedStats | None = None


class ReconError(BaseModel):
    code: str
    message: str
    details: str


class ReconReport(BaseModel):
    type: Literal["tabular", "text"]
    version: str = "1.1"
    generated_at: str
    config_hash: str
    summary: TabularSummary | TextSummary
    details: TabularDetails | TextDetails
    samples: list[Any] | dict[str, Any] = Field(default_factory=list)
    samples_agg: list[Any] | None = None
    error: ReconError | None = None
