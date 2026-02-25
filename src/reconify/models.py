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


class RegexExtractParams(BaseModel):
    pattern: str = Field(min_length=1)
    group: int = Field(default=1, ge=0)


class RegexExtractRule(BaseModel):
    regex_extract: RegexExtractParams


SimpleStringRule = Literal["trim", "case_insensitive", "contains"]
StringRuleItem = SimpleStringRule | RegexExtractRule


class TabularCompare(BaseModel):
    include_columns: list[str] | None = None
    exclude_columns: list[str] | None = None
    trim_whitespace: bool = True
    case_insensitive: bool = False
    normalize_nulls: list[str] = Field(default_factory=lambda: ["", "NULL", "null"])


class RowFilterOp(StrEnum):
    equals = "equals"
    not_equals = "not_equals"
    in_ = "in"
    contains = "contains"
    regex = "regex"
    is_null = "is_null"
    not_null = "not_null"


class RowFilterRule(BaseModel):
    column: str
    op: RowFilterOp
    value: str | int | float | bool | None = None
    values: list[str | int | float | bool] | None = None
    pattern: str | None = None
    case_insensitive: bool | None = None
    trim_whitespace: bool | None = None

    @model_validator(mode="after")
    def _check_op_params(self) -> RowFilterRule:
        op = self.op
        if op in (RowFilterOp.equals, RowFilterOp.not_equals, RowFilterOp.contains):
            if self.value is None:
                raise ValueError(f"op={op.value!r} requires 'value' to be set")
        elif op == RowFilterOp.in_:
            if not self.values:
                raise ValueError("op='in' requires 'values' to be a non-empty list")
        elif op == RowFilterOp.regex:
            if not self.pattern:
                raise ValueError("op='regex' requires 'pattern' to be a non-empty string")
        elif op in (RowFilterOp.is_null, RowFilterOp.not_null) and (
            self.value is not None or self.values is not None or self.pattern is not None
        ):
            raise ValueError(f"op={op.value!r} must not have 'value', 'values', or 'pattern'")
        return self


class RowFiltersConfig(BaseModel):
    apply_to: Literal["both", "source", "target"] = "both"
    mode: Literal["exclude", "include"] = "exclude"
    rules: list[RowFilterRule] = Field(default_factory=list)


class TabularFilters(BaseModel):
    exclude_keys: list[dict[str, Any]] = Field(default_factory=list)
    row_filters: RowFiltersConfig | None = None


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
    ignore_columns: list[str] = Field(default_factory=list)
    tolerance: dict[str, float] = Field(default_factory=dict)
    string_rules: dict[str, list[StringRuleItem]] = Field(default_factory=dict)
    normalization: dict[str, list[NormStep]] = Field(default_factory=dict)

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

    @model_validator(mode="after")
    def _check_tolerance(self) -> TabularConfig:
        for col, val in self.tolerance.items():
            if val < 0:
                raise ValueError(f"tolerance[{col!r}]: value must be >= 0, got {val}")
        return self

    @model_validator(mode="after")
    def _check_normalization(self) -> TabularConfig:
        generated = set(self.normalization.keys())
        for col_name, pipeline in self.normalization.items():
            if not pipeline:
                raise ValueError(f"normalization[{col_name!r}]: pipeline must not be empty")
            for step in pipeline:
                for arg in step.args:
                    if isinstance(arg, str) and arg in generated:
                        raise ValueError(
                            f"normalization[{col_name!r}]: arg {arg!r} references "
                            f"a generated column"
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


class RowFiltersInfo(BaseModel):
    count: int
    apply_to: str
    mode: str


class TabularFiltersApplied(BaseModel):
    exclude_keys_count: int = 0
    source_excluded_rows: int = 0
    target_excluded_rows: int = 0
    source_excluded_rows_exclude_keys: int = 0
    target_excluded_rows_exclude_keys: int = 0
    source_excluded_rows_row_filters: int = 0
    target_excluded_rows_row_filters: int = 0
    row_filters: RowFiltersInfo | None = None


class TabularDetails(BaseModel):
    format: str = "csv"
    keys: list[str] = Field(default_factory=list)
    compared_columns: list[str] = Field(default_factory=list)
    read_rows_source: int = 0
    read_rows_target: int = 0
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
    read_lines_source: int = 0
    read_lines_target: int = 0
    ignored_blank_lines_source: int = 0
    ignored_blank_lines_target: int = 0
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
