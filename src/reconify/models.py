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


class TabularConfig(BaseModel):
    type: Literal["tabular"]
    source: str
    target: str
    key: Annotated[list[str], Field(min_length=1)]
    ignore_columns: list[str] = Field(default_factory=list)
    tolerance: dict[str, float] = Field(default_factory=dict)
    string_rules: dict[str, list[StringRule]] = Field(default_factory=dict)
    normalization: dict[str, list[NormStep]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_normalization_no_generated_refs(self) -> TabularConfig:
        """Args in normalization steps must not reference generated columns."""
        generated = set(self.normalization.keys())
        if not generated:
            return self
        for col, steps in self.normalization.items():
            for step in steps:
                for arg in step.args:
                    if isinstance(arg, str) and arg in generated:
                        raise ValueError(
                            f"normalization[{col!r}] step {step.op.value} "
                            f"references generated column {arg!r} in args"
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
    total_rows_source: int = 0
    total_rows_target: int = 0
    matched_rows: int = 0
    missing_in_source: int = 0
    missing_in_target: int = 0
    different_rows: int = 0
    comparison_time_seconds: float = 0.0


class TabularDetails(BaseModel):
    column_stats: dict[str, Any] = Field(default_factory=dict)


class TextRulesApplied(BaseModel):
    drop_lines_count: int = 0
    replace_rules_count: int = 0


class TextSummary(BaseModel):
    total_lines_source: int = 0
    total_lines_target: int = 0
    different_lines: int = 0
    comparison_time_seconds: float = 0.0


class TextDetails(BaseModel):
    mode: str = TextMode.line_by_line.value
    rules_applied: TextRulesApplied = Field(default_factory=TextRulesApplied)


class ReconReport(BaseModel):
    type: Literal["tabular", "text"]
    version: str = "1.0"
    generated_at: str
    config_hash: str
    summary: TabularSummary | TextSummary
    details: TabularDetails | TextDetails
    samples: list[Any] = Field(default_factory=list)
