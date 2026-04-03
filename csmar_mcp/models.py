from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _generate_validation_id() -> str:
    return f"validation_{uuid4().hex[:10]}"


def _generate_download_id() -> str:
    return f"download_{uuid4().hex[:10]}"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude_none=True,
            exclude_defaults=True,
            exclude_unset=True,
        )


def _clean_columns(value: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()

    for column in value:
        text = column.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)

    if not cleaned:
        raise ValueError("columns must contain at least one non-empty field name")

    return cleaned


def _clean_tags(value: list[str] | None) -> list[str] | None:
    if value is None:
        return None

    cleaned: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)

    return cleaned or None


def _validate_date(value: str | None) -> str | None:
    if value is None:
        return value
    if not _DATE_RE.match(value):
        raise ValueError("date must use YYYY-MM-DD format")
    return value


class ListDatabasesOutput(StrictModel):
    databases: list[str] = Field(..., description="Purchased database names.")


class ListTablesInput(StrictModel):
    database_name: str = Field(
        ...,
        min_length=1,
        description="Purchased database name copied verbatim from csmar_list_databases.",
    )


class TableListItem(StrictModel):
    table_code: str = Field(..., description="Table code used in later tool calls.")
    table_name: str = Field(..., description="Human-readable table name.")


class ListTablesOutput(StrictModel):
    items: list[TableListItem] = Field(..., description="Tables in the selected database.")


class SearchTablesInput(StrictModel):
    query: str = Field(
        ...,
        min_length=1,
        description="Search text for business topic, table code, or table name.",
    )
    database_name: str | None = Field(
        default=None,
        min_length=1,
        description="Optional purchased database name copied verbatim from csmar_list_databases.",
    )
    limit: int = Field(default=5, ge=1, le=5, description="Maximum number of tables to return (hard cap: 5).")


class SearchTableItem(StrictModel):
    table_code: str = Field(..., description="Table code used in later tool calls.")
    table_name: str = Field(..., description="Human-readable table name.")
    database_name: str = Field(..., description="Purchased database that contains the table.")
    why_matched: str = Field(..., description="Short reason why this table matches the query.")
    score: float = Field(..., ge=0.0, description="Relevance score in descending rank order.")


class SearchTablesOutput(StrictModel):
    items: list[SearchTableItem] = Field(..., description="Matching table candidates.")


class SearchFieldsInput(StrictModel):
    query: str = Field(
        ...,
        min_length=1,
        description="Field lookup text for deterministic matching against field_name, label, and description.",
    )
    database_name: str | None = Field(
        default=None,
        min_length=1,
        description="Optional database filter copied from csmar_list_databases.",
    )
    table_code: str | None = Field(
        default=None,
        min_length=1,
        description="Optional table filter copied from csmar_search_tables or csmar_list_tables.",
    )
    role_hint: str | None = Field(default=None, min_length=1, description="Optional role hint for ranking bias.")
    frequency_hint: str | None = Field(
        default=None,
        min_length=1,
        description="Optional frequency hint for ranking bias.",
    )
    limit: int = Field(default=20, ge=1, le=50, description="Maximum number of fields to return.")


class FieldSchemaItem(StrictModel):
    field_name: str = Field(..., description="Field code used in columns and conditions.")
    field_label: str | None = Field(default=None, description="Human-readable field label.")
    field_description: str | None = Field(default=None, description="Optional field description.")
    data_type: str | None = Field(default=None, description="Optional field data type.")
    frequency_tags: list[str] | None = Field(default=None, description="Optional frequency tags.")
    role_tags: list[str] | None = Field(default=None, description="Optional role tags.")

    @field_validator("frequency_tags", "role_tags")
    @classmethod
    def validate_tags(cls, value: list[str] | None) -> list[str] | None:
        return _clean_tags(value)


class SearchFieldItem(FieldSchemaItem):
    table_code: str = Field(..., description="Table code that contains this field.")
    table_name: str = Field(..., description="Human-readable table name.")
    database_name: str = Field(..., description="Purchased database that contains this field.")
    why_matched: str = Field(
        ...,
        description="Short deterministic reason for match (exact/contains/similar + optional hint bias).",
    )
    score: float = Field(..., ge=0.0, description="Relevance score in descending rank order.")


class SearchFieldsOutput(StrictModel):
    items: list[SearchFieldItem] = Field(..., description="Matching field candidates.")


class GetTableSchemaInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code returned by search or list tools.")


class GetTableSchemaOutput(StrictModel):
    table_code: str = Field(..., description="Table code.")
    fields: list[FieldSchemaItem] = Field(..., description="Schema fields.")


class ProbeQueryInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code.")
    columns: list[str] = Field(..., min_length=1, description="Columns to probe.")
    condition: str | None = Field(
        default=None,
        description="CSMAR native condition string. Omit to query the whole table.",
    )
    start_date: str | None = Field(default=None, description="Start date in YYYY-MM-DD format.")
    end_date: str | None = Field(default=None, description="End date in YYYY-MM-DD format.")
    sample_rows: int = Field(default=3, ge=0, le=5, description="Maximum sample rows to return.")

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        return _clean_columns(value)

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, value: str | None) -> str | None:
        return _validate_date(value)

    @model_validator(mode="after")
    def validate_date_range(self) -> "ProbeQueryInput":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ProbeQueryOutput(StrictModel):
    validation_id: str = Field(..., description="Stable identifier used by csmar_materialize_query.")
    query_fingerprint: str = Field(..., description="Stable hash for this logical query.")
    row_count: int = Field(..., ge=0, description="Number of rows matching this query.")
    sample_rows: list[dict[str, Any]] | None = Field(
        default=None,
        description="Optional sample rows limited to requested columns.",
    )
    invalid_columns: list[str] | None = Field(
        default=None,
        description="Columns not found in the table schema.",
    )
    can_materialize: bool = Field(..., description="Whether this validation can be materialized safely.")

    @field_validator("invalid_columns")
    @classmethod
    def validate_invalid_columns(cls, value: list[str] | None) -> list[str] | None:
        return _clean_tags(value)


class MaterializeQueryInput(StrictModel):
    validation_id: str = Field(..., min_length=1, description="Validation id returned by csmar_probe_query.")
    output_dir: str = Field(..., min_length=1, description="Directory where ZIP and extracted files are written.")


class MaterializeAudit(StrictModel):
    retries: int = Field(..., ge=0, description="Number of retries used during materialization.")
    packaged_at: str = Field(..., description="UTC timestamp when package status became ready.")
    completed_at: str = Field(..., description="UTC timestamp when local files were extracted.")


class MaterializeQueryOutput(StrictModel):
    download_id: str = Field(default_factory=_generate_download_id, description="Download identifier.")
    query_fingerprint: str = Field(..., description="Fingerprint copied from probe output.")
    output_dir: str = Field(..., description="Absolute output directory used for this materialization.")
    files: list[str] = Field(..., description="Absolute extracted file paths.")
    row_count: int = Field(..., ge=0, description="Row count copied from the probe stage.")
    archive_path: str = Field(..., description="Absolute ZIP archive path.")
    audit: MaterializeAudit = Field(..., description="Execution audit metadata.")


class ToolError(StrictModel):
    code: str = Field(..., description="Stable machine-readable error code.")
    message: str = Field(..., description="Short human-readable error message.")
    hint: str = Field(..., description="Concrete next step for the caller.")
    retry_after_seconds: int | None = Field(
        default=None,
        description="Retry delay in seconds when the error is rate-limit related.",
    )
    suggested_args_patch: dict[str, Any] | None = Field(
        default=None,
        description="Minimal argument patch that the caller can apply before retrying.",
    )
