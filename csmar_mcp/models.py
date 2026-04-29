from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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
    start_time: str | None = Field(
        default=None, description="Earliest data date upstream advertises for this table."
    )
    end_time: str | None = Field(
        default=None, description="Latest data date upstream advertises for this table."
    )


class ListTablesOutput(StrictModel):
    items: list[TableListItem] = Field(..., description="Tables in the selected database.")


class FieldSchemaItem(StrictModel):
    field_code: str = Field(..., description="Field code used in columns and conditions.")
    field_label: str | None = Field(default=None, description="Human-readable field label.")
    data_type: str | None = Field(
        default=None, description="Upstream-declared data type, e.g. varchar / decimal."
    )
    field_key: str | None = Field(
        default=None, description="Upstream role key, e.g. Code (primary) / Date (temporal)."
    )
    nullable: bool | None = Field(
        default=None, description="Whether upstream marks this field nullable."
    )


class GetTableSchemaInput(StrictModel):
    table_code: str = Field(
        ..., min_length=1, description="Table code returned by search or list tools."
    )


class GetTableSchemaOutput(StrictModel):
    table_code: str = Field(..., description="Table code.")
    fields: list[FieldSchemaItem] = Field(..., description="Schema fields.")


class BulkSchemaInput(StrictModel):
    table_codes: list[str] = Field(
        ...,
        min_length=1,
        max_length=20,
        description="Table codes to fetch. Cache-first; only missing entries hit CSMAR.",
    )

    @field_validator("table_codes")
    @classmethod
    def validate_codes(cls, value: list[str]) -> list[str]:
        return _clean_columns(value)


class BulkSchemaItem(StrictModel):
    table_code: str = Field(..., description="Table code.")
    fields: list[FieldSchemaItem] | None = Field(
        default=None, description="Schema fields when available."
    )
    error: ToolErrorPayload | None = Field(
        default=None, description="Per-table error when fetch failed."
    )


class BulkSchemaOutput(StrictModel):
    items: list[BulkSchemaItem] = Field(..., description="Per-table results.")


class RefreshCacheInput(StrictModel):
    namespace: str = Field(
        ...,
        description="databases | tables | schema | all.",
    )
    key: str | None = Field(
        default=None,
        description="Optional specific cache key (e.g., table_code). Omit to clear the whole namespace.",
    )

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        allowed = {"databases", "tables", "schema", "all"}
        if value not in allowed:
            raise ValueError(f"namespace must be one of {sorted(allowed)}")
        return value


class RefreshCacheOutput(StrictModel):
    cleared: dict[str, int] = Field(..., description="Number of entries cleared per namespace.")


class ToolErrorPayload(StrictModel):
    code: str = Field(..., description="Stable error code.")
    message: str = Field(..., description="Short human-readable message.")
    hint: str | None = Field(default=None, description="Suggested next step.")


class ProbeQueryInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code.")
    columns: list[str] = Field(..., min_length=1, description="Columns to probe.")
    condition: str | None = Field(
        default=None,
        description="CSMAR native condition string. Omit to query the whole table.",
    )
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format. Required.")
    end_date: str = Field(..., description="End date in YYYY-MM-DD format. Required.")
    sample_rows: int = Field(default=3, ge=0, le=5, description="Maximum sample rows to return.")

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        return _clean_columns(value)

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, value: str) -> str:
        validated = _validate_date(value)
        assert validated is not None
        return validated

    @model_validator(mode="after")
    def validate_date_range(self) -> ProbeQueryInput:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ProbeQueryOutput(StrictModel):
    validation_id: str = Field(
        ..., description="Stable identifier used by csmar_materialize_query."
    )
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
    can_materialize: bool = Field(
        ..., description="Whether this validation can be materialized safely."
    )

    @field_validator("invalid_columns")
    @classmethod
    def validate_invalid_columns(cls, value: list[str] | None) -> list[str] | None:
        return _clean_tags(value)


class MaterializeQueryInput(StrictModel):
    validation_id: str = Field(
        ..., min_length=1, description="Validation id returned by csmar_probe_query."
    )
    output_dir: str = Field(
        ..., min_length=1, description="Directory where ZIP and extracted files are written."
    )


class MaterializeAudit(StrictModel):
    retries: int = Field(..., ge=0, description="Number of retries used during materialization.")
    packaged_at: str = Field(..., description="UTC timestamp when package status became ready.")
    completed_at: str = Field(..., description="UTC timestamp when local files were extracted.")


class MaterializeQueryOutput(StrictModel):
    download_id: str = Field(
        default_factory=_generate_download_id, description="Download identifier."
    )
    query_fingerprint: str = Field(..., description="Fingerprint copied from probe output.")
    output_dir: str = Field(
        ..., description="Absolute output directory used for this materialization."
    )
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
