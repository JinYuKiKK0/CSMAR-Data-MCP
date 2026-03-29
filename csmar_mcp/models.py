from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _generate_download_id() -> str:
    return f"download_{uuid4().hex[:8]}"


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


def _validate_date(value: str | None) -> str | None:
    if value is None:
        return value
    if not _DATE_RE.match(value):
        raise ValueError("date must use YYYY-MM-DD format")
    return value


class CatalogSearchInput(StrictModel):
    query: str = Field(
        ...,
        min_length=1,
        description="Search text for a business topic, table code, or table name.",
    )
    database_name: str | None = Field(
        default=None,
        min_length=1,
        description="Optional purchased database name used to narrow the search.",
    )
    limit: int = Field(default=10, ge=1, le=20, description="Maximum number of tables to return.")


class ListDatabasesOutput(StrictModel):
    databases: list[str] = Field(..., description="Purchased database names.")


class ListTablesInput(StrictModel):
    database_name: str = Field(..., min_length=1, description="Purchased database name.")


class TableListItem(StrictModel):
    table_code: str = Field(..., description="Table code used in later tool calls.")
    table_name: str = Field(..., description="Human-readable table name.")


class ListTablesOutput(StrictModel):
    database_name: str = Field(..., description="Purchased database name.")
    items: list[TableListItem] = Field(..., description="Tables in the selected database.")


class CatalogItem(StrictModel):
    table_code: str = Field(..., description="Table code used in later tool calls.")
    table_name: str = Field(..., description="Human-readable table name.")
    database_name: str = Field(..., description="Purchased database that contains the table.")
    why_matched: str = Field(..., description="Short reason why this table matches the query.")


class CatalogSearchOutput(StrictModel):
    items: list[CatalogItem] = Field(..., description="Matching table candidates.")


class GetTableSchemaInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code returned by catalog search.")
    field_query: str | None = Field(
        default=None,
        min_length=1,
        description="Optional substring used to filter the returned field list.",
    )
    preview_columns: list[str] | None = Field(
        default=None,
        description="Columns to include in preview rows. Required when preview_rows > 0.",
    )
    preview_rows: int = Field(default=0, ge=0, le=5, description="Number of preview rows to return.")

    @field_validator("preview_columns")
    @classmethod
    def validate_preview_columns(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        return _clean_columns(value)

    @model_validator(mode="after")
    def validate_preview_request(self) -> "GetTableSchemaInput":
        if self.preview_rows > 0 and not self.preview_columns:
            raise ValueError("preview_columns is required when preview_rows > 0")
        return self


class TableSchemaOutput(StrictModel):
    table_code: str = Field(..., description="Table code.")
    field_count: int = Field(..., description="Number of fields returned in this response.")
    fields: list[str] = Field(..., description="Filtered field list.")
    preview_rows: list[dict[str, Any]] | None = Field(
        default=None,
        description="Optional preview rows limited to preview_columns.",
    )


class QueryValidateInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code.")
    columns: list[str] = Field(..., min_length=1, description="Columns to validate and sample.")
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
    def validate_date_range(self) -> "QueryValidateInput":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class QueryValidateOutput(StrictModel):
    table_code: str = Field(..., description="Table code.")
    row_count: int = Field(..., description="Number of matching rows.")
    sample_rows: list[dict[str, Any]] | None = Field(
        default=None,
        description="Optional sample rows limited to requested columns.",
    )
    can_download: bool = Field(..., description="Whether download can proceed with the same arguments.")


class DownloadMaterializeInput(StrictModel):
    table_code: str = Field(..., min_length=1, description="Table code.")
    columns: list[str] = Field(..., min_length=1, description="Columns to download.")
    output_dir: str = Field(..., min_length=1, description="Directory used for ZIP and extracted files.")
    condition: str | None = Field(
        default=None,
        description="CSMAR native condition string. Omit to download the whole table.",
    )
    start_date: str | None = Field(default=None, description="Start date in YYYY-MM-DD format.")
    end_date: str | None = Field(default=None, description="End date in YYYY-MM-DD format.")
    download_id: str = Field(
        default_factory=_generate_download_id,
        min_length=1,
        description="Stable download identifier used by the manifest resource.",
    )

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        return _clean_columns(value)

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, value: str | None) -> str | None:
        return _validate_date(value)

    @model_validator(mode="after")
    def validate_date_range(self) -> "DownloadMaterializeInput":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class DownloadMaterializeOutput(StrictModel):
    download_id: str = Field(..., description="Download identifier.")
    table_code: str = Field(..., description="Table code.")
    zip_path: str = Field(..., description="Absolute ZIP file path.")
    extract_dir: str = Field(..., description="Absolute extraction directory path.")
    file_count: int = Field(..., description="Number of extracted files.")


class DownloadManifest(StrictModel):
    download_id: str = Field(..., description="Download identifier.")
    table_code: str = Field(..., description="Table code.")
    zip_path: str = Field(..., description="Absolute ZIP file path.")
    extract_dir: str = Field(..., description="Absolute extraction directory path.")
    files: list[str] = Field(..., description="Absolute extracted file paths.")


class ToolError(StrictModel):
    code: str = Field(..., description="Stable machine-readable error code.")
    message: str = Field(..., description="Short human-readable error message.")
    hint: str = Field(..., description="Concrete next step for the caller.")
    retry_after_seconds: int | None = Field(
        default=None,
        description="Retry delay in seconds when the error is rate-limit related.",
    )
    candidate_values: list[str] | None = Field(
        default=None,
        description="Suggested replacement values such as similar fields or tables.",
    )
    suggested_args_patch: dict[str, Any] | None = Field(
        default=None,
        description="Minimal argument patch that the caller can apply before retrying.",
    )
