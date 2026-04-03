from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    database_name: str
    table_code: str
    table_name: str


@dataclass(frozen=True, slots=True)
class FieldSchemaRecord:
    field_name: str
    field_label: str | None = None
    field_description: str | None = None
    data_type: str | None = None
    frequency_tags: tuple[str, ...] | None = None
    role_tags: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class TableMatch:
    table_code: str
    table_name: str
    database_name: str
    why_matched: str
    score: float


@dataclass(frozen=True, slots=True)
class FieldMatch:
    field_name: str
    field_label: str | None
    field_description: str | None
    data_type: str | None
    frequency_tags: tuple[str, ...] | None
    role_tags: tuple[str, ...] | None
    table_code: str
    table_name: str
    database_name: str
    why_matched: str
    score: float


@dataclass(frozen=True, slots=True)
class ProbeSpec:
    table_code: str
    columns: tuple[str, ...]
    condition: str | None
    start_date: str | None
    end_date: str | None
    sample_rows: int


@dataclass(frozen=True, slots=True)
class ProbeResult:
    validation_id: str
    query_fingerprint: str
    row_count: int
    sample_rows: tuple[dict[str, Any], ...] | None
    invalid_columns: tuple[str, ...] | None
    can_materialize: bool


@dataclass(frozen=True, slots=True)
class ValidationRecord:
    validation_id: str
    query_fingerprint: str
    table_code: str
    columns: tuple[str, ...]
    condition: str | None
    start_date: str | None
    end_date: str | None
    row_count: int
    can_materialize: bool


@dataclass(frozen=True, slots=True)
class MaterializeAuditRecord:
    retries: int
    packaged_at: datetime
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    download_id: str
    query_fingerprint: str
    output_dir: str
    files: tuple[str, ...]
    row_count: int
    archive_path: str
    audit: MaterializeAuditRecord


@dataclass(frozen=True, slots=True)
class CsmarToolTrace:
    trace_id: str
    tool_name: str
    request_payload: dict[str, Any]
    result_summary: dict[str, Any] | None
    error: dict[str, Any] | None
    query_fingerprint: str | None
    validation_id: str | None
    cached: bool
    started_at: datetime
    completed_at: datetime
