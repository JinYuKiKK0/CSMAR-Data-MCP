from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .core.errors import CsmarError
from .core.types import (
    CatalogRecord,
    FieldMatch,
    FieldSchemaRecord,
    MaterializationResult,
    ProbeResult,
    ProbeSpec,
    ValidationRecord,
)
from .infra import CsmarGateway, PersistentState
from .models import (
    FieldSchemaItem,
    GetTableSchemaOutput,
    MaterializeAudit,
    MaterializeQueryOutput,
    ProbeQueryInput,
    ProbeQueryOutput,
    SearchFieldItem,
    SearchTableItem,
)
from .services import MetadataService, QueryService


CsmarMcpError = CsmarError


class CsmarClient:
    def __init__(
        self,
        account: str,
        password: str,
        lang: str = "0",
        belong: str = "0",
        poll_interval_seconds: int = 3,
        poll_timeout_seconds: int = 900,
        cache_ttl_minutes: int = 30,
        state_dir: str | Path | None = None,
    ) -> None:
        resolved_state_dir = Path(state_dir) if state_dir is not None else self._default_state_dir()
        self._state = PersistentState(cache_ttl_minutes=cache_ttl_minutes, state_dir=resolved_state_dir)
        self._gateway = CsmarGateway(
            account=account,
            password=password,
            lang=lang,
            belong=belong,
            poll_interval_seconds=poll_interval_seconds,
            poll_timeout_seconds=poll_timeout_seconds,
        )
        self._metadata = MetadataService(self._gateway, self._state)
        self._query = QueryService(self._gateway, self._metadata, self._state)

    def _default_state_dir(self) -> Path:
        return (Path.cwd() / ".stata_agent" / "csmar_mcp").resolve()

    def has_cached_entry(self, namespace: str, key: str) -> bool:
        return self._state.has_cached(namespace, key)

    def has_cached_probe(self, cache_key: str) -> bool:
        return self._state.has_cached("probes", cache_key)

    def has_cached_download(self, query_fingerprint: str, output_dir: str) -> bool:
        cache_key = self._query.build_materialize_cache_key(
            query_fingerprint=query_fingerprint,
            output_dir=output_dir,
        )
        return self._state.has_cached("downloads", cache_key)

    def log_tool_trace(
        self,
        *,
        tool_name: str,
        request_payload: dict[str, Any],
        result_summary: dict[str, Any] | None,
        error: dict[str, Any] | None,
        query_fingerprint: str | None,
        validation_id: str | None,
        cached: bool,
        started_at: datetime,
        completed_at: datetime,
        upstream_code: int | None = None,
        raw_message: str | None = None,
    ) -> str:
        trace_id = f"trace_{uuid4().hex[:10]}"
        self._state.add_tool_trace(
            trace_id=trace_id,
            tool_name=tool_name,
            request_payload=request_payload,
            result_summary=result_summary,
            error=error,
            query_fingerprint=query_fingerprint,
            validation_id=validation_id,
            cached=cached,
            started_at=started_at,
            completed_at=completed_at,
            upstream_code=upstream_code,
            raw_message=raw_message,
        )
        return trace_id

    def get_tool_trace(self, trace_id: str) -> dict[str, Any] | None:
        return self._state.get_tool_trace(trace_id)

    def list_databases(self) -> list[str]:
        return self._metadata.list_databases()

    def list_tables(self, database_name: str) -> list[CatalogRecord]:
        return self._metadata.list_tables(database_name)

    def search_tables(self, query: str, database_name: str | None = None, limit: int = 5) -> list[SearchTableItem]:
        return [
            SearchTableItem(
                table_code=item.table_code,
                table_name=item.table_name,
                database_name=item.database_name,
                why_matched=item.why_matched,
                score=item.score,
            )
            for item in self._metadata.search_tables(query, database_name=database_name, limit=limit)
        ]

    def read_table_schema(self, table_code: str) -> GetTableSchemaOutput:
        return GetTableSchemaOutput(
            table_code=table_code,
            fields=[self._to_field_schema_item(item) for item in self._metadata.read_table_schema(table_code)],
        )

    def search_fields(
        self,
        query: str,
        database_name: str | None = None,
        table_code: str | None = None,
        role_hint: str | None = None,
        frequency_hint: str | None = None,
        limit: int = 20,
    ) -> list[SearchFieldItem]:
        return [
            self._to_search_field_item(item)
            for item in self._metadata.search_fields(
                query=query,
                database_name=database_name,
                table_code=table_code,
                role_hint=role_hint,
                frequency_hint=frequency_hint,
                limit=limit,
            )
        ]

    def build_cache_key(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> str:
        return self._query.build_cache_key(
            table_code=table_code,
            columns=columns,
            condition=condition,
            start_date=start_date,
            end_date=end_date,
        )

    def build_query_fingerprint(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> str:
        return self._query.build_query_fingerprint(
            table_code=table_code,
            columns=columns,
            condition=condition,
            start_date=start_date,
            end_date=end_date,
        )

    def get_cached_probe(self, cache_key: str) -> ProbeQueryOutput | None:
        result = self._query.get_cached_probe(cache_key)
        if result is None:
            return None
        return self._to_probe_query_output(result)

    def get_validation_record(self, validation_id: str) -> ValidationRecord | None:
        return self._query.get_validation_record(validation_id)

    def get_rate_limit_remaining_seconds(self, cache_key: str) -> int | None:
        return self._query.get_rate_limit_remaining_seconds(cache_key)

    def probe_query(self, request: ProbeQueryInput) -> ProbeQueryOutput:
        result = self._query.probe_query(
            ProbeSpec(
                table_code=request.table_code,
                columns=tuple(request.columns),
                condition=request.condition,
                start_date=request.start_date,
                end_date=request.end_date,
                sample_rows=request.sample_rows,
            )
        )
        return self._to_probe_query_output(result)

    def materialize_query(
        self,
        validation_id: str,
        output_dir: str,
        *,
        max_retries: int = 2,
    ) -> MaterializeQueryOutput:
        result = self._query.materialize_query(validation_id, output_dir, max_retries=max_retries)
        return self._to_materialize_query_output(result)

    def local_condition_error(self, condition: str | None) -> CsmarError | None:
        return self._query.local_condition_error(condition)

    def _to_field_schema_item(self, item: FieldSchemaRecord) -> FieldSchemaItem:
        return FieldSchemaItem(
            field_name=item.field_name,
            field_label=item.field_label,
            field_description=item.field_description,
            data_type=item.data_type,
            frequency_tags=list(item.frequency_tags) if item.frequency_tags else None,
            role_tags=list(item.role_tags) if item.role_tags else None,
        )

    def _to_search_field_item(self, item: FieldMatch) -> SearchFieldItem:
        return SearchFieldItem(
            field_name=item.field_name,
            field_label=item.field_label,
            field_description=item.field_description,
            data_type=item.data_type,
            frequency_tags=list(item.frequency_tags) if item.frequency_tags else None,
            role_tags=list(item.role_tags) if item.role_tags else None,
            table_code=item.table_code,
            table_name=item.table_name,
            database_name=item.database_name,
            why_matched=item.why_matched,
            score=item.score,
        )

    def _to_probe_query_output(self, result: ProbeResult) -> ProbeQueryOutput:
        return ProbeQueryOutput(
            validation_id=result.validation_id,
            query_fingerprint=result.query_fingerprint,
            row_count=result.row_count,
            sample_rows=list(result.sample_rows) if result.sample_rows else None,
            invalid_columns=list(result.invalid_columns) if result.invalid_columns else None,
            can_materialize=result.can_materialize,
        )

    def _to_materialize_query_output(self, result: MaterializationResult) -> MaterializeQueryOutput:
        return MaterializeQueryOutput(
            download_id=result.download_id,
            query_fingerprint=result.query_fingerprint,
            output_dir=result.output_dir,
            files=list(result.files),
            row_count=result.row_count,
            archive_path=result.archive_path,
            audit=MaterializeAudit(
                retries=result.audit.retries,
                packaged_at=self._to_iso_timestamp(result.audit.packaged_at),
                completed_at=self._to_iso_timestamp(result.audit.completed_at),
            ),
        )

    def _to_iso_timestamp(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
