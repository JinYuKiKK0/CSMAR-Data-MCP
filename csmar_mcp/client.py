from __future__ import annotations

import hashlib
import json
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher, get_close_matches
from pathlib import Path
from typing import Any, Callable
from urllib import parse
from uuid import uuid4

import urllib3

from csmarapi.CsmarService import CsmarService

from .models import (
    FieldSchemaItem,
    GetTableSchemaOutput,
    MaterializeAudit,
    MaterializeQueryOutput,
    ProbeQueryInput,
    ProbeQueryOutput,
    SearchFieldItem,
    SearchTableItem,
    ToolError,
)


@dataclass(slots=True)
class CacheEntry:
    created_at: datetime
    value: Any


class CsmarMcpError(Exception):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        hint: str | None = None,
        upstream_code: int | None = None,
        retry_after_seconds: int | None = None,
        candidate_values: list[str] | None = None,
        suggested_args_patch: dict[str, Any] | None = None,
        raw_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.hint = hint
        self.upstream_code = upstream_code
        self.retry_after_seconds = retry_after_seconds
        self.candidate_values = candidate_values
        self.suggested_args_patch = suggested_args_patch
        self.raw_message = raw_message

    def to_tool_error(self) -> ToolError:
        return ToolError(
            code=self.error_code,
            message=self.message,
            hint=self.hint or "Review the arguments and retry.",
            retry_after_seconds=self.retry_after_seconds,
            candidate_values=self.candidate_values,
            suggested_args_patch=self.suggested_args_patch,
        )


@dataclass(frozen=True, slots=True)
class CatalogRecord:
    database_name: str
    table_code: str
    table_name: str


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
    ) -> None:
        self._account = account
        self._password = password
        self._lang = lang if lang in {"0", "1", "2"} else "0"
        self._belong = belong if belong in {"0", "1"} else "0"

        self._service = CsmarService()
        self._http = urllib3.PoolManager()
        self._lock = threading.RLock()

        self._catalog_cache: dict[str, CacheEntry] = {}
        self._schema_cache: dict[str, CacheEntry] = {}
        self._probe_cache: dict[str, CacheEntry] = {}
        self._validation_registry: dict[str, CacheEntry] = {}
        self._download_cache: dict[str, CacheEntry] = {}
        self._rate_limit_cooldowns: dict[str, datetime] = {}

        self._cache_ttl = timedelta(minutes=max(1, cache_ttl_minutes))
        self._poll_interval_seconds = max(1, poll_interval_seconds)
        self._poll_timeout_seconds = max(30, poll_timeout_seconds)
        self._logged_in = False

    def list_databases(self) -> list[str]:
        response = self._get(self._service.urlUtil.getListDbsUrl(), include_belong=True)
        return self._deduplicate(
            self._normalize_name_list(response.get("data"), dict_name_keys=("dbName", "databaseName", "name", "value"))
        )

    def list_tables(self, database_name: str) -> list[CatalogRecord]:
        cache_key = f"tables::{database_name.strip()}"

        with self._lock:
            cached = self._get_cache_value(self._catalog_cache, cache_key)
            if cached is not None:
                return list(cached)

        encoded_database_name = parse.quote(database_name)
        endpoint = f"{self._service.urlUtil.getListTablesUrl()}?dbName={encoded_database_name}"
        response = self._get(endpoint, include_belong=True)
        table_records = self._normalize_table_list(database_name, response.get("data", []))

        with self._lock:
            self._catalog_cache[cache_key] = CacheEntry(created_at=self._now(), value=list(table_records))

        return table_records

    def search_tables(self, query: str, database_name: str | None = None, limit: int = 10) -> list[SearchTableItem]:
        normalized_query = query.strip().lower()
        databases = [database_name] if database_name else self.list_databases()
        matches: list[SearchTableItem] = []

        for db_name in databases:
            for record in self.list_tables(db_name):
                scored = self._score_table_match(record, normalized_query)
                if scored is None:
                    continue
                score, why_matched = scored
                matches.append(
                    SearchTableItem(
                        table_code=record.table_code,
                        table_name=record.table_name,
                        database_name=record.database_name,
                        why_matched=why_matched,
                        score=score,
                    )
                )

        matches.sort(key=lambda item: (-item.score, item.table_code))
        return matches[:limit]

    def list_field_schema_items(self, table_code: str) -> list[FieldSchemaItem]:
        cache_key = f"schema::{table_code.strip()}"
        with self._lock:
            cached = self._get_cache_value(self._schema_cache, cache_key)
            if cached is not None:
                return [item.model_copy(deep=True) for item in cached]

        encoded_table_name = parse.quote(table_code)
        endpoint = f"{self._service.urlUtil.getListFieldsUrl()}?table={encoded_table_name}"
        response = self._get(endpoint, include_belong=True)
        fields = self._normalize_field_schema_list(response.get("data"))

        with self._lock:
            self._schema_cache[cache_key] = CacheEntry(
                created_at=self._now(),
                value=[item.model_copy(deep=True) for item in fields],
            )

        return fields

    def read_table_schema(self, table_code: str) -> GetTableSchemaOutput:
        fields = self.list_field_schema_items(table_code)
        return GetTableSchemaOutput(table_code=table_code, fields=fields)

    def search_fields(
        self,
        query: str,
        database_name: str | None = None,
        table_code: str | None = None,
        role_hint: str | None = None,
        frequency_hint: str | None = None,
        limit: int = 20,
    ) -> list[SearchFieldItem]:
        table_candidates = self._resolve_table_candidates(database_name=database_name, table_code=table_code)
        normalized_query = query.strip().lower()

        matches: list[SearchFieldItem] = []
        for table in table_candidates:
            schema_fields = self.list_field_schema_items(table.table_code)
            for field in schema_fields:
                scored = self._score_field_match(field, normalized_query, role_hint, frequency_hint)
                if scored is None:
                    continue

                score, why_matched = scored
                matches.append(
                    SearchFieldItem(
                        field_name=field.field_name,
                        field_label=field.field_label,
                        field_description=field.field_description,
                        data_type=field.data_type,
                        frequency_tags=field.frequency_tags,
                        role_tags=field.role_tags,
                        table_code=table.table_code,
                        table_name=table.table_name,
                        database_name=table.database_name,
                        why_matched=why_matched,
                        score=score,
                    )
                )

        matches.sort(key=lambda item: (-item.score, item.table_code, item.field_name))
        return matches[:limit]

    def suggest_tables(self, table_code: str, database_name: str | None = None, limit: int = 5) -> list[str]:
        records = self.search_tables(table_code, database_name=database_name, limit=max(limit, 10))
        suggestions = [f"{item.table_code} ({item.table_name})" for item in records]
        if suggestions:
            return suggestions[:limit]

        databases = [database_name] if database_name else self.list_databases()
        code_pool: list[str] = []
        for db_name in databases:
            code_pool.extend(record.table_code for record in self.list_tables(db_name))

        return get_close_matches(table_code, code_pool, n=limit, cutoff=0.3)

    def suggest_databases(self, database_name: str, limit: int = 5) -> list[str]:
        normalized_query = database_name.strip().lower()
        if not normalized_query:
            return []

        matches: list[tuple[float, str]] = []
        for candidate in self.list_databases():
            lowered_candidate = candidate.lower()
            score = max(
                SequenceMatcher(None, normalized_query, lowered_candidate).ratio(),
                SequenceMatcher(None, normalized_query, lowered_candidate.replace("数据库", "")).ratio(),
            )
            if normalized_query in lowered_candidate or lowered_candidate in normalized_query:
                score = max(score, 0.9)
            if score >= 0.45:
                matches.append((score, candidate))

        matches.sort(key=lambda item: (-item[0], item[1]))
        suggestions = self._deduplicate([candidate for _, candidate in matches])
        if suggestions:
            return suggestions[:limit]

        return get_close_matches(database_name, self.list_databases(), n=limit, cutoff=0.45)

    def suggest_fields(self, table_code: str, columns: list[str], limit: int = 5) -> list[str]:
        field_pool = [item.field_name for item in self.list_field_schema_items(table_code)]
        suggestions: list[str] = []
        for column in columns:
            for candidate in get_close_matches(column, field_pool, n=limit, cutoff=0.5):
                if candidate not in suggestions:
                    suggestions.append(candidate)
        return suggestions[:limit]

    def build_cache_key(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> str:
        normalized_condition = self._normalize_condition(condition)
        normalized_columns = ",".join(columns)
        return "|".join(
            [
                table_code.strip(),
                normalized_columns,
                normalized_condition,
                (start_date or "").strip(),
                (end_date or "").strip(),
            ]
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
        cache_key = self.build_cache_key(
            table_code=table_code,
            columns=columns,
            condition=condition,
            start_date=start_date,
            end_date=end_date,
        )
        return hashlib.sha1(cache_key.encode("utf-8")).hexdigest()[:16]

    def get_cached_probe(self, cache_key: str) -> ProbeQueryOutput | None:
        with self._lock:
            cached = self._get_cache_value(self._probe_cache, cache_key)
            if cached is None:
                return None
            return cached.model_copy(deep=True)

    def get_validation_record(self, validation_id: str) -> ValidationRecord | None:
        with self._lock:
            record = self._get_cache_value(self._validation_registry, validation_id)
            if record is None:
                return None
            return record

    def set_cached_probe(self, cache_key: str, result: ProbeQueryOutput, record: ValidationRecord) -> None:
        with self._lock:
            now = self._now()
            self._probe_cache[cache_key] = CacheEntry(created_at=now, value=result.model_copy(deep=True))
            self._validation_registry[result.validation_id] = CacheEntry(created_at=now, value=record)

    def mark_rate_limited(self, cache_key: str) -> None:
        with self._lock:
            self._rate_limit_cooldowns[cache_key] = self._now() + self._cache_ttl

    def get_rate_limit_remaining_seconds(self, cache_key: str) -> int | None:
        with self._lock:
            expires_at = self._rate_limit_cooldowns.get(cache_key)
            if expires_at is None:
                return None

            remaining_seconds = int((expires_at - self._now()).total_seconds())
            if remaining_seconds <= 0:
                self._rate_limit_cooldowns.pop(cache_key, None)
                return None

            return remaining_seconds

    def probe_query(self, request: ProbeQueryInput) -> ProbeQueryOutput:
        cache_key = self.build_cache_key(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        cached = self.get_cached_probe(cache_key)
        if cached is not None:
            return cached

        query_fingerprint = self.build_query_fingerprint(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        available_fields = {item.field_name for item in self.list_field_schema_items(request.table_code)}
        invalid_columns = [column for column in request.columns if column not in available_fields]

        validation_id = self._generate_validation_id()
        if invalid_columns:
            result = ProbeQueryOutput(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                row_count=0,
                invalid_columns=invalid_columns,
                can_materialize=False,
            )
            record = ValidationRecord(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                table_code=request.table_code,
                columns=tuple(request.columns),
                condition=request.condition,
                start_date=request.start_date,
                end_date=request.end_date,
                row_count=0,
                can_materialize=False,
            )
            self.set_cached_probe(cache_key, result, record)
            return result

        row_count = self.query_count(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        sample_rows: list[dict[str, Any]] | None = None
        if request.sample_rows > 0 and row_count > 0:
            sample_rows = self.query_sample(
                table_code=request.table_code,
                columns=request.columns,
                sample_rows=request.sample_rows,
                condition=request.condition,
                start_date=request.start_date,
                end_date=request.end_date,
            )

        can_materialize = row_count > 0
        result = ProbeQueryOutput(
            validation_id=validation_id,
            query_fingerprint=query_fingerprint,
            row_count=row_count,
            sample_rows=sample_rows or None,
            can_materialize=can_materialize,
        )

        record = ValidationRecord(
            validation_id=validation_id,
            query_fingerprint=query_fingerprint,
            table_code=request.table_code,
            columns=tuple(request.columns),
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
            row_count=row_count,
            can_materialize=can_materialize,
        )
        self.set_cached_probe(cache_key, result, record)
        return result

    def materialize_query(
        self,
        validation_id: str,
        output_dir: str,
        *,
        max_retries: int = 2,
    ) -> MaterializeQueryOutput:
        record = self.get_validation_record(validation_id)
        if record is None:
            raise CsmarMcpError(
                "invalid_arguments",
                "validation_id was not found or has expired.",
                hint="Call csmar_probe_query first, then pass the returned validation_id.",
            )

        if not record.can_materialize:
            raise CsmarMcpError(
                "invalid_arguments",
                "This validation result cannot be materialized.",
                hint="Fix invalid columns or broaden filters, then run csmar_probe_query again.",
            )

        resolved_output_dir = Path(output_dir).expanduser().resolve()
        materialize_cache_key = f"{record.query_fingerprint}|{resolved_output_dir}"
        with self._lock:
            cached = self._get_cache_value(self._download_cache, materialize_cache_key)
            if cached is not None and self._materialization_exists(cached):
                return cached.model_copy(deep=True)
            if cached is not None:
                self._download_cache.pop(materialize_cache_key, None)

        last_error: CsmarMcpError | None = None
        for attempt in range(max(0, max_retries) + 1):
            try:
                output = self._materialize_query_once(record, resolved_output_dir, retries=attempt)
                with self._lock:
                    self._download_cache[materialize_cache_key] = CacheEntry(
                        created_at=self._now(),
                        value=output.model_copy(deep=True),
                    )
                return output
            except CsmarMcpError as error:
                last_error = error
                if attempt >= max_retries:
                    break
                time.sleep(min(2 * (attempt + 1), 8))

        if last_error is None:
            raise CsmarMcpError(
                "download_failed",
                "The download did not complete.",
                hint="Retry after running csmar_probe_query for the same query.",
            )
        raise last_error

    def query_count(
        self,
        table_code: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        payload = self._build_query_payload(table_code, columns, condition, start_date, end_date)
        response = self._post(self._service.urlUtil.getQueryCountUrl(), payload)
        count_value = response.get("data", 0)
        try:
            return int(count_value)
        except (TypeError, ValueError) as error:
            raise CsmarMcpError(
                "upstream_error",
                "CSMAR returned an unexpected row count.",
                hint="Retry the same request. If it fails again, inspect table schema and simplify conditions.",
                raw_message=repr(count_value),
            ) from error

    def query_sample(
        self,
        table_code: str,
        columns: list[str],
        sample_rows: int,
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        if sample_rows <= 0:
            return []

        limited_condition = self._append_limit_clause(condition, sample_rows)
        payload = self._build_query_payload(table_code, columns, limited_condition, start_date, end_date)
        response = self._post(self._service.urlUtil.getQueryUrl(), payload)
        data = response.get("data", {})
        rows = data.get("previewDatas", []) if isinstance(data, dict) else []
        normalized_rows = [row for row in rows if isinstance(row, dict)]
        return normalized_rows[:sample_rows]

    def _materialize_query_once(
        self,
        record: ValidationRecord,
        output_dir: Path,
        *,
        retries: int,
    ) -> MaterializeQueryOutput:
        payload = self._build_query_payload(
            table_code=record.table_code,
            columns=list(record.columns),
            condition=record.condition,
            start_date=record.start_date,
            end_date=record.end_date,
        )

        pack_response = self._post(self._service.urlUtil.getPackUrl(), payload)
        sign_code = str(pack_response.get("data", "")).strip()
        if not sign_code:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR did not return a package identifier.",
                hint="Retry materialization. If it fails again, run csmar_probe_query and retry.",
            )

        pack_result, packaged_at = self._poll_pack_result(sign_code)
        file_url = str(pack_result.get("filePath", "")).strip()
        if not file_url:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR finished packaging but did not provide a file URL.",
                hint="Retry materialization. If the issue persists, narrow the query and probe again.",
            )

        output_dir.mkdir(parents=True, exist_ok=True)

        download_id = self._generate_download_id()
        zip_path = (output_dir / f"{download_id}_{sign_code}.zip").resolve()
        extract_dir = (output_dir / f"{download_id}_{sign_code}").resolve()

        http_response = self._http.request("GET", file_url)
        if http_response.status >= 400:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR returned a download URL but the archive could not be fetched.",
                hint="Retry materialization. If it fails again, narrow the query and retry.",
                raw_message=f"HTTP {http_response.status}",
            )

        zip_path.write_bytes(http_response.data)

        try:
            with zipfile.ZipFile(zip_path) as zip_file:
                zip_file.extractall(path=extract_dir)
        except Exception as error:
            raise CsmarMcpError(
                "unzip_failed",
                "The downloaded archive could not be extracted.",
                hint="Retry materialization. If it fails again, clean output_dir and retry.",
                raw_message=str(error),
            ) from error

        extracted_files = sorted(str(path.resolve()) for path in extract_dir.rglob("*") if path.is_file())
        completed_at = self._now()

        return MaterializeQueryOutput(
            download_id=download_id,
            query_fingerprint=record.query_fingerprint,
            output_dir=str(output_dir),
            files=extracted_files,
            row_count=record.row_count,
            archive_path=str(zip_path),
            audit=MaterializeAudit(
                retries=retries,
                packaged_at=self._to_iso_timestamp(packaged_at),
                completed_at=self._to_iso_timestamp(completed_at),
            ),
        )

    def _poll_pack_result(self, sign_code: str) -> tuple[dict[str, Any], datetime]:
        endpoint = f"{self._service.urlUtil.getPackResultUrl()}/{sign_code}"
        deadline = time.monotonic() + self._poll_timeout_seconds

        while True:
            response = self._get(endpoint, include_belong=True)
            data = response.get("data", {})
            status = str(data.get("status", ""))

            if status == "1":
                return data, self._now()
            if status == "0":
                raise CsmarMcpError(
                    "download_failed",
                    "CSMAR failed to build the download package.",
                    hint="Check columns and condition, run csmar_probe_query again, then retry materialization.",
                )
            if time.monotonic() >= deadline:
                raise CsmarMcpError(
                    "download_failed",
                    "Timed out while waiting for the packaged archive.",
                    hint="Retry materialization or narrow the query.",
                )

            time.sleep(self._poll_interval_seconds)

    def _materialization_exists(self, output: MaterializeQueryOutput) -> bool:
        archive_path = Path(output.archive_path)
        if not archive_path.exists():
            return False
        return all(Path(file_path).exists() for file_path in output.files)

    def _resolve_table_candidates(
        self,
        *,
        database_name: str | None,
        table_code: str | None,
    ) -> list[CatalogRecord]:
        if table_code:
            if database_name:
                candidates = [record for record in self.list_tables(database_name) if record.table_code == table_code]
            else:
                candidates = self._find_table_records(table_code)

            if not candidates:
                raise CsmarMcpError(
                    "table_not_found",
                    "The table_code was not found.",
                    hint="Use csmar_search_tables to find a valid table_code, then retry.",
                    candidate_values=self.suggest_tables(table_code, database_name=database_name),
                )
            return candidates

        if database_name:
            return self.list_tables(database_name)

        candidates: list[CatalogRecord] = []
        for db_name in self.list_databases():
            candidates.extend(self.list_tables(db_name))
        return candidates

    def _find_table_records(self, table_code: str) -> list[CatalogRecord]:
        normalized = table_code.strip().lower()
        if not normalized:
            return []

        matches: list[CatalogRecord] = []
        for db_name in self.list_databases():
            for record in self.list_tables(db_name):
                if record.table_code.lower() == normalized:
                    matches.append(record)
        return matches

    def _score_table_match(self, record: CatalogRecord, query: str) -> tuple[float, str] | None:
        code = record.table_code.lower()
        name = record.table_name.lower()
        database = record.database_name.lower()

        if query == code:
            return 100.0, "exact table code match"
        if query == name:
            return 98.0, "exact table name match"
        if query in code:
            return 94.0, "table code contains query"
        if query in name:
            return 90.0, "table name contains query"
        if query in database:
            return 75.0, "database name contains query"

        ratio = max(
            SequenceMatcher(None, query, code).ratio(),
            SequenceMatcher(None, query, name).ratio(),
            SequenceMatcher(None, query, database).ratio(),
        )
        if ratio < 0.35:
            return None
        return round(60.0 + ratio * 30.0, 2), "similar to query text"

    def _score_field_match(
        self,
        field: FieldSchemaItem,
        query: str,
        role_hint: str | None,
        frequency_hint: str | None,
    ) -> tuple[float, str] | None:
        field_name = field.field_name.lower()
        field_label = (field.field_label or "").lower()
        field_description = (field.field_description or "").lower()
        data_type = (field.data_type or "").lower()
        role_blob = " ".join(field.role_tags or []).lower()
        frequency_blob = " ".join(field.frequency_tags or []).lower()

        reasons: list[str] = []
        if query == field_name:
            score = 100.0
            reasons.append("exact field name match")
        elif field_label and query == field_label:
            score = 98.0
            reasons.append("exact field label match")
        elif query in field_name:
            score = 94.0
            reasons.append("field name contains query")
        elif field_label and query in field_label:
            score = 91.0
            reasons.append("field label contains query")
        elif field_description and query in field_description:
            score = 87.0
            reasons.append("field description contains query")
        else:
            ratio = max(
                SequenceMatcher(None, query, field_name).ratio(),
                SequenceMatcher(None, query, field_label).ratio() if field_label else 0.0,
                SequenceMatcher(None, query, field_description).ratio() if field_description else 0.0,
                SequenceMatcher(None, query, data_type).ratio() if data_type else 0.0,
            )
            if ratio < 0.34:
                return None
            score = round(60.0 + ratio * 30.0, 2)
            reasons.append("similar to query text")

        if role_hint:
            normalized_role_hint = role_hint.strip().lower()
            if normalized_role_hint and (
                normalized_role_hint in role_blob
                or normalized_role_hint in field_label
                or normalized_role_hint in field_description
            ):
                score += 4.0
                reasons.append("role hint matched")
            elif normalized_role_hint:
                score -= 1.0

        if frequency_hint:
            normalized_frequency_hint = frequency_hint.strip().lower()
            if normalized_frequency_hint and (
                normalized_frequency_hint in frequency_blob
                or normalized_frequency_hint in field_label
                or normalized_frequency_hint in field_description
            ):
                score += 4.0
                reasons.append("frequency hint matched")
            elif normalized_frequency_hint:
                score -= 1.0

        return round(max(0.0, min(100.0, score)), 2), "; ".join(reasons)

    def _normalize_field_schema_list(self, values: Any) -> list[FieldSchemaItem]:
        if not isinstance(values, list):
            return []

        items: list[FieldSchemaItem] = []
        seen: set[str] = set()
        for raw_item in values:
            if isinstance(raw_item, str):
                field_name = raw_item.strip()
                if not field_name or field_name in seen:
                    continue
                seen.add(field_name)
                items.append(FieldSchemaItem(field_name=field_name))
                continue

            if not isinstance(raw_item, dict):
                continue

            field_name = self._pick_text(
                raw_item,
                preferred_keys=("field", "fieldName", "column", "columnName", "name", "value"),
                token_hints=("field", "column", "name", "code"),
            )
            if not field_name or field_name in seen:
                continue

            seen.add(field_name)
            field_label = self._pick_text(
                raw_item,
                preferred_keys=("fieldLabel", "label", "fieldNameCn", "cnName", "nameCn", "displayName", "title"),
                token_hints=("label", "title", "cn", "ch", "display"),
            )
            if field_label == field_name:
                field_label = None

            field_description = self._pick_text(
                raw_item,
                preferred_keys=("fieldDesc", "description", "remark", "comment", "memo", "help"),
                token_hints=("desc", "description", "remark", "comment", "memo", "help"),
            )
            data_type = self._pick_text(
                raw_item,
                preferred_keys=("dataType", "fieldType", "type", "valueType"),
                token_hints=("type", "dtype"),
            )

            frequency_tags = self._extract_tags(
                raw_item,
                preferred_keys=("frequencyTags", "frequencyTag", "freqTag", "frequency", "freq", "period", "cycle"),
                token_hints=("frequency", "freq", "period", "cycle"),
            )
            role_tags = self._extract_tags(
                raw_item,
                preferred_keys=("roleTags", "roleTag", "role", "dimension", "measure", "metric", "identifier"),
                token_hints=("role", "dimension", "measure", "metric", "identifier"),
            )

            items.append(
                FieldSchemaItem(
                    field_name=field_name,
                    field_label=field_label,
                    field_description=field_description,
                    data_type=data_type,
                    frequency_tags=frequency_tags,
                    role_tags=role_tags,
                )
            )

        return items

    def _pick_text(
        self,
        payload: dict[str, Any],
        *,
        preferred_keys: tuple[str, ...],
        token_hints: tuple[str, ...],
    ) -> str | None:
        for key in preferred_keys:
            value = payload.get(key)
            text = self._to_text(value)
            if text:
                return text

        lowered_map = {key.lower(): value for key, value in payload.items()}
        for key in preferred_keys:
            value = lowered_map.get(key.lower())
            text = self._to_text(value)
            if text:
                return text

        for key, value in payload.items():
            lowered_key = key.lower()
            if not any(token in lowered_key for token in token_hints):
                continue
            text = self._to_text(value)
            if text:
                return text
        return None

    def _extract_tags(
        self,
        payload: dict[str, Any],
        *,
        preferred_keys: tuple[str, ...],
        token_hints: tuple[str, ...],
    ) -> list[str] | None:
        for key in preferred_keys:
            if key in payload:
                tags = self._to_tag_list(payload.get(key))
                if tags:
                    return tags

        lowered_map = {key.lower(): value for key, value in payload.items()}
        for key in preferred_keys:
            if key.lower() in lowered_map:
                tags = self._to_tag_list(lowered_map[key.lower()])
                if tags:
                    return tags

        for key, value in payload.items():
            lowered_key = key.lower()
            if not any(token in lowered_key for token in token_hints):
                continue
            tags = self._to_tag_list(value)
            if tags:
                return tags

        return None

    def _to_text(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (str, int, float)):
            text = str(value).strip()
            return text or None
        return None

    def _to_tag_list(self, value: Any) -> list[str] | None:
        raw_values: list[str] = []
        if value is None:
            return None

        if isinstance(value, str):
            raw_values = [item.strip() for item in re.split(r"[,;|/，；、]", value) if item.strip()]
        elif isinstance(value, (list, tuple, set)):
            for item in value:
                text = self._to_text(item)
                if text:
                    raw_values.append(text)
        else:
            text = self._to_text(value)
            if text:
                raw_values = [text]

        if not raw_values:
            return None

        deduplicated: list[str] = []
        seen: set[str] = set()
        for item in raw_values:
            if item in seen:
                continue
            seen.add(item)
            deduplicated.append(item)

        return deduplicated or None

    def _build_query_payload(
        self,
        table_code: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "columns": columns,
            "condition": self._normalize_condition(condition),
            "table": table_code,
        }
        if start_date:
            payload["startTime"] = start_date
        if end_date:
            payload["endTime"] = end_date
        return payload

    def _append_limit_clause(self, condition: str | None, limit: int) -> str:
        normalized = self._normalize_condition(condition)
        if " limit " in normalized.lower():
            return normalized
        return f"{normalized} limit 0,{limit}"

    def _normalize_condition(self, condition: str | None) -> str:
        normalized = (condition or "").strip()
        return normalized if normalized else "1=1"

    def _generate_validation_id(self) -> str:
        return f"validation_{uuid4().hex[:10]}"

    def _generate_download_id(self) -> str:
        return f"download_{uuid4().hex[:10]}"

    def _to_iso_timestamp(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _get(self, endpoint: str, include_belong: bool = False) -> dict[str, Any]:
        def requester() -> dict[str, Any]:
            headers = self._build_headers(include_belong=include_belong, include_json=False)
            return self._service.doGet(endpoint, headers=headers)

        return self._request_with_reauth(requester)

    def _post(self, endpoint: str, payload: dict[str, Any], include_belong: bool = False) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        def requester() -> dict[str, Any]:
            headers = self._build_headers(include_belong=include_belong, include_json=True)
            return self._service.doPost(endpoint, body=body, headers=headers)

        return self._request_with_reauth(requester)

    def _request_with_reauth(self, requester: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        try:
            self._ensure_login()
            response = requester()

            if self._is_auth_error(response):
                with self._lock:
                    self._login()
                response = requester()

            if response.get("code") != 0:
                raise self._to_error(response)
            return response
        except CsmarMcpError:
            raise
        except Exception as error:
            raise CsmarMcpError(
                "upstream_error",
                "CSMAR did not return a valid response.",
                hint="Retry the same request. If it fails again, narrow the request and inspect server logs.",
                raw_message=str(error),
            ) from error

    def _ensure_login(self) -> None:
        with self._lock:
            if self._logged_in and self._get_token_lines() is not None:
                return
            self._login()

    def _login(self) -> None:
        raw_response = self._service.logon(self._account, self._password, self._lang, self._belong)
        response = raw_response if isinstance(raw_response, dict) else {"code": -1, "msg": "Login request failed"}
        if response.get("code") != 0:
            raise self._to_error(response, fallback_error_code="auth_failed")

        token = str(response.get("data", {}).get("token", "")).strip()
        if not token:
            raise CsmarMcpError(
                "auth_failed",
                "Authentication failed because CSMAR did not return a token.",
                hint="Check account and password, then restart the MCP server.",
            )

        self._service.writeToken(token, self._lang, self._belong)
        self._logged_in = True

    def _get_token_lines(self) -> list[str] | None:
        try:
            token_lines = self._service.getTokenFromFile()
        except Exception:
            return None

        if not token_lines or token_lines is False:
            return None

        if not isinstance(token_lines, list) or len(token_lines) < 2:
            return None

        return token_lines

    def _build_headers(self, include_belong: bool, include_json: bool) -> dict[str, str]:
        token_lines = self._get_token_lines()
        if token_lines is None:
            self._login()
            token_lines = self._get_token_lines()

        if token_lines is None:
            raise CsmarMcpError(
                "auth_failed",
                "Authentication failed because the token file could not be read.",
                hint="Check account and password, then restart the MCP server.",
            )

        headers: dict[str, str] = {
            "Lang": token_lines[1].strip(),
            "Token": token_lines[0].strip(),
        }

        if include_belong:
            headers["belong"] = token_lines[2].strip() if len(token_lines) >= 3 else self._belong

        if include_json:
            headers["Content-Type"] = "application/json"

        return headers

    def _is_auth_error(self, response: dict[str, Any]) -> bool:
        code = response.get("code")
        if code == -3004:
            return True
        message = str(response.get("msg", "")).lower()
        return "offline" in message or "login" in message

    def _to_error(self, response: dict[str, Any], fallback_error_code: str = "upstream_error") -> CsmarMcpError:
        upstream_code = response.get("code")
        raw_message = str(response.get("msg") or "Unknown upstream error from CSMAR")
        lowered_message = raw_message.lower()

        if upstream_code == -3004 or "offline" in lowered_message or "login" in lowered_message:
            error_code = "auth_failed"
        elif any(token in lowered_message for token in ("purchase", "permission", "authorized")):
            error_code = "not_purchased"
        elif (
            any(token in lowered_message for token in ("database", "db", "数据库"))
            and any(token in lowered_message for token in ("not exist", "does not exist", "missing", "不存在"))
        ):
            error_code = "database_not_found"
        elif "table" in lowered_message and any(token in lowered_message for token in ("not", "exist", "missing")):
            error_code = "table_not_found"
        elif "field" in lowered_message and any(token in lowered_message for token in ("not", "exist", "missing")):
            error_code = "field_not_found"
        elif any(token in lowered_message for token in ("condition", "syntax", "sql")):
            error_code = "invalid_condition"
        elif self._is_rate_limited_message(lowered_message):
            error_code = "rate_limited"
        else:
            error_code = fallback_error_code

        return CsmarMcpError(
            error_code=error_code,
            message=self._summarize_error(error_code),
            hint=self._default_hint(error_code),
            upstream_code=upstream_code,
            raw_message=raw_message,
        )

    def _summarize_error(self, error_code: str) -> str:
        messages = {
            "auth_failed": "Authentication with CSMAR failed.",
            "database_not_found": "The database_name was not found.",
            "not_purchased": "The requested database or table is not available to this account.",
            "table_not_found": "The table_code was not found.",
            "field_not_found": "One or more requested columns do not exist in the table.",
            "invalid_condition": "The condition could not be parsed by CSMAR.",
            "rate_limited": "CSMAR is cooling down the same query.",
            "download_failed": "CSMAR could not build or fetch the requested archive.",
            "unzip_failed": "The downloaded archive could not be extracted.",
            "upstream_error": "CSMAR returned an unexpected error.",
            "invalid_arguments": "The tool arguments are invalid.",
        }
        return messages.get(error_code, "CSMAR returned an unexpected error.")

    def _default_hint(self, error_code: str) -> str:
        hints = {
            "auth_failed": "Check account and password, then restart the MCP server.",
            "database_not_found": "Call csmar_list_databases and copy database_name verbatim, then retry.",
            "not_purchased": "Choose a table from a purchased database before retrying.",
            "table_not_found": "Use csmar_search_tables to find a valid table_code, then retry.",
            "field_not_found": "Use csmar_get_table_schema to inspect fields, then retry.",
            "invalid_condition": "Fix condition syntax and retry. Example: use '=' instead of '=='.",
            "rate_limited": "Retry after cooldown expires or change condition/date range.",
            "download_failed": "Run csmar_probe_query first, then retry csmar_materialize_query.",
            "unzip_failed": "Retry materialization. If it fails again, clear output_dir and retry.",
            "upstream_error": "Retry the same request. If it fails again, narrow the query.",
            "invalid_arguments": "Fix invalid fields and retry with the same tool.",
        }
        return hints.get(error_code, "Review the arguments and retry.")

    def _is_rate_limited_message(self, lowered_message: str) -> bool:
        rate_limit_tokens = (
            "30",
            "minute",
            "rate",
            "frequen",
            "too often",
            "repeatedly",
            "repeat",
            "don't submit",
            "do not submit",
            "same request",
            "请不要重复提交",
            "重复提交",
        )
        return any(token in lowered_message for token in rate_limit_tokens)

    def _normalize_name_list(self, values: Any, dict_name_keys: tuple[str, ...]) -> list[str]:
        if not isinstance(values, list):
            return []

        normalized_values: list[str] = []
        for item in values:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    normalized_values.append(text)
                continue

            if not isinstance(item, dict):
                continue

            selected_value: str | None = None
            for key in dict_name_keys:
                raw_value = item.get(key)
                if raw_value is not None and str(raw_value).strip():
                    selected_value = str(raw_value).strip()
                    break

            if not selected_value:
                for raw_value in item.values():
                    if raw_value is not None and isinstance(raw_value, (str, int, float)):
                        text = str(raw_value).strip()
                        if text:
                            selected_value = text
                            break

            if selected_value:
                normalized_values.append(selected_value)

        return normalized_values

    def _normalize_table_list(self, database_name: str, values: Any) -> list[CatalogRecord]:
        if not isinstance(values, list):
            return []

        table_records: list[CatalogRecord] = []
        seen_codes: set[str] = set()

        for item in values:
            if isinstance(item, str) or not isinstance(item, dict):
                continue

            table_code: str | None = None
            table_name: str | None = None

            for code_key in ("tableCode", "code", "table", "tableNameEn", "engName"):
                raw_value = item.get(code_key)
                if raw_value is not None and str(raw_value).strip():
                    table_code = str(raw_value).strip()
                    break

            for name_key in ("tableName", "name", "tableNameCn", "cnName"):
                raw_value = item.get(name_key)
                if raw_value is not None and str(raw_value).strip():
                    table_name = str(raw_value).strip()
                    break

            if table_code is None:
                for key, raw_value in item.items():
                    if raw_value is None:
                        continue
                    text = str(raw_value).strip()
                    if not text:
                        continue
                    lowered_key = key.lower()
                    if "code" in lowered_key or "en" in lowered_key:
                        table_code = text
                    elif "name" in lowered_key or "cn" in lowered_key:
                        table_name = text

            if table_code and table_code not in seen_codes:
                seen_codes.add(table_code)
                table_records.append(
                    CatalogRecord(
                        database_name=database_name,
                        table_code=table_code,
                        table_name=table_name or table_code,
                    )
                )

        return table_records

    def _deduplicate(self, values: list[str]) -> list[str]:
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)
        return unique_values

    def _get_cache_value(self, cache: dict[str, CacheEntry], key: str) -> Any | None:
        entry = cache.get(key)
        if entry is None:
            return None
        if self._now() - entry.created_at > self._cache_ttl:
            cache.pop(key, None)
            return None
        return entry.value

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
