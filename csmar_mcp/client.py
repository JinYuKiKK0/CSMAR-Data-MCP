from __future__ import annotations

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

import urllib3

from csmarapi.CsmarService import CsmarService

from .models import (
    CatalogItem,
    DownloadManifest,
    DownloadMaterializeInput,
    DownloadMaterializeOutput,
    QueryValidateInput,
    QueryValidateOutput,
    TableSchemaOutput,
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
        self._validation_cache: dict[str, CacheEntry] = {}
        self._download_cache: dict[str, CacheEntry] = {}
        self._rate_limit_cooldowns: dict[str, datetime] = {}
        self._download_manifests: dict[str, DownloadManifest] = {}

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

    def search_catalog(self, query: str, database_name: str | None = None, limit: int = 10) -> list[CatalogItem]:
        normalized_query = query.strip().lower()
        databases = [database_name] if database_name else self.list_databases()
        matches: list[tuple[float, CatalogItem]] = []

        for db_name in databases:
            for record in self.list_tables(db_name):
                scored = self._score_catalog_match(record, normalized_query)
                if scored is None:
                    continue
                score, why_matched = scored
                matches.append(
                    (
                        score,
                        CatalogItem(
                            table_code=record.table_code,
                            table_name=record.table_name,
                            database_name=record.database_name,
                            why_matched=why_matched,
                        ),
                    )
                )

        if not matches:
            fallback_matches: list[tuple[float, CatalogItem]] = []
            for db_name in databases:
                for record in self.list_tables(db_name):
                    ratio = max(
                        SequenceMatcher(None, normalized_query, record.table_code.lower()).ratio(),
                        SequenceMatcher(None, normalized_query, record.table_name.lower()).ratio(),
                    )
                    if ratio < 0.35:
                        continue
                    fallback_matches.append(
                        (
                            ratio,
                            CatalogItem(
                                table_code=record.table_code,
                                table_name=record.table_name,
                                database_name=record.database_name,
                                why_matched="similar to the search text",
                            ),
                        )
                    )
            matches = fallback_matches

        matches.sort(key=lambda item: (-item[0], item[1].table_code))
        return [item for _, item in matches[:limit]]

    def list_fields(self, table_code: str) -> list[str]:
        encoded_table_name = parse.quote(table_code)
        endpoint = f"{self._service.urlUtil.getListFieldsUrl()}?table={encoded_table_name}"
        response = self._get(endpoint, include_belong=True)
        return self._deduplicate(
            self._normalize_name_list(
                response.get("data"),
                dict_name_keys=("field", "fieldName", "name", "columnName", "column", "value"),
            )
        )

    def read_table_schema(self, table_code: str) -> TableSchemaOutput:
        fields = self.list_fields(table_code)
        return TableSchemaOutput(table_code=table_code, field_count=len(fields), fields=fields)

    def suggest_tables(self, table_code: str, database_name: str | None = None, limit: int = 5) -> list[str]:
        records = self.search_catalog(table_code, database_name=database_name, limit=max(limit, 10))
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
        field_pool = self.list_fields(table_code)
        suggestions: list[str] = []
        for column in columns:
            for candidate in get_close_matches(column, field_pool, n=limit, cutoff=0.5):
                if candidate not in suggestions:
                    suggestions.append(candidate)
        return suggestions[:limit]

    def preview(self, table_code: str) -> list[dict[str, Any]]:
        payload = {"table": table_code}
        response = self._post(self._service.urlUtil.getPreviewUrl(), payload)
        data = response.get("data", {})
        preview_rows = data.get("previewDatas", []) if isinstance(data, dict) else []
        return [row for row in preview_rows if isinstance(row, dict)]

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

    def get_cached_validation(self, cache_key: str) -> QueryValidateOutput | None:
        with self._lock:
            cached = self._get_cache_value(self._validation_cache, cache_key)
            return cached.model_copy(deep=True) if cached is not None else None

    def set_cached_validation(self, cache_key: str, result: QueryValidateOutput) -> None:
        with self._lock:
            self._validation_cache[cache_key] = CacheEntry(created_at=self._now(), value=result.model_copy(deep=True))

    def get_cached_manifest(self, cache_key: str) -> DownloadManifest | None:
        with self._lock:
            cached = self._get_cache_value(self._download_cache, cache_key)
            if cached is None:
                return None
            if self._manifest_exists(cached):
                return cached.model_copy(deep=True)
            self._download_cache.pop(cache_key, None)
            return None

    def set_cached_manifest(self, cache_key: str, manifest: DownloadManifest) -> None:
        with self._lock:
            manifest_copy = manifest.model_copy(deep=True)
            self._download_cache[cache_key] = CacheEntry(created_at=self._now(), value=manifest_copy)
            self._download_manifests[manifest.download_id] = manifest_copy

    def get_download_manifest(self, download_id: str) -> DownloadManifest | None:
        with self._lock:
            manifest = self._download_manifests.get(download_id)
            if manifest is None:
                return None
            if not self._manifest_exists(manifest):
                self._download_manifests.pop(download_id, None)
                return None
            return manifest.model_copy(deep=True)

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

    def validate_query(self, request: QueryValidateInput) -> QueryValidateOutput:
        cache_key = self.build_cache_key(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        cached = self.get_cached_validation(cache_key)
        if cached is not None:
            return cached

        table_fields = set(self.list_fields(request.table_code))
        missing_columns = [column for column in request.columns if column not in table_fields]
        if missing_columns:
            raise CsmarMcpError(
                "field_not_found",
                "One or more requested columns do not exist in the table.",
                hint="Use csmar_get_table_schema to inspect the table fields, then retry with valid columns.",
                candidate_values=self.suggest_fields(request.table_code, missing_columns),
            )

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

        result = QueryValidateOutput(
            table_code=request.table_code,
            row_count=row_count,
            sample_rows=sample_rows or None,
            can_download=row_count > 0,
        )
        self.set_cached_validation(cache_key, result)
        return result

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
                hint="Retry the same request. If it fails again, narrow the query and inspect the schema first.",
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

    def materialize_download(self, request: DownloadMaterializeInput, max_retries: int = 2) -> DownloadMaterializeOutput:
        cache_key = self.build_cache_key(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        cached_manifest = self.get_cached_manifest(cache_key)
        if cached_manifest is not None:
            aliased_manifest = self._alias_manifest(cached_manifest, request.download_id)
            return self._manifest_to_output(aliased_manifest)

        last_error: CsmarMcpError | None = None
        for attempt in range(max(0, max_retries) + 1):
            try:
                manifest = self._materialize_download_once(request)
                self.set_cached_manifest(cache_key, manifest)
                return self._manifest_to_output(manifest)
            except CsmarMcpError as error:
                last_error = error
                if error.error_code == "rate_limited":
                    cached_manifest = self.get_cached_manifest(cache_key)
                    if cached_manifest is not None:
                        aliased_manifest = self._alias_manifest(cached_manifest, request.download_id)
                        return self._manifest_to_output(aliased_manifest)
                    raise

                if attempt >= max_retries:
                    break

                time.sleep(min(2 * (attempt + 1), 8))

        if last_error is None:
            raise CsmarMcpError(
                "download_failed",
                "The download did not complete.",
                hint="Retry the download after validating the same query with csmar_query_validate.",
            )
        raise last_error

    def _materialize_download_once(self, request: DownloadMaterializeInput) -> DownloadManifest:
        payload = self._build_query_payload(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        pack_response = self._post(self._service.urlUtil.getPackUrl(), payload)
        sign_code = str(pack_response.get("data", "")).strip()
        if not sign_code:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR did not return a package identifier.",
                hint="Retry the download. If it fails again, validate the same query before downloading.",
            )

        pack_result = self._poll_pack_result(sign_code)
        file_url = str(pack_result.get("filePath", "")).strip()
        if not file_url:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR finished packaging but did not provide a file URL.",
                hint="Retry the download. If the issue persists, narrow the query and try again.",
            )

        resolved_output_dir = Path(request.output_dir).expanduser().resolve()
        resolved_output_dir.mkdir(parents=True, exist_ok=True)

        sanitized_download_id = self._sanitize_path_fragment(request.download_id)
        zip_path = (resolved_output_dir / f"{sanitized_download_id}_{sign_code}.zip").resolve()
        extract_dir = (resolved_output_dir / f"{sanitized_download_id}_{sign_code}").resolve()

        http_response = self._http.request("GET", file_url)
        if http_response.status >= 400:
            raise CsmarMcpError(
                "download_failed",
                "CSMAR returned a download URL but the archive could not be fetched.",
                hint="Retry the download. If it fails again, try a narrower query.",
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
                hint="Retry the download. If it fails again, delete the output directory and try again.",
                raw_message=str(error),
            ) from error

        extracted_files = sorted(str(path.resolve()) for path in extract_dir.rglob("*") if path.is_file())
        return DownloadManifest(
            download_id=request.download_id,
            table_code=request.table_code,
            zip_path=str(zip_path),
            extract_dir=str(extract_dir),
            files=extracted_files,
        )

    def _poll_pack_result(self, sign_code: str) -> dict[str, Any]:
        endpoint = f"{self._service.urlUtil.getPackResultUrl()}/{sign_code}"
        deadline = time.monotonic() + self._poll_timeout_seconds

        while True:
            response = self._get(endpoint, include_belong=True)
            data = response.get("data", {})
            status = str(data.get("status", ""))

            if status == "1":
                return data
            if status == "0":
                raise CsmarMcpError(
                    "download_failed",
                    "CSMAR failed to build the download package.",
                    hint="Check the columns and condition, then retry. Validate the same request before downloading if needed.",
                )
            if time.monotonic() >= deadline:
                raise CsmarMcpError(
                    "download_failed",
                    "Timed out while waiting for the packaged archive.",
                    hint="Retry the download or narrow the query to reduce package size.",
                )

            time.sleep(self._poll_interval_seconds)

    def _alias_manifest(self, manifest: DownloadManifest, download_id: str) -> DownloadManifest:
        if manifest.download_id == download_id:
            with self._lock:
                self._download_manifests[download_id] = manifest.model_copy(deep=True)
            return manifest.model_copy(deep=True)

        aliased_manifest = manifest.model_copy(update={"download_id": download_id}, deep=True)
        with self._lock:
            self._download_manifests[download_id] = aliased_manifest.model_copy(deep=True)
        return aliased_manifest

    def _manifest_to_output(self, manifest: DownloadManifest) -> DownloadMaterializeOutput:
        return DownloadMaterializeOutput(
            download_id=manifest.download_id,
            table_code=manifest.table_code,
            zip_path=manifest.zip_path,
            extract_dir=manifest.extract_dir,
            file_count=len(manifest.files),
        )

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

    def _score_catalog_match(self, record: CatalogRecord, query: str) -> tuple[float, str] | None:
        code = record.table_code.lower()
        name = record.table_name.lower()
        database = record.database_name.lower()

        if query == code:
            return 100.0, "exact table code match"
        if query == name:
            return 95.0, "exact table name match"
        if query in code:
            return 90.0, "table code contains the search text"
        if query in name:
            return 85.0, "table name contains the search text"
        if query in database:
            return 70.0, "database name contains the search text"
        return None

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
                hint="Retry the same request. If it fails again, narrow the request and inspect the server logs.",
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
                hint="Check the account and password, then restart the MCP server.",
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
                hint="Check the account and password, then restart the MCP server.",
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
        }
        return messages.get(error_code, "CSMAR returned an unexpected error.")

    def _default_hint(self, error_code: str) -> str:
        hints = {
            "auth_failed": "Check the account and password, then restart the MCP server.",
            "database_not_found": "Call csmar_list_databases, copy a returned database_name verbatim, then retry.",
            "not_purchased": "Choose a table from a purchased database before retrying.",
            "table_not_found": "Use csmar_catalog_search to find a valid table_code, then retry.",
            "field_not_found": "Use csmar_get_table_schema to inspect the field list, then retry.",
            "invalid_condition": "Fix the condition syntax and retry. Example: use '=' instead of '=='.",
            "rate_limited": "Retry after the cooldown expires or change the condition or date range.",
            "download_failed": "Validate the query first, then retry the download.",
            "unzip_failed": "Retry the download. If it fails again, clear the output directory and retry.",
            "upstream_error": "Retry the same request. If it fails again, narrow the query and inspect the schema first.",
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

    def _manifest_exists(self, manifest: DownloadManifest) -> bool:
        zip_path = Path(manifest.zip_path)
        extract_dir = Path(manifest.extract_dir)
        if not zip_path.exists() or not extract_dir.exists():
            return False
        return all(Path(file_path).exists() for file_path in manifest.files)

    def _sanitize_path_fragment(self, value: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
        return sanitized or "download"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
