from __future__ import annotations

import json
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib import parse

import urllib3

from csmarapi.CsmarService import CsmarService

from .models import DownloadArtifact, DownloadPlan, ProbeQuery, ProbeResult, TableInfo


@dataclass(slots=True)
class CacheEntry:
    created_at: datetime
    value: Any


class CsmarMcpError(Exception):
    def __init__(self, error_code: str, message: str, upstream_code: int | None = None) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.upstream_code = upstream_code


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
        self._probe_cache: dict[str, CacheEntry] = {}
        self._download_cache: dict[str, CacheEntry] = {}
        self._rate_limit_cooldowns: dict[str, datetime] = {}

        self._cache_ttl = timedelta(minutes=max(1, cache_ttl_minutes))
        self._poll_interval_seconds = max(1, poll_interval_seconds)
        self._poll_timeout_seconds = max(30, poll_timeout_seconds)
        self._logged_in = False

    def build_probe_cache_key(self, query: ProbeQuery) -> str:
        return self._build_cache_key(
            table_name=query.table_name,
            columns=query.columns,
            condition=query.condition,
            start_date=query.start_date,
            end_date=query.end_date,
        )

    def build_download_cache_key(self, plan: DownloadPlan) -> str:
        return self._build_cache_key(
            table_name=plan.table_name,
            columns=plan.columns,
            condition=plan.condition,
            start_date=plan.start_date,
            end_date=plan.end_date,
        )

    def get_cached_probe(self, cache_key: str) -> ProbeResult | None:
        with self._lock:
            value = self._get_cache_value(self._probe_cache, cache_key)
            return value.model_copy(deep=True) if value else None

    def set_cached_probe(self, cache_key: str, result: ProbeResult) -> None:
        with self._lock:
            self._probe_cache[cache_key] = CacheEntry(created_at=self._now(), value=result.model_copy(deep=True))

    def get_cached_download(self, cache_key: str) -> DownloadArtifact | None:
        with self._lock:
            value = self._get_cache_value(self._download_cache, cache_key)
            if value and self._artifact_exists(value):
                return value.model_copy(deep=True)
            if value:
                self._download_cache.pop(cache_key, None)
            return None

    def set_cached_download(self, cache_key: str, artifact: DownloadArtifact) -> None:
        with self._lock:
            self._download_cache[cache_key] = CacheEntry(created_at=self._now(), value=artifact.model_copy(deep=True))

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

    def list_databases(self) -> list[str]:
        response = self._get(self._service.urlUtil.getListDbsUrl(), include_belong=True)
        return self._deduplicate(
            self._normalize_name_list(response.get("data"), dict_name_keys=("dbName", "databaseName", "name", "value"))
        )

    def list_tables(self, database_name: str) -> list[TableInfo]:
        encoded_database_name = parse.quote(database_name)
        endpoint = f"{self._service.urlUtil.getListTablesUrl()}?dbName={encoded_database_name}"
        response = self._get(endpoint, include_belong=True)
        raw_data = response.get("data", [])
        return self._normalize_table_list(raw_data)

    def _normalize_table_list(self, values: Any) -> list[TableInfo]:
        if not isinstance(values, list):
            return []

        table_infos: list[TableInfo] = []
        seen_codes: set[str] = set()

        for item in values:
            if isinstance(item, str):
                continue

            if not isinstance(item, dict):
                continue

            table_code = None
            table_name = None

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
                    if raw_value is not None:
                        text = str(raw_value).strip()
                        if text:
                            if key.lower().find("code") >= 0 or key.lower().find("en") >= 0:
                                table_code = text
                            elif key.lower().find("name") >= 0 or key.lower().find("cn") >= 0:
                                table_name = text

            if table_code and table_code not in seen_codes:
                seen_codes.add(table_code)
                table_infos.append(TableInfo(
                    table_code=table_code,
                    table_name=table_name or table_code,
                ))

        return table_infos

    def list_fields(self, table_name: str) -> list[str]:
        encoded_table_name = parse.quote(table_name)
        endpoint = f"{self._service.urlUtil.getListFieldsUrl()}?table={encoded_table_name}"
        response = self._get(endpoint, include_belong=True)
        return self._deduplicate(
            self._normalize_name_list(
                response.get("data"),
                dict_name_keys=("field", "fieldName", "name", "columnName", "column", "value"),
            )
        )

    def preview(self, table_name: str) -> list[dict[str, Any]]:
        payload = {"table": table_name}
        response = self._post(self._service.urlUtil.getPreviewUrl(), payload)
        data = response.get("data", {})
        preview_rows = data.get("previewDatas", []) if isinstance(data, dict) else []
        return [row for row in preview_rows if isinstance(row, dict)]

    def query_count(
        self,
        table_name: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        payload = self._build_query_payload(table_name, columns, condition, start_date, end_date)
        response = self._post(self._service.urlUtil.getQueryCountUrl(), payload)
        count_value = response.get("data", 0)
        try:
            return int(count_value)
        except (TypeError, ValueError) as error:
            raise CsmarMcpError(
                "upstream_error",
                f"Unexpected count result type from CSMAR: {count_value!r}",
            ) from error

    def query_sample(
        self,
        table_name: str,
        columns: list[str],
        sample_rows: int,
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, Any]]:
        if sample_rows <= 0:
            return []

        limited_condition = self._append_limit_clause(condition, sample_rows)
        payload = self._build_query_payload(table_name, columns, limited_condition, start_date, end_date)
        response = self._post(self._service.urlUtil.getQueryUrl(), payload)
        data = response.get("data", {})
        rows = data.get("previewDatas", []) if isinstance(data, dict) else []
        normalized_rows = [row for row in rows if isinstance(row, dict)]
        return normalized_rows[:sample_rows]

    def materialize_download(self, plan: DownloadPlan, output_dir: str, max_retries: int = 2) -> DownloadArtifact:
        cache_key = self.build_download_cache_key(plan)
        cached_artifact = self.get_cached_download(cache_key)
        if cached_artifact:
            cached_artifact.status = "cached"
            cached_artifact.retry_count = 0
            return cached_artifact

        last_error: CsmarMcpError | None = None
        for attempt in range(max(0, max_retries) + 1):
            try:
                artifact = self._materialize_download_once(plan, output_dir)
                artifact.retry_count = attempt
                self.set_cached_download(cache_key, artifact)
                return artifact
            except CsmarMcpError as error:
                last_error = error
                if error.error_code == "rate_limited":
                    cached_artifact = self.get_cached_download(cache_key)
                    if cached_artifact:
                        cached_artifact.status = "cached"
                        cached_artifact.retry_count = attempt
                        return cached_artifact
                    raise

                if attempt >= max_retries:
                    break

                time.sleep(min(2 * (attempt + 1), 8))

        if not last_error:
            raise CsmarMcpError("download_failed", "Unknown download error")
        raise last_error

    def _materialize_download_once(self, plan: DownloadPlan, output_dir: str) -> DownloadArtifact:
        payload = self._build_query_payload(
            table_name=plan.table_name,
            columns=plan.columns,
            condition=plan.condition,
            start_date=plan.start_date,
            end_date=plan.end_date,
        )

        pack_response = self._post(self._service.urlUtil.getPackUrl(), payload)
        sign_code = str(pack_response.get("data", "")).strip()
        if not sign_code:
            raise CsmarMcpError("download_failed", "Pack request succeeded but no signCode was returned")

        pack_result = self._poll_pack_result(sign_code)
        file_url = str(pack_result.get("filePath", "")).strip()
        if not file_url:
            raise CsmarMcpError("download_failed", "Pack result does not include filePath")

        resolved_output_dir = Path(output_dir).expanduser().resolve()
        resolved_output_dir.mkdir(parents=True, exist_ok=True)

        sanitized_download_id = self._sanitize_path_fragment(plan.download_id)
        zip_path = (resolved_output_dir / f"{sanitized_download_id}_{sign_code}.zip").resolve()
        extract_dir = (resolved_output_dir / f"{sanitized_download_id}_{sign_code}").resolve()

        http_response = self._http.request("GET", file_url)
        if http_response.status >= 400:
            raise CsmarMcpError(
                "download_failed",
                f"Failed to download packaged file from CSMAR: HTTP {http_response.status}",
            )

        zip_path.write_bytes(http_response.data)

        try:
            with zipfile.ZipFile(zip_path) as zip_file:
                zip_file.extractall(path=extract_dir)
        except Exception as error:
            raise CsmarMcpError(
                "unzip_failed",
                f"Failed to extract archive {zip_path}: {error}",
            ) from error

        extracted_files = sorted(str(path.resolve()) for path in extract_dir.rglob("*") if path.is_file())

        return DownloadArtifact(
            download_id=plan.download_id,
            table_name=plan.table_name,
            status="success",
            zip_path=str(zip_path),
            extract_dir=str(extract_dir),
            files=extracted_files,
            retry_count=0,
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
                    "CSMAR reported packaging failure. Check columns, condition, and purchase permissions.",
                )
            if time.monotonic() >= deadline:
                raise CsmarMcpError(
                    "download_failed",
                    f"Timed out waiting for packaged file after {self._poll_timeout_seconds} seconds.",
                )

            time.sleep(self._poll_interval_seconds)

    def _build_query_payload(
        self,
        table_name: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "columns": columns,
            "condition": self._normalize_condition(condition),
            "table": table_name,
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

    def _build_cache_key(
        self,
        table_name: str,
        columns: list[str],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> str:
        normalized_condition = self._normalize_condition(condition)
        normalized_columns = ",".join(columns)
        return "|".join(
            [
                table_name.strip(),
                normalized_columns,
                normalized_condition,
                (start_date or "").strip(),
                (end_date or "").strip(),
            ]
        )

    def _normalize_condition(self, condition: str | None) -> str:
        normalized = (condition or "").strip()
        return normalized if normalized else "1=1"

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
        self._ensure_login()
        response = requester()

        if self._is_auth_error(response):
            with self._lock:
                self._login()
            response = requester()

        if response.get("code") != 0:
            raise self._to_error(response)
        return response

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
            raise CsmarMcpError("auth_failed", "Login succeeded but no token was returned by CSMAR")

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
            raise CsmarMcpError("auth_failed", "Unable to read token after login")

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
        message = str(response.get("msg") or "Unknown upstream error from CSMAR")
        lowered_message = message.lower()

        if upstream_code == -3004 or "offline" in lowered_message or "login" in lowered_message:
            error_code = "auth_failed"
        elif any(token in lowered_message for token in ("purchase", "permission", "authorized")):
            error_code = "not_purchased"
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

        return CsmarMcpError(error_code=error_code, message=message, upstream_code=upstream_code)

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
        if not entry:
            return None

        if self._now() - entry.created_at > self._cache_ttl:
            cache.pop(key, None)
            return None

        return entry.value

    def _artifact_exists(self, artifact: DownloadArtifact) -> bool:
        if not artifact.zip_path or not artifact.extract_dir:
            return False

        zip_path = Path(artifact.zip_path)
        extract_dir = Path(artifact.extract_dir)

        if not zip_path.exists() or not extract_dir.exists():
            return False

        return all(Path(file_path).exists() for file_path in artifact.files)

    def _sanitize_path_fragment(self, value: str) -> str:
        sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
        return sanitized or "plan"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
