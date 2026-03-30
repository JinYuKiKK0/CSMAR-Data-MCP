from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timezone
from typing import Any, Callable
from urllib import parse

import urllib3

from csmarapi.CsmarService import CsmarService

from ..core.errors import CsmarError
from ..core.types import CatalogRecord, FieldSchemaRecord


class CsmarGateway:
    def __init__(
        self,
        account: str,
        password: str,
        *,
        lang: str = "0",
        belong: str = "0",
        poll_interval_seconds: int = 3,
        poll_timeout_seconds: int = 900,
    ) -> None:
        self._account = account
        self._password = password
        self._lang = lang if lang in {"0", "1", "2"} else "0"
        self._belong = belong if belong in {"0", "1"} else "0"
        self._poll_interval_seconds = max(1, poll_interval_seconds)
        self._poll_timeout_seconds = max(30, poll_timeout_seconds)

        self._service = CsmarService()
        self._http = urllib3.PoolManager()
        self._lock = threading.RLock()
        self._logged_in = False

    def list_databases(self) -> list[str]:
        response = self._get(self._service.urlUtil.getListDbsUrl(), include_belong=True)
        return self._deduplicate(
            self._normalize_name_list(response.get("data"), dict_name_keys=("dbName", "databaseName", "name", "value"))
        )

    def list_tables(self, database_name: str) -> list[CatalogRecord]:
        encoded_database_name = parse.quote(database_name)
        endpoint = f"{self._service.urlUtil.getListTablesUrl()}?dbName={encoded_database_name}"
        response = self._get(endpoint, include_belong=True)
        return self._normalize_table_list(database_name, response.get("data", []))

    def list_field_schema_items(self, table_code: str) -> list[FieldSchemaRecord]:
        encoded_table_name = parse.quote(table_code)
        endpoint = f"{self._service.urlUtil.getListFieldsUrl()}?table={encoded_table_name}"
        response = self._get(endpoint, include_belong=True)
        return self._normalize_field_schema_list(response.get("data"))

    def query_count(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        payload = self._build_query_payload(table_code, columns, condition, start_date, end_date)
        response = self._post(self._service.urlUtil.getQueryCountUrl(), payload)
        parsed_count = self._extract_first_int(response.get("data", 0))
        if parsed_count is None:
            raise CsmarError(
                "upstream_error",
                "CSMAR returned an unexpected row count.",
                hint="Retry the same request. If it fails again, inspect table schema and simplify conditions.",
                raw_message=repr(response.get("data", 0)),
            )
        return parsed_count

    def query_sample(
        self,
        *,
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
        rows = self._extract_preview_rows(response.get("data", {}))
        return rows[:sample_rows]

    def start_package(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> str:
        payload = self._build_query_payload(table_code, columns, condition, start_date, end_date)
        pack_response = self._post(self._service.urlUtil.getPackUrl(), payload)
        sign_code = str(pack_response.get("data", "")).strip()
        if not sign_code:
            raise CsmarError(
                "download_failed",
                "CSMAR did not return a package identifier.",
                hint="Retry materialization. If it fails again, run csmar_probe_query and retry.",
            )
        return sign_code

    def poll_pack_result(self, sign_code: str) -> tuple[str, datetime]:
        import time

        endpoint = f"{self._service.urlUtil.getPackResultUrl()}/{sign_code}"
        deadline = time.monotonic() + self._poll_timeout_seconds

        while True:
            response = self._get(endpoint, include_belong=True)
            data = response.get("data", {})
            status = str(data.get("status", ""))

            if status == "1":
                file_url = str(data.get("filePath", "")).strip()
                if not file_url:
                    raise CsmarError(
                        "download_failed",
                        "CSMAR finished packaging but did not provide a file URL.",
                        hint="Retry materialization. If the issue persists, narrow the query and probe again.",
                    )
                return file_url, self._now()

            if status == "0":
                raise CsmarError(
                    "download_failed",
                    "CSMAR failed to build the download package.",
                    hint="Check columns and condition, run csmar_probe_query again, then retry materialization.",
                )

            if time.monotonic() >= deadline:
                raise CsmarError(
                    "download_failed",
                    "Timed out while waiting for the packaged archive.",
                    hint="Retry materialization or narrow the query.",
                )

            time.sleep(self._poll_interval_seconds)

    def download_bytes(self, file_url: str) -> bytes:
        http_response = self._http.request("GET", file_url)
        if http_response.status >= 400:
            raise CsmarError(
                "download_failed",
                "CSMAR returned a download URL but the archive could not be fetched.",
                hint="Retry materialization. If it fails again, narrow the query and retry.",
                raw_message=f"HTTP {http_response.status}",
            )
        return bytes(http_response.data)

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
        except CsmarError:
            raise
        except Exception as error:
            raise CsmarError(
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
            raise CsmarError(
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
            raise CsmarError(
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

    def _to_error(self, response: dict[str, Any], fallback_error_code: str = "upstream_error") -> CsmarError:
        upstream_code = response.get("code")
        raw_message = str(response.get("msg") or "Unknown upstream error from CSMAR")
        lowered_message = raw_message.lower()

        if upstream_code == -3004 or "offline" in lowered_message or "login" in lowered_message:
            error_code = "auth_failed"
        elif upstream_code == -3110 or self._is_daily_limit_message(lowered_message):
            error_code = "daily_limit_exceeded"
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

        return CsmarError(
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
            "daily_limit_exceeded": "CSMAR daily query limit has been reached for this account.",
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
            "daily_limit_exceeded": "Wait until tomorrow (UTC+8) for the daily limit to reset, or contact CSMAR support.",
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

    def _is_daily_limit_message(self, lowered_message: str) -> bool:
        daily_limit_tokens = (
            "limit for today",
            "daily limit",
            "reached the limit",
            "query limit",
            "queries has reached",
            "今日查询",
            "已达上限",
            "次数已达",
        )
        return any(token in lowered_message for token in daily_limit_tokens)

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

    def _normalize_field_schema_list(self, values: Any) -> list[FieldSchemaRecord]:
        if not isinstance(values, list):
            return []

        items: list[FieldSchemaRecord] = []
        seen: set[str] = set()
        for raw_item in values:
            if isinstance(raw_item, str):
                field_name = raw_item.strip()
                if not field_name or field_name in seen:
                    continue
                seen.add(field_name)
                items.append(FieldSchemaRecord(field_name=field_name))
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
                preferred_keys=(
                    "fieldLabel",
                    "label",
                    "fieldNameCn",
                    "cnName",
                    "nameCn",
                    "displayName",
                    "title",
                    "caption",
                    "fieldCaption",
                    "alias",
                    "zhName",
                    "chsName",
                    "itemName",
                ),
                token_hints=("label", "title", "caption", "cn", "ch", "zh", "display", "alias", "item"),
            )
            if field_label == field_name:
                field_label = None

            field_description = self._pick_text(
                raw_item,
                preferred_keys=(
                    "fieldDesc",
                    "description",
                    "remark",
                    "comment",
                    "memo",
                    "help",
                    "definition",
                    "explain",
                    "indicatorMeaning",
                    "fieldMeaning",
                ),
                token_hints=("desc", "description", "remark", "comment", "memo", "help", "meaning", "explain"),
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
                FieldSchemaRecord(
                    field_name=field_name,
                    field_label=field_label,
                    field_description=field_description,
                    data_type=data_type,
                    frequency_tags=tuple(frequency_tags) if frequency_tags else None,
                    role_tags=tuple(role_tags) if role_tags else None,
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

    def _extract_first_int(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            matched = re.search(r"-?\d+", value)
            if matched is None:
                return None
            return int(matched.group())
        if isinstance(value, dict):
            for nested_value in value.values():
                parsed = self._extract_first_int(nested_value)
                if parsed is not None:
                    return parsed
            return None
        if isinstance(value, list):
            for nested_value in value:
                parsed = self._extract_first_int(nested_value)
                if parsed is not None:
                    return parsed
        return None

    def _extract_preview_rows(self, value: Any) -> list[dict[str, Any]]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            for key in ("previewDatas", "rows", "items", "list", "data"):
                nested_value = value.get(key)
                rows = self._extract_preview_rows(nested_value)
                if rows:
                    return rows
            for nested_value in value.values():
                rows = self._extract_preview_rows(nested_value)
                if rows:
                    return rows
        return []

    def _deduplicate(self, values: list[str]) -> list[str]:
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)
        return unique_values

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
