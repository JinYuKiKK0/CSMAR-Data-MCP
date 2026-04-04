from __future__ import annotations

import hashlib
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from ..core.errors import CsmarError
from ..core.types import (
    MaterializeAuditRecord,
    MaterializationResult,
    ProbeResult,
    ProbeSpec,
    ValidationRecord,
)
from ..infra.csmar_gateway import CsmarGateway
from ..infra.state import PersistentState
from .metadata import MetadataService


class QueryService:
    def __init__(self, gateway: CsmarGateway, metadata_service: MetadataService, state: PersistentState) -> None:
        self._gateway = gateway
        self._metadata_service = metadata_service
        self._state = state

    def build_cache_key(
        self,
        *,
        table_code: str,
        columns: list[str] | tuple[str, ...],
        condition: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> str:
        normalized_condition = self._normalize_condition(condition)
        normalized_columns = ",".join(self._normalize_columns(columns))
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
        columns: list[str] | tuple[str, ...],
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

    def build_materialize_cache_key(self, *, query_fingerprint: str, output_dir: str) -> str:
        resolved_output_dir = Path(output_dir).expanduser().resolve()
        return f"{query_fingerprint}|{resolved_output_dir}"

    def get_cached_probe(self, cache_key: str) -> ProbeResult | None:
        return self._state.get_cached("probes", cache_key)

    def get_validation_record(self, validation_id: str) -> ValidationRecord | None:
        return self._state.get_cached("validations", validation_id)

    def get_rate_limit_remaining_seconds(self, cache_key: str) -> int | None:
        return self._state.get_rate_limit_remaining_seconds(cache_key)

    def probe_query(self, request: ProbeSpec) -> ProbeResult:
        local_issue = self.local_condition_error(request.condition)
        if local_issue is not None:
            raise local_issue

        cache_key = self.build_cache_key(
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        remaining_seconds = self._state.get_rate_limit_remaining_seconds(cache_key)
        if remaining_seconds is not None:
            cached = self.get_cached_probe(cache_key)
            if cached is not None:
                return cached
            raise CsmarError(
                "rate_limited",
                "CSMAR is cooling down the same query.",
                hint="Retry after the cooldown expires or change the condition or date range.",
                retry_after_seconds=remaining_seconds,
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
        available_fields = {item.field_name for item in self._metadata_service.list_field_schema_items(request.table_code)}
        invalid_columns = tuple(column for column in request.columns if column not in available_fields)

        validation_id = self._generate_validation_id()
        if invalid_columns:
            result = ProbeResult(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                row_count=0,
                sample_rows=None,
                invalid_columns=invalid_columns,
                can_materialize=False,
            )
            record = ValidationRecord(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                table_code=request.table_code,
                columns=request.columns,
                condition=request.condition,
                start_date=request.start_date,
                end_date=request.end_date,
                row_count=0,
                can_materialize=False,
            )
            self._set_probe_result(cache_key, result, record)
            return result

        try:
            row_count = self._gateway.query_count(
                table_code=request.table_code,
                columns=list(request.columns),
                condition=request.condition,
                start_date=request.start_date,
                end_date=request.end_date,
            )
        except CsmarError as error:
            if error.error_code == "rate_limited":
                self._state.mark_rate_limited(cache_key)
                error.retry_after_seconds = self._state.get_rate_limit_remaining_seconds(cache_key)
            raise

        sample_rows: tuple[dict[str, object], ...] | None = None
        if request.sample_rows > 0 and row_count > 0:
            try:
                preview_rows = self._gateway.query_sample(
                    table_code=request.table_code,
                    columns=list(request.columns),
                    sample_rows=request.sample_rows,
                    condition=request.condition,
                    start_date=request.start_date,
                    end_date=request.end_date,
                )
                sample_rows = tuple(preview_rows) or None
            except CsmarError as error:
                if error.error_code not in {"rate_limited", "upstream_error"}:
                    raise

        can_materialize = row_count > 0
        result = ProbeResult(
            validation_id=validation_id,
            query_fingerprint=query_fingerprint,
            row_count=row_count,
            sample_rows=sample_rows,
            invalid_columns=None,
            can_materialize=can_materialize,
        )

        record = ValidationRecord(
            validation_id=validation_id,
            query_fingerprint=query_fingerprint,
            table_code=request.table_code,
            columns=request.columns,
            condition=request.condition,
            start_date=request.start_date,
            end_date=request.end_date,
            row_count=row_count,
            can_materialize=can_materialize,
        )
        self._set_probe_result(cache_key, result, record)
        return result

    def materialize_query(
        self,
        validation_id: str,
        output_dir: str,
        *,
        max_retries: int = 2,
    ) -> MaterializationResult:
        record = self.get_validation_record(validation_id)
        if record is None:
            raise CsmarError(
                "invalid_arguments",
                "validation_id was not found or has expired.",
                hint="Call csmar_probe_query first, then pass the returned validation_id.",
            )

        if not record.can_materialize:
            raise CsmarError(
                "invalid_arguments",
                "This validation result cannot be materialized.",
                hint="Fix invalid columns or broaden filters, then run csmar_probe_query again.",
            )

        resolved_output_dir = Path(output_dir).expanduser().resolve()
        materialize_cache_key = self.build_materialize_cache_key(
            query_fingerprint=record.query_fingerprint,
            output_dir=str(resolved_output_dir),
        )
        query_cache_key = self.build_cache_key(
            table_code=record.table_code,
            columns=record.columns,
            condition=record.condition,
            start_date=record.start_date,
            end_date=record.end_date,
        )

        remaining_seconds = self._state.get_rate_limit_remaining_seconds(query_cache_key)
        if remaining_seconds is not None:
            cached = self._state.get_cached("downloads", materialize_cache_key)
            if cached is not None and self._materialization_exists(cached):
                return cached
            if cached is not None:
                self._state.delete_cached("downloads", materialize_cache_key)
            raise CsmarError(
                "rate_limited",
                "CSMAR is cooling down the same query.",
                hint="Retry after the cooldown expires or change the condition or date range.",
                retry_after_seconds=remaining_seconds,
            )

        cached = self._state.get_cached("downloads", materialize_cache_key)
        if cached is not None and self._materialization_exists(cached):
            return cached
        if cached is not None:
            self._state.delete_cached("downloads", materialize_cache_key)

        last_error: CsmarError | None = None
        for attempt in range(max(0, max_retries) + 1):
            try:
                output = self._materialize_query_once(record, resolved_output_dir, retries=attempt)
                self._state.set_cached("downloads", materialize_cache_key, output)
                return output
            except CsmarError as error:
                if error.error_code == "rate_limited":
                    self._state.mark_rate_limited(query_cache_key)
                    error.retry_after_seconds = self._state.get_rate_limit_remaining_seconds(
                        query_cache_key
                    )
                    last_error = error
                    break
                last_error = error
                if attempt >= max_retries:
                    break
                time.sleep(min(2 * (attempt + 1), 8))

        if last_error is None:
            raise CsmarError(
                "download_failed",
                "The download did not complete.",
                hint="Retry after running csmar_probe_query for the same query.",
            )
        raise last_error

    def local_condition_error(self, condition: str | None) -> CsmarError | None:
        normalized = (condition or "").strip()
        if not normalized:
            return None

        if "==" in normalized:
            return CsmarError(
                "invalid_condition",
                "The condition uses '==' which CSMAR does not accept.",
                hint="Use '=' instead of '==', then retry.",
                suggested_args_patch={"condition": normalized.replace("==", "=")},
            )
        if any(mark in normalized for mark in ("“", "”", "‘", "’")):
            return CsmarError(
                "invalid_condition",
                "The condition uses smart quotes which CSMAR does not accept.",
                hint="Replace smart quotes with plain ASCII quotes, then retry.",
                suggested_args_patch={"condition": normalized.translate(str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"}))},
            )
        if "；" in normalized:
            return CsmarError(
                "invalid_condition",
                "The condition contains a full-width semicolon which CSMAR does not accept.",
                hint="Remove the full-width semicolon and retry.",
                suggested_args_patch={"condition": normalized.replace("；", "")},
            )
        return None

    def _set_probe_result(self, cache_key: str, result: ProbeResult, record: ValidationRecord) -> None:
        self._state.set_cached("probes", cache_key, result)
        self._state.set_cached("validations", result.validation_id, record)

    def _materialize_query_once(
        self,
        record: ValidationRecord,
        output_dir: Path,
        *,
        retries: int,
    ) -> MaterializationResult:
        sign_code = self._gateway.start_package(
            table_code=record.table_code,
            columns=list(record.columns),
            condition=record.condition,
            start_date=record.start_date,
            end_date=record.end_date,
        )
        file_url, packaged_at = self._gateway.poll_pack_result(sign_code)

        output_dir.mkdir(parents=True, exist_ok=True)

        download_id = self._generate_download_id()
        zip_path = (output_dir / f"{download_id}_{sign_code}.zip").resolve()
        extract_dir = (output_dir / f"{download_id}_{sign_code}").resolve()
        zip_path.write_bytes(self._gateway.download_bytes(file_url))

        try:
            with zipfile.ZipFile(zip_path) as zip_file:
                zip_file.extractall(path=extract_dir)
        except Exception as error:
            raise CsmarError(
                "unzip_failed",
                "The downloaded archive could not be extracted.",
                hint="Retry materialization. If it fails again, clean output_dir and retry.",
                raw_message=str(error),
            ) from error

        extracted_files = tuple(sorted(str(path.resolve()) for path in extract_dir.rglob("*") if path.is_file()))
        completed_at = self._now()

        return MaterializationResult(
            download_id=download_id,
            query_fingerprint=record.query_fingerprint,
            output_dir=str(output_dir),
            files=extracted_files,
            row_count=record.row_count,
            archive_path=str(zip_path),
            audit=MaterializeAuditRecord(
                retries=retries,
                packaged_at=packaged_at,
                completed_at=completed_at,
            ),
        )

    def _materialization_exists(self, output: MaterializationResult) -> bool:
        archive_path = Path(output.archive_path)
        if not archive_path.exists():
            return False
        return all(Path(file_path).exists() for file_path in output.files)

    def _normalize_condition(self, condition: str | None) -> str:
        normalized = (condition or "").strip()
        return normalized if normalized else "1=1"

    def _normalize_columns(self, columns: list[str] | tuple[str, ...]) -> tuple[str, ...]:
        cleaned = {column.strip() for column in columns if column.strip()}
        return tuple(sorted(cleaned))

    def _generate_validation_id(self) -> str:
        return f"validation_{uuid4().hex[:10]}"

    def _generate_download_id(self) -> str:
        return f"download_{uuid4().hex[:10]}"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
