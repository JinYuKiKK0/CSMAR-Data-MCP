from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Sequence

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, ToolAnnotations
from pydantic import ValidationError

from .client import CsmarClient
from .core.errors import CsmarError
from .models import (
    GetTableSchemaInput,
    ListDatabasesOutput,
    ListTablesInput,
    ListTablesOutput,
    MaterializeQueryInput,
    ProbeQueryInput,
    SearchFieldsInput,
    SearchFieldsOutput,
    SearchTablesInput,
    SearchTablesOutput,
    TableListItem,
)
from .presenters import enrich_error, failure, invalid_arguments, success, tool_error_boundary
from .runtime import configure_runtime, get_client, parse_runtime_settings


mcp = FastMCP(
    name="csmar_mcp",
    instructions=(
        "CSMAR MCP for StataAgent workflows. Use csmar_list_databases and csmar_list_tables for deterministic "
        "enumeration, csmar_search_tables (max 5 candidates) to narrow scope, csmar_get_table_schema for precise "
        "schema inspection, csmar_search_fields only as deterministic field lookup, csmar_probe_query for "
        "feasibility validation, and csmar_materialize_query only after "
        "probe success. Tools return concise structured JSON and repair hints on failure."
    ),
    json_response=True,
)


logger = logging.getLogger(__name__)


def _client() -> CsmarClient:
    return get_client()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_log_trace(
    client: CsmarClient,
    *,
    tool_name: str,
    request_payload: dict[str, object],
    started_at: datetime,
    result_summary: dict[str, object] | None,
    cached: bool,
    query_fingerprint: str | None = None,
    validation_id: str | None = None,
    error: CsmarError | None = None,
    error_payload: dict[str, object] | None = None,
) -> None:
    normalized_error = error_payload
    upstream_code: int | None = None
    raw_message: str | None = None
    if error is not None:
        normalized_error = {
            "code": error.error_code,
            "message": error.message,
            "hint": error.hint,
        }
        upstream_code = error.upstream_code
        raw_message = error.raw_message

    try:
        client.log_tool_trace(
            tool_name=tool_name,
            request_payload=request_payload,
            result_summary=result_summary,
            error=normalized_error,
            query_fingerprint=query_fingerprint,
            validation_id=validation_id,
            cached=cached,
            started_at=started_at,
            completed_at=_now_utc(),
            upstream_code=upstream_code,
            raw_message=raw_message,
        )
    except Exception as error:
        logger.warning("Tool trace logging failed for %s: %s", tool_name, error)


def _audit_unexpected_tool_error(
    tool_name: str,
    request_payload: dict[str, object],
    error: Exception,
) -> None:
    try:
        client = _client()
    except Exception as error:
        logger.warning("Unable to initialize client for audit trace in %s: %s", tool_name, error)
        return

    now = _now_utc()
    _safe_log_trace(
        client,
        tool_name=tool_name,
        request_payload=request_payload,
        started_at=now,
        result_summary=None,
        cached=False,
        error_payload={
            "code": "upstream_error",
            "message": str(error) or f"Unhandled internal error in {tool_name}.",
            "hint": "Retry the same tool once. If it still fails, inspect MCP server logs.",
        },
    )


def _log_invalid_arguments_trace(
    *,
    tool_name: str,
    request_payload: dict[str, object],
    started_at: datetime,
) -> None:
    try:
        client = _client()
    except Exception as error:
        logger.warning("Unable to initialize client for invalid-arguments trace in %s: %s", tool_name, error)
        return

    _safe_log_trace(
        client,
        tool_name=tool_name,
        request_payload=request_payload,
        started_at=started_at,
        result_summary=None,
        cached=False,
        error_payload={
            "code": "invalid_arguments",
            "message": "The tool arguments are invalid.",
        },
    )


@mcp.tool(
    name="csmar_list_databases",
    description="List all purchased databases.",
    annotations=ToolAnnotations(
        title="List Databases",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_list_databases", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_list_databases() -> CallToolResult:
    client = _client()
    started_at = _now_utc()
    request_payload: dict[str, object] = {}
    cached = client.has_cached_entry("databases", "all")
    try:
        result = ListDatabasesOutput(databases=client.list_databases())
        _safe_log_trace(
            client,
            tool_name="csmar_list_databases",
            request_payload=request_payload,
            started_at=started_at,
            result_summary={"count": len(result.databases)},
            cached=cached,
        )
        return success(result.as_dict(), f"Returned {len(result.databases)} purchased databases.")
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_list_databases",
            request_payload=request_payload,
            started_at=started_at,
            result_summary=None,
            cached=cached,
            error=error,
        )
        return failure(enrich_error(client, error))


@mcp.tool(
    name="csmar_list_tables",
    description=(
        "List all tables in a purchased database. Always copy database_name verbatim from csmar_list_databases."
    ),
    annotations=ToolAnnotations(
        title="List Tables",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_list_tables", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_list_tables(database_name: str) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {"database_name": database_name}
    try:
        params = ListTablesInput.model_validate({"database_name": database_name})
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_list_tables",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    cached = client.has_cached_entry("tables", params.database_name.strip())
    try:
        records = client.list_tables(params.database_name)
        result = ListTablesOutput(
            items=[TableListItem(table_code=record.table_code, table_name=record.table_name) for record in records],
        )
        _safe_log_trace(
            client,
            tool_name="csmar_list_tables",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={"count": len(result.items)},
            cached=cached,
        )
        return success(result.as_dict(), f"Returned {len(result.items)} tables from {params.database_name}.")
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_list_tables",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            error=error,
        )
        return failure(enrich_error(client, error, database_name=params.database_name))


@mcp.tool(
    name="csmar_search_tables",
    description=(
        "Search table candidates by business topic, table code, or table name. If database_name is provided, copy it "
        "verbatim from csmar_list_databases."
    ),
    annotations=ToolAnnotations(
        title="Search Tables",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_search_tables", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_search_tables(query: str, database_name: str | None = None, limit: int = 5) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {"query": query, "database_name": database_name, "limit": limit}
    try:
        params = SearchTablesInput.model_validate(
            {"query": query, "database_name": database_name, "limit": limit}
        )
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_search_tables",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    cached = bool(
        params.database_name and client.has_cached_entry("tables", params.database_name.strip())
    )
    try:
        result = SearchTablesOutput(
            items=client.search_tables(params.query, database_name=params.database_name, limit=params.limit)
        )
        _safe_log_trace(
            client,
            tool_name="csmar_search_tables",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={"count": len(result.items)},
            cached=cached,
        )
        return success(result.as_dict(), f"Returned {len(result.items)} matching tables.")
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_search_tables",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            error=error,
        )
        return failure(enrich_error(client, error, database_name=params.database_name))


@mcp.tool(
    name="csmar_search_fields",
    description=(
        "Search field candidates by deterministic literal/similarity matching and optional scope filters. "
        "Use role_hint/frequency_hint as ranking bias only."
    ),
    annotations=ToolAnnotations(
        title="Search Fields",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_search_fields", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_search_fields(
    query: str,
    database_name: str | None = None,
    table_code: str | None = None,
    role_hint: str | None = None,
    frequency_hint: str | None = None,
    limit: int = 20,
) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {
        "query": query,
        "database_name": database_name,
        "table_code": table_code,
        "role_hint": role_hint,
        "frequency_hint": frequency_hint,
        "limit": limit,
    }
    try:
        params = SearchFieldsInput.model_validate(
            {
                "query": query,
                "database_name": database_name,
                "table_code": table_code,
                "role_hint": role_hint,
                "frequency_hint": frequency_hint,
                "limit": limit,
            }
        )
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_search_fields",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    cached = bool(params.table_code and client.has_cached_entry("schema", params.table_code.strip()))
    try:
        result = SearchFieldsOutput(
            items=client.search_fields(
                query=params.query,
                database_name=params.database_name,
                table_code=params.table_code,
                role_hint=params.role_hint,
                frequency_hint=params.frequency_hint,
                limit=params.limit,
            )
        )
        _safe_log_trace(
            client,
            tool_name="csmar_search_fields",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={"count": len(result.items)},
            cached=cached,
        )
        return success(result.as_dict(), f"Returned {len(result.items)} matching fields.")
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_search_fields",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            error=error,
        )
        return failure(
            enrich_error(
                client,
                error,
                database_name=params.database_name,
                table_code=params.table_code,
            )
        )


@mcp.tool(
    name="csmar_get_table_schema",
    description="Return canonical schema for a table code. This interface is schema-only and returns no preview rows.",
    annotations=ToolAnnotations(
        title="Get Table Schema",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_get_table_schema", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_get_table_schema(table_code: str) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {"table_code": table_code}
    try:
        params = GetTableSchemaInput.model_validate({"table_code": table_code})
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_get_table_schema",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    cached = client.has_cached_entry("schema", params.table_code.strip())
    try:
        result = client.read_table_schema(params.table_code)
        _safe_log_trace(
            client,
            tool_name="csmar_get_table_schema",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={"field_count": len(result.fields)},
            cached=cached,
        )
        return success(result.as_dict(), f"Returned schema for {params.table_code}.")
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_get_table_schema",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            error=error,
        )
        return failure(enrich_error(client, error, table_code=params.table_code))


@mcp.tool(
    name="csmar_probe_query",
    description=(
        "Probe a query before materialization. Returns validation_id, query_fingerprint, row_count, sample_rows, "
        "invalid_columns, and can_materialize."
    ),
    annotations=ToolAnnotations(
        title="Probe Query",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_probe_query", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_probe_query(
    table_code: str,
    columns: list[str],
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sample_rows: int = 3,
) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {
        "table_code": table_code,
        "columns": columns,
        "condition": condition,
        "start_date": start_date,
        "end_date": end_date,
        "sample_rows": sample_rows,
    }
    try:
        params = ProbeQueryInput.model_validate(
            {
                "table_code": table_code,
                "columns": columns,
                "condition": condition,
                "start_date": start_date,
                "end_date": end_date,
                "sample_rows": sample_rows,
            }
        )
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_probe_query",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    cache_key = client.build_cache_key(
        table_code=params.table_code,
        columns=params.columns,
        condition=params.condition,
        start_date=params.start_date,
        end_date=params.end_date,
    )
    query_fingerprint = client.build_query_fingerprint(
        table_code=params.table_code,
        columns=params.columns,
        condition=params.condition,
        start_date=params.start_date,
        end_date=params.end_date,
    )
    cached = client.has_cached_probe(cache_key)
    try:
        result = client.probe_query(params)
        if result.invalid_columns:
            summary = f"Probe completed for {params.table_code} with invalid columns; materialization is blocked."
        elif not result.can_materialize:
            summary = f"Probe completed for {params.table_code} with zero rows; materialization is blocked."
        else:
            summary = (
                f"Probe completed for {params.table_code}: {result.row_count} rows, "
                f"validation_id={result.validation_id}."
            )
        _safe_log_trace(
            client,
            tool_name="csmar_probe_query",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={
                "row_count": result.row_count,
                "can_materialize": result.can_materialize,
                "invalid_columns_count": len(result.invalid_columns or []),
            },
            cached=cached,
            query_fingerprint=result.query_fingerprint,
            validation_id=result.validation_id,
        )
        return success(result.as_dict(), summary)
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_probe_query",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            query_fingerprint=query_fingerprint,
            error=error,
        )
        return failure(
            enrich_error(
                client,
                error,
                table_code=params.table_code,
                columns=params.columns,
                condition=params.condition,
            )
        )


@mcp.tool(
    name="csmar_materialize_query",
    description="Materialize a validated query by validation_id into local files under output_dir.",
    annotations=ToolAnnotations(
        title="Materialize Query",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_materialize_query", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_materialize_query(validation_id: str, output_dir: str) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {
        "validation_id": validation_id,
        "output_dir": output_dir,
    }
    try:
        params = MaterializeQueryInput.model_validate(
            {
                "validation_id": validation_id,
                "output_dir": output_dir,
            }
        )
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_materialize_query",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    record = client.get_validation_record(params.validation_id)
    cached = False
    if record is not None:
        cached = client.has_cached_download(record.query_fingerprint, params.output_dir)
    try:
        result = client.materialize_query(params.validation_id, params.output_dir)
        _safe_log_trace(
            client,
            tool_name="csmar_materialize_query",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={
                "download_id": result.download_id,
                "file_count": len(result.files),
                "row_count": result.row_count,
            },
            cached=cached,
            query_fingerprint=result.query_fingerprint,
            validation_id=params.validation_id,
        )
        return success(
            result.as_dict(),
            (
                f"Materialized query {result.query_fingerprint} into {len(result.files)} files "
                f"(download_id={result.download_id})."
            ),
        )
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_materialize_query",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=cached,
            query_fingerprint=record.query_fingerprint if record is not None else None,
            validation_id=params.validation_id,
            error=error,
        )
        return failure(
            enrich_error(
                client,
                error,
                table_code=record.table_code if record is not None else None,
                columns=list(record.columns) if record is not None else None,
                condition=record.condition if record is not None else None,
                validation_id=params.validation_id,
            )
        )


def main(argv: Sequence[str] | None = None) -> None:
    settings = parse_runtime_settings(argv)
    configure_runtime(settings)
    mcp.run()


if __name__ == "__main__":
    main()
