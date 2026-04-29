from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, ToolAnnotations
from pydantic import ValidationError

from .client import CsmarClient
from .core.errors import CsmarError
from .models import (
    BulkSchemaInput,
    BulkSchemaItem,
    BulkSchemaOutput,
    GetTableSchemaInput,
    ListDatabasesOutput,
    ListTablesInput,
    ListTablesOutput,
    MaterializeQueryInput,
    ProbeQueryInput,
    RefreshCacheInput,
    RefreshCacheOutput,
    SearchFieldHit,
    SearchFieldInput,
    SearchFieldOutput,
    TableListItem,
    ToolErrorPayload,
)
from .presenters import enrich_error, failure, invalid_arguments, success, tool_error_boundary
from .runtime import configure_runtime, get_client, parse_runtime_settings

mcp = FastMCP(
    name="csmar_mcp",
    instructions=(
        "CSMAR MCP for searching and downloading data from the CSMAR database: metadata discovery + "
        "two-stage query (probe -> materialize). "
        "Metadata discovery is cache-first to conserve CSMAR upstream quota. "
        "Discovery order: (1) csmar_list_databases / csmar_list_tables for deterministic enumeration; "
        "(2) csmar_bulk_schema to fetch schemas for 2+ tables in one call (cache-first, prefer over looping "
        "csmar_get_table_schema); (3) csmar_get_table_schema only for a single targeted table. "
        "Query execution: csmar_probe_query validates feasibility and returns validation_id + can_materialize; "
        "call csmar_materialize_query only after a probe with can_materialize=true, passing the validation_id. "
        "csmar_refresh_cache is a danger tool: invoke ONLY when the user explicitly asks to refresh metadata; "
        "never call pre-emptively or as part of normal exploration. "
        "All tools return concise structured JSON; on failure, follow the hint field to repair arguments and retry."
    ),
)


logger = logging.getLogger(__name__)


def _client() -> CsmarClient:
    return get_client()


def _now_utc() -> datetime:
    return datetime.now(UTC)


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
    except Exception as trace_error:
        logger.warning("Tool trace logging failed for %s: %s", tool_name, trace_error)


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
        logger.warning(
            "Unable to initialize client for invalid-arguments trace in %s: %s", tool_name, error
        )
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
        return success(result.as_dict())
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
            items=[
                TableListItem(
                    table_code=record.table_code,
                    table_name=record.table_name,
                    start_time=record.start_time,
                    end_time=record.end_time,
                )
                for record in records
            ],
        )
        _safe_log_trace(
            client,
            tool_name="csmar_list_tables",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary={"count": len(result.items)},
            cached=cached,
        )
        return success(result.as_dict())
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
        return success(result.as_dict())
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
        "Probe a query before materialization. start_date and end_date (YYYY-MM-DD) are required. "
        "Returns validation_id, query_fingerprint, row_count, sample_rows, invalid_columns, and can_materialize."
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
    start_date: str,
    end_date: str,
    condition: str | None = None,
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
        return success(result.as_dict())
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
        return success(result.as_dict())
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


@mcp.tool(
    name="csmar_bulk_schema",
    description=(
        "Fetch schemas for multiple table_codes in a single call. Cache-first: entries already in the "
        "local metadata cache are returned instantly; only genuine misses hit CSMAR with at most 4 "
        "concurrent upstream calls. Prefer this over repeated csmar_get_table_schema calls whenever "
        "you need 2+ tables."
    ),
    annotations=ToolAnnotations(
        title="Bulk Get Table Schemas",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@tool_error_boundary("csmar_bulk_schema", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_bulk_schema(table_codes: list[str]) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {"table_codes": table_codes}
    try:
        params = BulkSchemaInput.model_validate({"table_codes": table_codes})
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_bulk_schema",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    results = client.bulk_read_schema(list(params.table_codes))

    items: list[BulkSchemaItem] = []
    for code, fields, _source, error in results:
        payload_error: ToolErrorPayload | None = None
        if error is not None:
            payload_error = ToolErrorPayload(
                code=error.error_code,
                message=error.message,
                hint=error.hint,
            )
        items.append(
            BulkSchemaItem(
                table_code=code,
                fields=fields,
                error=payload_error,
            )
        )

    result = BulkSchemaOutput(items=items)
    _safe_log_trace(
        client,
        tool_name="csmar_bulk_schema",
        request_payload=params.as_dict(),
        started_at=started_at,
        result_summary={"item_count": len(items)},
        cached=False,
    )
    return success(result.as_dict())


# @mcp.tool(
#     name="csmar_search_field",
#     description=(
#         "Search for field codes across the LOCAL metadata cache only. Zero CSMAR API calls. "
#         "Matches keyword (case-insensitive substring) against field_code, table_code, and table_name. "
#         "An empty result does NOT mean the field does not exist — it only means the relevant "
#         "table's schema has not been cached yet. In that case, fall back to csmar_list_tables / "
#         "csmar_get_table_schema to populate the cache, then retry."
#     ),
#     annotations=ToolAnnotations(
#         title="Search Field (Local Cache)",
#         readOnlyHint=True,
#         destructiveHint=False,
#         idempotentHint=True,
#         openWorldHint=False,
#     ),
# )
@tool_error_boundary("csmar_search_field", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_search_field(
    keyword: str,
    database: str | None = None,
    limit: int = 50,
) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {
        "keyword": keyword,
        "database": database,
        "limit": limit,
    }
    try:
        params = SearchFieldInput.model_validate(
            {"keyword": keyword, "database": database, "limit": limit}
        )
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_search_field",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    hits = client.search_field_in_cache(params.keyword, params.database, params.limit)
    hint = None
    if not hits:
        hint = (
            "No match in local cache. Run csmar_list_tables / csmar_get_table_schema to populate "
            "the cache, then retry."
        )
    result = SearchFieldOutput(
        results=[SearchFieldHit(**hit) for hit in hits],
        hint=hint,
    )
    _safe_log_trace(
        client,
        tool_name="csmar_search_field",
        request_payload=params.as_dict(),
        started_at=started_at,
        result_summary={"hits": len(hits)},
        cached=True,
    )
    return success(result.as_dict())


@mcp.tool(
    name="csmar_refresh_cache",
    description=(
        "Danger tool. Do NOT call unless the user explicitly asks to refresh the cached metadata "
        "(e.g. they suspect a database/table structure changed or they just purchased new data). "
        "Calling this will force subsequent metadata lookups to hit the rate-limited CSMAR API. "
        "Never invoke pre-emptively, never as part of normal exploration. "
        "namespace must be one of: databases, tables, schema, all. Optional key targets a single entry."
    ),
    annotations=ToolAnnotations(
        title="Refresh Metadata Cache (Danger)",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=False,
    ),
)
@tool_error_boundary("csmar_refresh_cache", on_unexpected_error=_audit_unexpected_tool_error)
def csmar_refresh_cache(namespace: str, key: str | None = None) -> CallToolResult:
    started_at = _now_utc()
    request_payload: dict[str, object] = {"namespace": namespace, "key": key}
    try:
        params = RefreshCacheInput.model_validate({"namespace": namespace, "key": key})
    except ValidationError as error:
        _log_invalid_arguments_trace(
            tool_name="csmar_refresh_cache",
            request_payload=request_payload,
            started_at=started_at,
        )
        return invalid_arguments(error)

    client = _client()
    try:
        cleared = client.refresh_cache(params.namespace, params.key)
    except CsmarError as error:
        _safe_log_trace(
            client,
            tool_name="csmar_refresh_cache",
            request_payload=params.as_dict(),
            started_at=started_at,
            result_summary=None,
            cached=False,
            error=error,
        )
        return failure(enrich_error(client, error))

    result = RefreshCacheOutput(cleared=cleared)
    _safe_log_trace(
        client,
        tool_name="csmar_refresh_cache",
        request_payload=params.as_dict(),
        started_at=started_at,
        result_summary={"total_cleared": sum(cleared.values())},
        cached=False,
    )
    return success(result.as_dict())


def main(argv: Sequence[str] | None = None) -> None:
    settings = parse_runtime_settings(argv)
    configure_runtime(settings)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
