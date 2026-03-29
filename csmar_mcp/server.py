from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache, wraps
from typing import Any, Callable, Sequence

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import ValidationError

from .client import CsmarClient, CsmarMcpError
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
    ToolError,
)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    account: str
    password: str


DEFAULT_LANG = "0"
DEFAULT_BELONG = "0"
DEFAULT_POLL_INTERVAL_SECONDS = 3
DEFAULT_POLL_TIMEOUT_SECONDS = 900
DEFAULT_CACHE_TTL_MINUTES = 30


_runtime_settings: RuntimeSettings | None = None


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csmar-mcp",
        description=(
            "Run the CSMAR MCP server over stdio. Only account and password are accepted as runtime args; "
            "other settings are fixed in code."
        ),
    )
    parser.add_argument("--account", required=True, help="CSMAR account")
    parser.add_argument("--password", required=True, help="CSMAR password")
    return parser


def _parse_runtime_settings(argv: Sequence[str] | None = None) -> RuntimeSettings:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    return RuntimeSettings(account=args.account, password=args.password)


def _configure_runtime(settings: RuntimeSettings) -> None:
    global _runtime_settings
    _runtime_settings = settings


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    if _runtime_settings is None:
        raise RuntimeError(
            "Runtime configuration is missing. Start the server with required CLI args, for example: "
            "--account ... --password ..."
        )
    return _runtime_settings


@lru_cache(maxsize=1)
def get_client() -> CsmarClient:
    settings = get_settings()
    return CsmarClient(
        account=settings.account,
        password=settings.password,
        lang=DEFAULT_LANG,
        belong=DEFAULT_BELONG,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        poll_timeout_seconds=DEFAULT_POLL_TIMEOUT_SECONDS,
        cache_ttl_minutes=DEFAULT_CACHE_TTL_MINUTES,
    )


def _text(text: str) -> TextContent:
    return TextContent(type="text", text=text)


def _success(payload: dict[str, Any], summary: str) -> CallToolResult:
    return CallToolResult(content=[_text(summary)], structuredContent=payload)


def _failure(error: ToolError) -> CallToolResult:
    payload = error.as_dict()
    return CallToolResult(content=[_text(f"[{error.code}] {error.message}")], structuredContent=payload, isError=True)


def _invalid_arguments(error: ValidationError) -> CallToolResult:
    issues: list[str] = []
    for item in error.errors():
        location = ".".join(str(part) for part in item.get("loc", ()))
        message = item.get("msg", "invalid value")
        issues.append(f"{location}: {message}" if location else message)

    tool_error = ToolError(
        code="invalid_arguments",
        message="The tool arguments are invalid.",
        hint="Fix the invalid fields and retry with the same tool.",
        candidate_values=issues[:5] or None,
    )
    return _failure(tool_error)


def _local_condition_error(condition: str) -> CsmarMcpError | None:
    normalized = condition.strip()
    if not normalized:
        return None

    if "==" in normalized:
        return CsmarMcpError(
            "invalid_condition",
            "The condition uses '==' which CSMAR does not accept.",
            hint="Use '=' instead of '==', then retry.",
            suggested_args_patch={"condition": normalized.replace("==", "=")},
        )
    if any(mark in normalized for mark in ("“", "”", "‘", "’")):
        return CsmarMcpError(
            "invalid_condition",
            "The condition uses smart quotes which CSMAR does not accept.",
            hint="Replace smart quotes with plain ASCII quotes, then retry.",
            suggested_args_patch={"condition": normalized.translate(str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"}))},
        )
    if "；" in normalized:
        return CsmarMcpError(
            "invalid_condition",
            "The condition contains a full-width semicolon which CSMAR does not accept.",
            hint="Remove the full-width semicolon and retry.",
            suggested_args_patch={"condition": normalized.replace("；", "")},
        )
    return None


def _rate_limited_error(remaining_seconds: int) -> ToolError:
    return ToolError(
        code="rate_limited",
        message="CSMAR is cooling down the same query.",
        hint="Retry after the cooldown expires or change the condition or date range.",
        retry_after_seconds=remaining_seconds,
    )


def _safe_suggestions(fetcher: Callable[[], list[str]]) -> list[str] | None:
    try:
        suggestions = fetcher()
    except Exception:
        return None
    return suggestions or None


def _internal_tool_error(tool_name: str) -> ToolError:
    return ToolError(
        code="upstream_error",
        message=f"The MCP server hit an internal error while running {tool_name}.",
        hint="Retry the same tool once. If it fails again, simplify the request or inspect MCP server logs.",
    )


def _tool_error_boundary(tool_name: str) -> Callable[[Callable[..., CallToolResult]], Callable[..., CallToolResult]]:
    def decorator(func: Callable[..., CallToolResult]) -> Callable[..., CallToolResult]:
        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> CallToolResult:
            try:
                return func(*args, **kwargs)
            except Exception:
                return _failure(_internal_tool_error(tool_name))

        return wrapped

    return decorator


def _enrich_error(
    client: CsmarClient,
    error: CsmarMcpError,
    *,
    table_code: str | None = None,
    columns: list[str] | None = None,
    database_name: str | None = None,
    condition: str | None = None,
    validation_id: str | None = None,
) -> ToolError:
    candidate_values = list(error.candidate_values) if error.candidate_values else None
    suggested_args_patch = dict(error.suggested_args_patch) if error.suggested_args_patch else None
    hint = error.hint or "Review the arguments and retry."

    if error.error_code == "database_not_found" and database_name:
        candidate_values = candidate_values or _safe_suggestions(lambda: client.suggest_databases(database_name))
        hint = "Call csmar_list_databases, copy database_name verbatim, then retry with that value."
    elif error.error_code == "table_not_found" and table_code:
        candidate_values = candidate_values or _safe_suggestions(
            lambda: client.suggest_tables(table_code, database_name=database_name)
        )
        hint = "Use one of the suggested table codes, then retry."
    elif error.error_code == "field_not_found" and table_code and columns:
        candidate_values = candidate_values or _safe_suggestions(lambda: client.suggest_fields(table_code, columns))
        hint = "Use csmar_get_table_schema to inspect fields, then retry with valid columns."
    elif error.error_code == "invalid_condition":
        hint = "Fix condition syntax and retry. Example: use '=' instead of '=='."
        if suggested_args_patch is None and condition:
            local_issue = _local_condition_error(condition)
            if local_issue is not None:
                suggested_args_patch = local_issue.suggested_args_patch
                hint = local_issue.hint or hint
    elif error.error_code == "invalid_arguments" and validation_id:
        lowered_message = error.message.lower()
        if "not found" in lowered_message or "expired" in lowered_message:
            hint = "Call csmar_probe_query first and pass a valid non-expired validation_id."
        elif "cannot be materialized" in lowered_message:
            hint = "Fix invalid columns or broaden filters, then run csmar_probe_query again."

    return ToolError(
        code=error.error_code,
        message=error.message,
        hint=hint,
        retry_after_seconds=error.retry_after_seconds,
        candidate_values=candidate_values,
        suggested_args_patch=suggested_args_patch,
    )


mcp = FastMCP(
    name="csmar_mcp",
    instructions=(
        "CSMAR MCP for StataAgent workflows. Use csmar_list_databases and csmar_list_tables for deterministic "
        "enumeration, csmar_search_tables and csmar_search_fields for discovery, csmar_get_table_schema for precise "
        "schema inspection, csmar_probe_query for feasibility validation, and csmar_materialize_query only after "
        "probe success. Tools return concise structured JSON and repair hints on failure."
    ),
    json_response=True,
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
@_tool_error_boundary("csmar_list_databases")
def csmar_list_databases() -> CallToolResult:
    client = get_client()
    try:
        result = ListDatabasesOutput(databases=client.list_databases())
        return _success(result.as_dict(), f"Returned {len(result.databases)} purchased databases.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error))


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
@_tool_error_boundary("csmar_list_tables")
def csmar_list_tables(database_name: str) -> CallToolResult:
    try:
        params = ListTablesInput.model_validate({"database_name": database_name})
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        records = client.list_tables(params.database_name)
        result = ListTablesOutput(
            items=[TableListItem(table_code=record.table_code, table_name=record.table_name) for record in records],
        )
        return _success(result.as_dict(), f"Returned {len(result.items)} tables from {params.database_name}.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, database_name=params.database_name))


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
@_tool_error_boundary("csmar_search_tables")
def csmar_search_tables(query: str, database_name: str | None = None, limit: int = 10) -> CallToolResult:
    try:
        params = SearchTablesInput.model_validate(
            {"query": query, "database_name": database_name, "limit": limit}
        )
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        result = SearchTablesOutput(
            items=client.search_tables(params.query, database_name=params.database_name, limit=params.limit)
        )
        return _success(result.as_dict(), f"Returned {len(result.items)} matching tables.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, database_name=params.database_name))


@mcp.tool(
    name="csmar_search_fields",
    description=(
        "Search field candidates by semantic query and optional scope filters. Use role_hint/frequency_hint to bias "
        "results toward variable intent."
    ),
    annotations=ToolAnnotations(
        title="Search Fields",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
@_tool_error_boundary("csmar_search_fields")
def csmar_search_fields(
    query: str,
    database_name: str | None = None,
    table_code: str | None = None,
    role_hint: str | None = None,
    frequency_hint: str | None = None,
    limit: int = 20,
) -> CallToolResult:
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
        return _invalid_arguments(error)

    client = get_client()
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
        return _success(result.as_dict(), f"Returned {len(result.items)} matching fields.")
    except CsmarMcpError as error:
        return _failure(
            _enrich_error(
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
@_tool_error_boundary("csmar_get_table_schema")
def csmar_get_table_schema(table_code: str) -> CallToolResult:
    try:
        params = GetTableSchemaInput.model_validate({"table_code": table_code})
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        result = client.read_table_schema(params.table_code)
        return _success(result.as_dict(), f"Returned schema for {params.table_code}.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, table_code=params.table_code))


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
@_tool_error_boundary("csmar_probe_query")
def csmar_probe_query(
    table_code: str,
    columns: list[str],
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sample_rows: int = 3,
) -> CallToolResult:
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
        return _invalid_arguments(error)

    if params.condition:
        local_issue = _local_condition_error(params.condition)
        if local_issue is not None:
            return _failure(_enrich_error(get_client(), local_issue, condition=params.condition))

    client = get_client()
    cache_key = client.build_cache_key(
        table_code=params.table_code,
        columns=params.columns,
        condition=params.condition,
        start_date=params.start_date,
        end_date=params.end_date,
    )
    remaining_seconds = client.get_rate_limit_remaining_seconds(cache_key)
    if remaining_seconds is not None:
        cached = client.get_cached_probe(cache_key)
        if cached is not None:
            return _success(cached.as_dict(), f"Returned cached probe for {params.table_code}.")
        return _failure(_rate_limited_error(remaining_seconds))

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
        return _success(result.as_dict(), summary)
    except CsmarMcpError as error:
        if error.error_code == "rate_limited":
            client.mark_rate_limited(cache_key)
            cached = client.get_cached_probe(cache_key)
            if cached is not None:
                return _success(cached.as_dict(), f"Returned cached probe for {params.table_code}.")
            error.retry_after_seconds = client.get_rate_limit_remaining_seconds(cache_key)
        return _failure(
            _enrich_error(
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
@_tool_error_boundary("csmar_materialize_query")
def csmar_materialize_query(validation_id: str, output_dir: str) -> CallToolResult:
    try:
        params = MaterializeQueryInput.model_validate(
            {
                "validation_id": validation_id,
                "output_dir": output_dir,
            }
        )
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    record = client.get_validation_record(params.validation_id)
    try:
        result = client.materialize_query(params.validation_id, params.output_dir)
        return _success(
            result.as_dict(),
            (
                f"Materialized query {result.query_fingerprint} into {len(result.files)} files "
                f"(download_id={result.download_id})."
            ),
        )
    except CsmarMcpError as error:
        if error.error_code == "rate_limited":
            cooldown_key = record.query_fingerprint if record is not None else params.validation_id
            client.mark_rate_limited(cooldown_key)
            error.retry_after_seconds = client.get_rate_limit_remaining_seconds(cooldown_key)
        return _failure(
            _enrich_error(
                client,
                error,
                table_code=record.table_code if record is not None else None,
                columns=list(record.columns) if record is not None else None,
                condition=record.condition if record is not None else None,
                validation_id=params.validation_id,
            )
        )


def main(argv: Sequence[str] | None = None) -> None:
    settings = _parse_runtime_settings(argv)
    _configure_runtime(settings)
    mcp.run()


if __name__ == "__main__":
    main()
