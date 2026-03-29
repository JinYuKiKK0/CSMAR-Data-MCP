from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Sequence

from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult, PromptMessage, TextContent, ToolAnnotations
from pydantic import ValidationError

from .client import CsmarClient, CsmarMcpError
from .models import (
    CatalogSearchInput,
    CatalogSearchOutput,
    DownloadMaterializeInput,
    GetTableSchemaInput,
    ListDatabasesOutput,
    ListTablesInput,
    ListTablesOutput,
    QueryValidateInput,
    TableSchemaOutput,
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
            "Run the Lean V2 CSMAR MCP server over stdio. Only account and password are "
            "accepted as runtime args; other settings are fixed in code."
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
            "Runtime configuration is missing. Start the server with required CLI args, "
            "for example: --account ... --password ..."
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


def _json_text(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


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


def _enrich_error(
    client: CsmarClient,
    error: CsmarMcpError,
    *,
    table_code: str | None = None,
    columns: list[str] | None = None,
    database_name: str | None = None,
    condition: str | None = None,
) -> ToolError:
    candidate_values = list(error.candidate_values) if error.candidate_values else None
    suggested_args_patch = dict(error.suggested_args_patch) if error.suggested_args_patch else None
    hint = error.hint or "Review the arguments and retry."

    if error.error_code == "table_not_found" and table_code:
        candidate_values = client.suggest_tables(table_code, database_name=database_name) or None
        hint = "Use one of the suggested table codes, then retry."
    elif error.error_code == "field_not_found" and table_code and columns:
        candidate_values = candidate_values or client.suggest_fields(table_code, columns) or None
        hint = "Use csmar_get_table_schema to inspect the field list, then retry with valid columns."
    elif error.error_code == "invalid_condition":
        hint = "Fix the condition syntax and retry. Example: use '=' instead of '=='."
        if suggested_args_patch is None and condition:
            local_issue = _local_condition_error(condition)
            if local_issue is not None:
                suggested_args_patch = local_issue.suggested_args_patch
                hint = local_issue.hint or hint

    return ToolError(
        code=error.error_code,
        message=error.message,
        hint=hint,
        retry_after_seconds=error.retry_after_seconds,
        candidate_values=candidate_values,
        suggested_args_patch=suggested_args_patch,
    )


def _filter_fields(fields: list[str], field_query: str | None) -> list[str]:
    if field_query is None:
        return fields
    needle = field_query.lower()
    return [field for field in fields if needle in field.lower()]


def _filter_preview_rows(rows: list[dict[str, Any]], preview_columns: list[str]) -> list[dict[str, Any]]:
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        filtered_rows.append({column: row.get(column) for column in preview_columns})
    return filtered_rows


mcp = FastMCP(
    name="csmar_mcp",
    instructions=(
        "Lean CSMAR MCP for agent workflows. Use csmar_list_databases and csmar_list_tables for "
        "exploration, csmar_catalog_search for targeted lookup, csmar_get_table_schema to inspect "
        "fields, csmar_query_validate before downloading, and csmar_download_materialize only after "
        "validation succeeds. Tools return concise structured JSON and short repair hints on failure."
    ),
    json_response=True,
)


@mcp.tool(
    name="csmar_list_databases",
    description="List all purchased databases. Use this first when the user wants to explore what data is available.",
    annotations=ToolAnnotations(
        title="List Databases",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_list_databases() -> CallToolResult:
    client = get_client()
    try:
        result = ListDatabasesOutput(databases=client.list_databases())
        return _success(result.as_dict(), f"Returned {len(result.databases)} purchased databases.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error))


@mcp.tool(
    name="csmar_list_tables",
    description="List all tables in a purchased database. Use this after csmar_list_databases when exploring available data.",
    annotations=ToolAnnotations(
        title="List Tables",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_list_tables(database_name: str) -> CallToolResult:
    try:
        params = ListTablesInput.model_validate({"database_name": database_name})
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        records = client.list_tables(params.database_name)
        result = ListTablesOutput(
            database_name=params.database_name,
            items=[TableListItem(table_code=record.table_code, table_name=record.table_name) for record in records],
        )
        return _success(result.as_dict(), f"Returned {len(result.items)} tables from {params.database_name}.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, database_name=params.database_name))


@mcp.tool(
    name="csmar_catalog_search",
    description="Find candidate CSMAR tables by business topic, table code, or table name. Use this first when you do not already know the table_code.",
    annotations=ToolAnnotations(
        title="Search CSMAR Catalog",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_catalog_search(query: str, database_name: str | None = None, limit: int = 10) -> CallToolResult:
    try:
        params = CatalogSearchInput.model_validate(
            {"query": query, "database_name": database_name, "limit": limit}
        )
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        result = CatalogSearchOutput(items=client.search_catalog(params.query, params.database_name, params.limit))
        return _success(result.as_dict(), f"Returned {len(result.items)} matching tables.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, database_name=params.database_name))


@mcp.tool(
    name="csmar_get_table_schema",
    description="Inspect a table schema and optionally fetch a tiny preview. Use this after catalog search and before validate or download.",
    annotations=ToolAnnotations(
        title="Get Table Schema",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_get_table_schema(
    table_code: str,
    field_query: str | None = None,
    preview_columns: list[str] | None = None,
    preview_rows: int = 0,
) -> CallToolResult:
    try:
        params = GetTableSchemaInput.model_validate(
            {
                "table_code": table_code,
                "field_query": field_query,
                "preview_columns": preview_columns,
                "preview_rows": preview_rows,
            }
        )
    except ValidationError as error:
        return _invalid_arguments(error)

    client = get_client()
    try:
        schema = client.read_table_schema(params.table_code)
        filtered_fields = _filter_fields(schema.fields, params.field_query)
        payload = TableSchemaOutput(
            table_code=params.table_code,
            field_count=len(filtered_fields),
            fields=filtered_fields,
        )

        if params.preview_rows > 0 and params.preview_columns:
            preview = client.preview(params.table_code)[: params.preview_rows]
            payload.preview_rows = _filter_preview_rows(preview, params.preview_columns)

        return _success(payload.as_dict(), f"Returned {payload.field_count} fields for {params.table_code}.")
    except CsmarMcpError as error:
        return _failure(_enrich_error(client, error, table_code=params.table_code))


@mcp.tool(
    name="csmar_query_validate",
    description="Validate a query before download. Use this to confirm fields, condition syntax, row count, and a tiny sample.",
    annotations=ToolAnnotations(
        title="Validate Query",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_query_validate(
    table_code: str,
    columns: list[str],
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sample_rows: int = 3,
) -> CallToolResult:
    try:
        params = QueryValidateInput.model_validate(
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
        cached = client.get_cached_validation(cache_key)
        if cached is not None:
            return _success(cached.as_dict(), f"Returned cached validation for {params.table_code}.")
        return _failure(_rate_limited_error(remaining_seconds))

    try:
        result = client.validate_query(params)
        return _success(result.as_dict(), f"Validated query for {params.table_code}: {result.row_count} matching rows.")
    except CsmarMcpError as error:
        if error.error_code == "rate_limited":
            client.mark_rate_limited(cache_key)
            cached = client.get_cached_validation(cache_key)
            if cached is not None:
                return _success(cached.as_dict(), f"Returned cached validation for {params.table_code}.")
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
    name="csmar_download_materialize",
    description="Download a validated query to local files. Use this only after csmar_query_validate succeeds.",
    annotations=ToolAnnotations(
        title="Download And Materialize",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def csmar_download_materialize(
    table_code: str,
    columns: list[str],
    output_dir: str,
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    download_id: str | None = None,
) -> CallToolResult:
    try:
        payload: dict[str, Any] = {
            "table_code": table_code,
            "columns": columns,
            "output_dir": output_dir,
            "condition": condition,
            "start_date": start_date,
            "end_date": end_date,
        }
        if download_id is not None:
            payload["download_id"] = download_id
        params = DownloadMaterializeInput.model_validate(payload)
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
        cached = client.get_cached_manifest(cache_key)
        if cached is not None:
            output = client.materialize_download(params, max_retries=0)
            return _success(output.as_dict(), f"Returned cached download for {params.table_code}.")
        return _failure(_rate_limited_error(remaining_seconds))

    try:
        available_fields = set(client.list_fields(params.table_code))
        missing_columns = [column for column in params.columns if column not in available_fields]
        if missing_columns:
            return _failure(
                ToolError(
                    code="field_not_found",
                    message="One or more requested columns do not exist in the table.",
                    hint="Use csmar_get_table_schema to inspect the field list, then retry with valid columns.",
                    candidate_values=client.suggest_fields(params.table_code, missing_columns) or None,
                )
            )

        result = client.materialize_download(params)
        return _success(result.as_dict(), f"Materialized download {result.download_id} with {result.file_count} files.")
    except CsmarMcpError as error:
        if error.error_code == "rate_limited":
            client.mark_rate_limited(cache_key)
            cached = client.get_cached_manifest(cache_key)
            if cached is not None:
                output = client.materialize_download(params, max_retries=0)
                return _success(output.as_dict(), f"Returned cached download for {params.table_code}.")
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


@mcp.resource(
    "csmar://table/{table_code}/schema",
    name="csmar_table_schema_resource",
    description="Read the full field list for a table after you already know the table_code.",
    mime_type="application/json",
)
def csmar_table_schema_resource(table_code: str) -> str:
    client = get_client()
    schema = client.read_table_schema(table_code)
    return _json_text(schema.as_dict())


@mcp.resource(
    "csmar://artifacts/{download_id}/manifest",
    name="csmar_artifact_manifest_resource",
    description="Read the full extracted file list for a completed download.",
    mime_type="application/json",
)
def csmar_artifact_manifest_resource(download_id: str) -> str:
    client = get_client()
    manifest = client.get_download_manifest(download_id)
    if manifest is None:
        raise RuntimeError("Unknown download_id. Call csmar_download_materialize first.")
    return _json_text(manifest.as_dict())


@mcp.prompt(
    name="repair_csmar_request",
    description="Use this prompt after a CSMAR tool error to produce a concrete retry plan.",
)
def repair_csmar_request(
    error_code: str,
    tool_name: str,
    last_arguments: str | None = None,
    message: str | None = None,
) -> list[PromptMessage]:
    parts = [
        f"You are repairing a failed call to `{tool_name}`.",
        f"Error code: `{error_code}`.",
    ]
    if message:
        parts.append(f"Short error message: {message}")
    if last_arguments:
        parts.append(f"Last arguments: {last_arguments}")
    parts.append(
        "Produce a concise retry plan with: 1) root cause guess, 2) minimal argument changes, 3) the exact next tool call."
    )
    return [PromptMessage(role="user", content=_text("\n".join(parts)))]


def main(argv: Sequence[str] | None = None) -> None:
    settings = _parse_runtime_settings(argv)
    _configure_runtime(settings)
    mcp.run()


if __name__ == "__main__":
    main()
