from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from mcp.types import CallToolResult, TextContent
from pydantic import ValidationError

from .client import CsmarClient
from .core.errors import CsmarError
from .models import ToolError


def text(value: str) -> TextContent:
    return TextContent(type="text", text=value)


def success(payload: dict[str, Any], summary: str) -> CallToolResult:
    return CallToolResult(content=[text(summary)], structuredContent=payload)


def failure(error: ToolError) -> CallToolResult:
    payload = error.as_dict()
    return CallToolResult(content=[text(f"[{error.code}] {error.message}")], structuredContent=payload, isError=True)


def invalid_arguments(error: ValidationError) -> CallToolResult:
    issues: list[str] = []
    for item in error.errors():
        location = ".".join(str(part) for part in item.get("loc", ()))
        message = item.get("msg", "invalid value")
        issues.append(f"{location}: {message}" if location else message)

    tool_error = ToolError(
        code="invalid_arguments",
        message="The tool arguments are invalid.",
        hint="Fix the invalid fields and retry with the same tool.",
    )
    return failure(tool_error)


def internal_tool_error(tool_name: str) -> ToolError:
    return ToolError(
        code="upstream_error",
        message=f"The MCP server hit an internal error while running {tool_name}.",
        hint="Retry the same tool once. If it fails again, simplify the request or inspect MCP server logs.",
    )


def tool_error_boundary(tool_name: str) -> Callable[[Callable[..., CallToolResult]], Callable[..., CallToolResult]]:
    def decorator(func: Callable[..., CallToolResult]) -> Callable[..., CallToolResult]:
        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> CallToolResult:
            try:
                return func(*args, **kwargs)
            except Exception:
                return failure(internal_tool_error(tool_name))

        return wrapped

    return decorator


def enrich_error(
    client: CsmarClient,
    error: CsmarError,
    *,
    table_code: str | None = None,
    columns: list[str] | None = None,
    database_name: str | None = None,
    condition: str | None = None,
    validation_id: str | None = None,
) -> ToolError:
    suggested_args_patch = dict(error.suggested_args_patch) if error.suggested_args_patch else None
    hint = error.hint or "Review the arguments and retry."

    if error.error_code == "database_not_found":
        hint = "Call csmar_list_databases, copy database_name verbatim, then retry with that value."
    elif error.error_code == "table_not_found":
        hint = "Use csmar_search_tables to find a valid table_code, then retry."
    elif error.error_code == "field_not_found":
        hint = "Use csmar_get_table_schema to inspect fields, then retry with valid columns."
    elif error.error_code == "invalid_condition":
        hint = "Fix condition syntax and retry. Example: use '=' instead of '=='."
        if suggested_args_patch is None and condition:
            local_issue = client.local_condition_error(condition)
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
        suggested_args_patch=suggested_args_patch,
    )
