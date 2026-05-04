"""External HTTP API for testing CSMAR. Surfaces CSMAR's native error payload
(upstream `code` and `msg`) instead of the MCP-mapped error schema. Functionality
mirrors the MCP tools one-to-one.

Run:
    python -m csmar_mcp.test_api [--host 0.0.0.0] [--port 8000]

Credentials:
    Set CSMAR_MCP_ACCOUNT / CSMAR_MCP_PASSWORD in the environment or a local .env file.
    --account / --password remain supported as a compatibility fallback.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from typing import Any, cast

import uvicorn
from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .client import CsmarClient
from .core.errors import CsmarError
from .models import (
    GetTableSchemaInput,
    ListTablesInput,
    MaterializeQueryInput,
    ProbeQueryInput,
)
from .runtime import (
    ACCOUNT_ENV_VAR,
    DEFAULT_BELONG,
    DEFAULT_CACHE_TTL_MINUTES,
    DEFAULT_LANG,
    DEFAULT_METADATA_TTL_DAYS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_POLL_TIMEOUT_SECONDS,
    DEFAULT_RATE_LIMIT_COOLDOWN_MINUTES,
    MISSING_CREDENTIALS_MESSAGE,
    PASSWORD_ENV_VAR,
    resolve_credentials,
)

_client: CsmarClient | None = None


def _get_client() -> CsmarClient:
    if _client is None:
        raise RuntimeError("CsmarClient not configured. Call configure_test_client first.")
    return _client


def configure_test_client(account: str, password: str) -> None:
    global _client
    _client = CsmarClient(
        account=account,
        password=password,
        lang=DEFAULT_LANG,
        belong=DEFAULT_BELONG,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        poll_timeout_seconds=DEFAULT_POLL_TIMEOUT_SECONDS,
        cache_ttl_minutes=DEFAULT_CACHE_TTL_MINUTES,
        state_dir=None,
        metadata_ttl_days=DEFAULT_METADATA_TTL_DAYS,
        rate_limit_cooldown_minutes=DEFAULT_RATE_LIMIT_COOLDOWN_MINUTES,
    )


def _ok(data: Any) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data})


def _csmar_error(error: CsmarError) -> JSONResponse:
    if error.upstream_code is not None or error.raw_message is not None:
        body = {
            "ok": False,
            "source": "csmar",
            "csmar": {"code": error.upstream_code, "msg": error.raw_message},
        }
        return JSONResponse(body, status_code=502)
    body = {
        "ok": False,
        "source": "local",
        "error_code": error.error_code,
        "message": error.message,
    }
    return JSONResponse(body, status_code=400)


def _validation_error(error: ValidationError) -> JSONResponse:
    return JSONResponse(
        {
            "ok": False,
            "source": "local",
            "error_code": "invalid_arguments",
            "errors": error.errors(),
        },
        status_code=422,
    )


async def _read_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise CsmarError("invalid_arguments", f"Request body is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise CsmarError("invalid_arguments", "Request body must be a JSON object.")
    return cast("dict[str, Any]", parsed)


async def list_databases(_request: Request) -> JSONResponse:
    try:
        return _ok({"databases": _get_client().list_databases()})
    except CsmarError as error:
        return _csmar_error(error)


async def list_tables(request: Request) -> JSONResponse:
    try:
        params = ListTablesInput.model_validate(
            {"database_name": request.query_params.get("database_name")}
        )
    except ValidationError as error:
        return _validation_error(error)
    try:
        records = _get_client().list_tables(params.database_name)
        return _ok(
            {"items": [{"table_code": r.table_code, "table_name": r.table_name} for r in records]}
        )
    except CsmarError as error:
        return _csmar_error(error)


async def get_table_schema(request: Request) -> JSONResponse:
    try:
        params = GetTableSchemaInput.model_validate(
            {"table_code": request.query_params.get("table_code")}
        )
    except ValidationError as error:
        return _validation_error(error)
    try:
        result = _get_client().read_table_schema(params.table_code)
        return _ok(result.as_dict())
    except CsmarError as error:
        return _csmar_error(error)


async def probe_query(request: Request) -> JSONResponse:
    try:
        body = await _read_body(request)
    except CsmarError as error:
        return _csmar_error(error)
    try:
        params = ProbeQueryInput.model_validate(body)
    except ValidationError as error:
        return _validation_error(error)
    try:
        result = _get_client().probe_query(params)
        return _ok(result.as_dict())
    except CsmarError as error:
        return _csmar_error(error)


async def materialize_query(request: Request) -> JSONResponse:
    try:
        body = await _read_body(request)
    except CsmarError as error:
        return _csmar_error(error)
    try:
        params = MaterializeQueryInput.model_validate(body)
    except ValidationError as error:
        return _validation_error(error)
    try:
        result = _get_client().materialize_query(params.validation_id, params.output_dir)
        return _ok(result.as_dict())
    except CsmarError as error:
        return _csmar_error(error)


routes = [
    Route("/list_databases", list_databases, methods=["GET"]),
    Route("/list_tables", list_tables, methods=["GET"]),
    Route("/get_table_schema", get_table_schema, methods=["GET"]),
    Route("/probe_query", probe_query, methods=["POST"]),
    Route("/materialize_query", materialize_query, methods=["POST"]),
]

app = Starlette(routes=routes)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="csmar-test-api",
        description=(
            "HTTP test API for CSMAR. Returns CSMAR's native upstream code/msg on error. "
            f"Credentials can be passed via CLI args or the "
            f"{ACCOUNT_ENV_VAR}/{PASSWORD_ENV_VAR} environment variables."
        ),
    )
    parser.add_argument("--account", default=None, help="CSMAR account")
    parser.add_argument("--password", default=None, help="CSMAR password")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    args.account, args.password = resolve_credentials(args.account, args.password)
    if not args.account or not args.password:
        parser.error(MISSING_CREDENTIALS_MESSAGE)
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    configure_test_client(args.account, args.password)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
