from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Sequence

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import ValidationError

from .client import CsmarClient, CsmarMcpError
from .models import (
    DescribeTableInput,
    DescribeTableOutput,
    DownloadPlan,
    DownloadArtifact,
    ListDatabasesOutput,
    ListTablesInput,
    ListTablesOutput,
    MaterializeDownloadsInput,
    MaterializeDownloadsOutput,
    ProbeQuery,
    ProbeQueriesInput,
    ProbeQueriesOutput,
    ProbeResult,
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
            "Run CSMAR MCP server over stdio. Only account and password are "
            "accepted as runtime args; other settings are fixed in code."
        ),
    )
    parser.add_argument("--account", required=True, help="CSMAR account")
    parser.add_argument("--password", required=True, help="CSMAR password")
    return parser


def _parse_runtime_settings(argv: Sequence[str] | None = None) -> RuntimeSettings:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    return RuntimeSettings(
        account=args.account,
        password=args.password,
    )


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


def _format_tool_error(error: CsmarMcpError) -> str:
    if error.upstream_code is None:
        return f"[{error.error_code}] {error.message}"
    return f"[{error.error_code}] {error.message} (upstream_code={error.upstream_code})"


def _normalize_probe_params(
    params: ProbeQueriesInput | None,
    *,
    queries: list[ProbeQuery] | None,
    table_name: str | None,
    columns: list[str] | None,
    condition: str | None,
    start_date: str | None,
    end_date: str | None,
    sample_rows: int | None,
    probe_id: str | None,
) -> ProbeQueriesInput:
    payload: dict[str, Any] = params.model_dump(mode="python", exclude_none=True) if params else {}

    if queries is not None:
        payload["queries"] = queries
    if table_name is not None:
        payload["table_name"] = table_name
    if columns is not None:
        payload["columns"] = columns
    if condition is not None:
        payload["condition"] = condition
    if start_date is not None:
        payload["start_date"] = start_date
    if end_date is not None:
        payload["end_date"] = end_date
    if sample_rows is not None:
        payload["sample_rows"] = sample_rows
    if probe_id is not None:
        payload["probe_id"] = probe_id

    try:
        return ProbeQueriesInput.model_validate(payload)
    except ValidationError as error:
        raise RuntimeError(
            "参数格式错误。支持两种调用方式："
            "1) 顶层扁平字段（table_name/columns/...）；"
            "2) params 结构（params={...}）。"
            f"详情: {error}"
        ) from error


def _normalize_download_params(
    params: MaterializeDownloadsInput | None,
    *,
    downloads: list[DownloadPlan] | None,
    plans: list[DownloadPlan] | None,
    output_dir: str | None,
    table_name: str | None,
    columns: list[str] | None,
    condition: str | None,
    start_date: str | None,
    end_date: str | None,
    download_id: str | None,
    plan_id: str | None,
) -> MaterializeDownloadsInput:
    payload: dict[str, Any] = params.model_dump(mode="python", exclude_none=True) if params else {}

    if downloads is not None:
        payload["downloads"] = downloads
    elif plans is not None:
        payload["downloads"] = plans

    if output_dir is not None:
        payload["output_dir"] = output_dir
    if table_name is not None:
        payload["table_name"] = table_name
    if columns is not None:
        payload["columns"] = columns
    if condition is not None:
        payload["condition"] = condition
    if start_date is not None:
        payload["start_date"] = start_date
    if end_date is not None:
        payload["end_date"] = end_date

    resolved_download_id = download_id if download_id is not None else plan_id
    if resolved_download_id is not None:
        payload["download_id"] = resolved_download_id

    try:
        return MaterializeDownloadsInput.model_validate(payload)
    except ValidationError as error:
        raise RuntimeError(
            "参数格式错误。支持两种调用方式："
            "1) 顶层扁平字段（table_name/columns/output_dir/...）；"
            "2) params 结构（params={...}）。"
            f"详情: {error}"
        ) from error


def _build_cooldown_reason(remaining_seconds: int) -> str:
    return (
        "请求被本地冷却窗口拦截，以避免触发 CSMAR 30 分钟同条件限制。"
        f"请在约 {remaining_seconds} 秒后重试，或调整 condition/start_date/end_date。"
    )


mcp = FastMCP(
    name="csmar_mcp",
    instructions=(
        "CSMAR MCP 服务器，用于元数据发现、轻量级探测查询和下载打包文件到本地。"
        "使用流程：1. csmar_list_databases 获取数据库列表 -> "
        "2. csmar_list_tables 获取表列表（返回 table_code 用于后续操作）-> "
        "3. csmar_describe_table 查看表结构 -> "
        "4. csmar_probe_queries 探测查询可行性 -> "
        "5. csmar_materialize_downloads 下载数据。"
        "注意：查询时日期范围需在 5 年内。"
    ),
    json_response=True,
)


@mcp.tool(
    name="csmar_list_databases",
    annotations=ToolAnnotations(
        title="列出已购数据库",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_list_databases() -> ListDatabasesOutput:
    """列出当前账号已购买的 CSMAR 数据库。"""
    client = get_client()
    try:
        databases = client.list_databases()
        return ListDatabasesOutput(databases=databases)
    except CsmarMcpError as error:
        raise RuntimeError(_format_tool_error(error)) from error


@mcp.tool(
    name="csmar_list_tables",
    annotations=ToolAnnotations(
        title="列出数据库中的表",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_list_tables(params: ListTablesInput) -> ListTablesOutput:
    """列出指定数据库下的所有表，返回表代码（table_code）和中文名（table_name）。
    后续查询和下载操作需要使用 table_code 字段。"""
    client = get_client()
    try:
        tables = client.list_tables(params.database_name)
        return ListTablesOutput(database_name=params.database_name, tables=tables)
    except CsmarMcpError as error:
        raise RuntimeError(_format_tool_error(error)) from error


@mcp.tool(
    name="csmar_describe_table",
    annotations=ToolAnnotations(
        title="描述表结构",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_describe_table(params: DescribeTableInput) -> DescribeTableOutput:
    """返回表的字段列表和预览数据。table_name 参数应使用 csmar_list_tables 返回的 table_code。"""
    client = get_client()
    try:
        fields = client.list_fields(params.table_name)
        preview_rows = []
        preview_truncated = False

        if params.preview_rows > 0:
            raw_preview_rows = client.preview(params.table_name)
            preview_rows = raw_preview_rows[: params.preview_rows]
            preview_truncated = len(raw_preview_rows) > params.preview_rows

        return DescribeTableOutput(
            table_name=params.table_name,
            fields=fields,
            preview_rows=preview_rows,
            preview_truncated=preview_truncated,
        )
    except CsmarMcpError as error:
        raise RuntimeError(_format_tool_error(error)) from error


@mcp.tool(
    name="csmar_probe_queries",
    annotations=ToolAnnotations(
        title="探测查询可行性",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def csmar_probe_queries(
    params: ProbeQueriesInput | None = None,
    queries: list[ProbeQuery] | None = None,
    table_name: str | None = None,
    columns: list[str] | None = None,
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    sample_rows: int | None = None,
    probe_id: str | None = None,
) -> ProbeQueriesOutput:
    """探测 CSMAR 查询可行性，返回字段校验、记录数与样本数据。

    调用入口兼容：
    - 顶层参数：直接传 table_name/columns/.../sample_rows
    - 兼容参数：传 params={...}

    支持两种参数格式（任选其一）：
    1) 批量格式
    {
        "queries": [
            {
                "probe_id": "q1",
                "table_name": "FS_Combas",
                "columns": ["Stkcd", "ShortName", "Accper"],
                "condition": "Stkcd='000001'",
                "start_date": "2020-01-01",
                "end_date": "2024-12-31"
            }
        ],
        "sample_rows": 5
    }

    2) 扁平单查询格式
    {
        "table_name": "FS_Combas",
        "columns": ["Stkcd", "ShortName", "Accper"],
        "condition": "Stkcd='000001'",
        "start_date": "2020-01-01",
        "end_date": "2024-12-31",
        "sample_rows": 5
    }

    返回字段包含：
    - columns_exist / missing_columns（字段校验）
    - match_count / sample_rows（统计与样本）
    - columns_valid / record_count / sample / can_download（简化字段）

    限制：日期范围（start_date 到 end_date）需在 5 年内。
    """
    normalized_params = _normalize_probe_params(
        params,
        queries=queries,
        table_name=table_name,
        columns=columns,
        condition=condition,
        start_date=start_date,
        end_date=end_date,
        sample_rows=sample_rows,
        probe_id=probe_id,
    )

    client = get_client()
    table_fields_cache: dict[str, set[str]] = {}
    results: list[ProbeResult] = []
    warnings: list[str] = []

    for query in normalized_params.queries or []:
        cache_key = client.build_probe_cache_key(query)
        cached_result = client.get_cached_probe(cache_key)
        if cached_result is not None:
            results.append(cached_result)
            warnings.append(f"probe_id={query.probe_id}: used cached probe result")
            continue

        remaining_seconds = client.get_rate_limit_remaining_seconds(cache_key)
        if remaining_seconds is not None:
            results.append(
                ProbeResult(
                    probe_id=query.probe_id,
                    table_name=query.table_name,
                    columns_exist=False,
                    missing_columns=[],
                    match_count=None,
                    sample_rows=[],
                    sample_truncated=False,
                    accessible=False,
                    error_code="rate_limited",
                    failure_reason=_build_cooldown_reason(remaining_seconds),
                )
            )
            warnings.append(
                f"probe_id={query.probe_id}: local cooldown active, skipped upstream request"
            )
            continue

        try:
            if query.table_name not in table_fields_cache:
                table_fields_cache[query.table_name] = set(client.list_fields(query.table_name))

            table_fields = table_fields_cache[query.table_name]
            missing_columns = [column for column in query.columns if column not in table_fields]
            if missing_columns:
                result = ProbeResult(
                    probe_id=query.probe_id,
                    table_name=query.table_name,
                    columns_exist=False,
                    missing_columns=missing_columns,
                    match_count=None,
                    sample_rows=[],
                    sample_truncated=False,
                    accessible=True,
                    error_code="field_not_found",
                    failure_reason="Some requested columns do not exist in the table",
                )
                results.append(result)
                continue

            match_count = client.query_count(
                table_name=query.table_name,
                columns=query.columns,
                condition=query.condition,
                start_date=query.start_date,
                end_date=query.end_date,
            )

            sample_data_rows: list[dict[str, Any]] = []
            if normalized_params.sample_rows > 0 and match_count > 0:
                sample_data_rows = client.query_sample(
                    table_name=query.table_name,
                    columns=query.columns,
                    sample_rows=normalized_params.sample_rows,
                    condition=query.condition,
                    start_date=query.start_date,
                    end_date=query.end_date,
                )

            result = ProbeResult(
                probe_id=query.probe_id,
                table_name=query.table_name,
                columns_exist=True,
                missing_columns=[],
                match_count=match_count,
                sample_rows=sample_data_rows,
                sample_truncated=match_count > len(sample_data_rows),
                accessible=True,
            )
            client.set_cached_probe(cache_key, result)
            results.append(result)

        except CsmarMcpError as error:
            if error.error_code == "rate_limited":
                client.mark_rate_limited(cache_key)
                cached_result = client.get_cached_probe(cache_key)
                if cached_result is not None:
                    results.append(cached_result)
                    warnings.append(
                        f"probe_id={query.probe_id}: request rate-limited, reused cached probe result"
                    )
                    continue

            results.append(
                ProbeResult(
                    probe_id=query.probe_id,
                    table_name=query.table_name,
                    columns_exist=False,
                    missing_columns=[],
                    match_count=None,
                    sample_rows=[],
                    sample_truncated=False,
                    accessible=False,
                    error_code=error.error_code,
                    failure_reason=error.message,
                )
            )

    return ProbeQueriesOutput(results=results, warnings=warnings)


@mcp.tool(
    name="csmar_materialize_downloads",
    annotations=ToolAnnotations(
        title="下载并物化数据",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def csmar_materialize_downloads(
    params: MaterializeDownloadsInput | None = None,
    downloads: list[DownloadPlan] | None = None,
    plans: list[DownloadPlan] | None = None,
    output_dir: str | None = None,
    table_name: str | None = None,
    columns: list[str] | None = None,
    condition: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    download_id: str | None = None,
    plan_id: str | None = None,
) -> MaterializeDownloadsOutput:
    """下载数据到指定目录，返回 ZIP 与解压后的本地产物清单。

    调用入口兼容：
    - 顶层参数：直接传 table_name/columns/output_dir/...；
    - 兼容参数：传 params={...}。

    支持两种参数格式（任选其一）：
    1) 批量格式（推荐字段名 downloads，兼容历史字段 plans）
    {
      "downloads": [
        {
          "download_id": "dl_001",
          "table_name": "FS_Combas",
          "columns": ["Stkcd", "ShortName", "Accper"],
          "condition": "Stkcd='000001'",
          "start_date": "2020-01-01",
          "end_date": "2024-12-31"
        }
      ],
      "output_dir": "D:/tmp/csmar"
    }

    2) 扁平单下载格式
    {
      "table_name": "FS_Combas",
      "columns": ["Stkcd", "ShortName", "Accper"],
      "condition": "Stkcd='000001'",
      "start_date": "2020-01-01",
      "end_date": "2024-12-31",
      "output_dir": "D:/tmp/csmar"
    }

    返回 artifacts 中的主标识为 download_id，并保留 plan_id 兼容字段。
    限制：日期范围（start_date 到 end_date）需在 5 年内。
    """
    normalized_params = _normalize_download_params(
        params,
        downloads=downloads,
        plans=plans,
        output_dir=output_dir,
        table_name=table_name,
        columns=columns,
        condition=condition,
        start_date=start_date,
        end_date=end_date,
        download_id=download_id,
        plan_id=plan_id,
    )

    client = get_client()
    artifacts: list[DownloadArtifact] = []
    warnings: list[str] = []

    for plan in normalized_params.downloads or []:
        cache_key = client.build_download_cache_key(plan)

        remaining_seconds = client.get_rate_limit_remaining_seconds(cache_key)
        if remaining_seconds is not None:
            artifacts.append(
                DownloadArtifact(
                    download_id=plan.download_id,
                    table_name=plan.table_name,
                    status="failed",
                    zip_path=None,
                    extract_dir=None,
                    files=[],
                    retry_count=0,
                    error_code="rate_limited",
                    failure_reason=_build_cooldown_reason(remaining_seconds),
                )
            )
            warnings.append(
                f"download_id={plan.download_id}: local cooldown active, skipped upstream request"
            )
            continue

        try:
            artifact = client.materialize_download(plan, normalized_params.output_dir, max_retries=2)
            artifacts.append(artifact)
            if artifact.status == "cached":
                warnings.append(f"download_id={plan.download_id}: used cached download artifact")
        except CsmarMcpError as error:
            if error.error_code == "rate_limited":
                client.mark_rate_limited(cache_key)
                cached_artifact = client.get_cached_download(cache_key)
                if cached_artifact is not None:
                    cached_artifact.status = "cached"
                    artifacts.append(cached_artifact)
                    warnings.append(
                        f"download_id={plan.download_id}: request rate-limited, reused cached artifact"
                    )
                    continue

            artifacts.append(
                DownloadArtifact(
                    download_id=plan.download_id,
                    table_name=plan.table_name,
                    status="failed",
                    zip_path=None,
                    extract_dir=None,
                    files=[],
                    retry_count=0,
                    error_code=error.error_code,
                    failure_reason=error.message,
                )
            )

    return MaterializeDownloadsOutput(artifacts=artifacts, warnings=warnings)


def main(argv: Sequence[str] | None = None) -> None:
    settings = _parse_runtime_settings(argv)
    _configure_runtime(settings)
    mcp.run()


if __name__ == "__main__":
    main()
