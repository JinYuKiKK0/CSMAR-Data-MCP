from __future__ import annotations

import re
from uuid import uuid4
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _generate_probe_id() -> str:
    return f"probe_{uuid4().hex[:8]}"


def _generate_download_id() -> str:
    return f"download_{uuid4().hex[:8]}"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ListTablesInput(StrictModel):
    database_name: str = Field(
        ..., min_length=1, description="数据库名称，由 csmar_list_databases 返回"
    )


class TableInfo(StrictModel):
    table_code: str = Field(..., description="表代码，用于查询和下载（如 TRD_Dalyr）")
    table_name: str = Field(..., description="表中文名称（如 日个股回报率文件）")


class DescribeTableInput(StrictModel):
    table_name: str = Field(
        ...,
        min_length=1,
        description="表代码（如 TRD_Dalyr），由 csmar_list_tables 返回的 table_code 字段",
    )
    preview_rows: int = Field(
        default=5,
        ge=0,
        le=10,
        description="预览行数，0 表示不返回预览数据",
    )


class ProbeQuery(StrictModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    probe_id: str = Field(
        default_factory=_generate_probe_id,
        min_length=1,
        description="探测请求标识，省略时自动生成",
    )
    table_name: str = Field(
        ...,
        min_length=1,
        description="表代码（如 TRD_Dalyr），由 csmar_list_tables 返回",
    )
    columns: list[str] = Field(
        ...,
        min_length=1,
        description="查询字段名列表（如 ['Stkcd', 'Trddt', 'Clsprc']）",
    )
    condition: str | None = Field(
        default=None,
        description="CSMAR 原生查询条件（如 'Stkcd=000001'），不填则查询全部",
    )
    start_date: str | None = Field(
        default=None,
        description="起始日期，格式 YYYY-MM-DD，注意：日期范围需在 5 年内",
    )
    end_date: str | None = Field(
        default=None,
        description="结束日期，格式 YYYY-MM-DD，注意：日期范围需在 5 年内",
    )

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        cleaned = [col.strip() for col in value if col and col.strip()]
        if not cleaned:
            raise ValueError("columns must contain at least one non-empty field name")
        return cleaned

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not _DATE_RE.match(value):
            raise ValueError("date must use YYYY-MM-DD format")
        return value

    @model_validator(mode="after")
    def validate_date_range(self) -> "ProbeQuery":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ProbeQueriesInput(StrictModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    queries: list[ProbeQuery] | None = Field(
        default=None,
        min_length=1,
        description="批量探测查询列表。兼容历史格式：queries=[...]",
    )
    sample_rows: int = Field(
        default=5,
        ge=0,
        le=10,
        description="每个探测返回的样本行数",
    )
    probe_id: str | None = Field(
        default=None,
        min_length=1,
        description="扁平单查询模式下可选，省略时自动生成",
    )
    table_name: str | None = Field(
        default=None,
        min_length=1,
        description="扁平单查询模式下的表代码（如 TRD_Dalyr）",
    )
    columns: list[str] | None = Field(
        default=None,
        min_length=1,
        description="扁平单查询模式下的字段列表",
    )
    condition: str | None = Field(
        default=None,
        description="扁平单查询模式下的 CSMAR 原生查询条件",
    )
    start_date: str | None = Field(
        default=None,
        description="扁平单查询模式下的起始日期，格式 YYYY-MM-DD",
    )
    end_date: str | None = Field(
        default=None,
        description="扁平单查询模式下的结束日期，格式 YYYY-MM-DD",
    )

    @model_validator(mode="after")
    def normalize_queries(self) -> "ProbeQueriesInput":
        has_batch = bool(self.queries)
        has_flat_fields = any(
            value is not None
            for value in (
                self.probe_id,
                self.table_name,
                self.columns,
                self.condition,
                self.start_date,
                self.end_date,
            )
        )

        if has_batch and has_flat_fields:
            raise ValueError(
                "参数格式错误：不要同时传入 queries 与扁平字段。"
                "请二选一：{\"queries\": [...]} 或 {\"table_name\": \"FS_Combas\", \"columns\": [\"Stkcd\"]}"
            )

        if has_batch:
            return self

        if not self.table_name:
            raise ValueError(
                "参数格式错误。请使用批量格式 {\"queries\": [...]}，"
                "或扁平格式 {\"table_name\": \"FS_Combas\", \"columns\": [\"Stkcd\"]}。"
            )

        if not self.columns:
            raise ValueError(
                "扁平格式缺少 columns 字段。示例："
                "{\"table_name\": \"FS_Combas\", \"columns\": [\"Stkcd\", \"Accper\"]}"
            )

        self.queries = [
            ProbeQuery(
                probe_id=self.probe_id or _generate_probe_id(),
                table_name=self.table_name,
                columns=self.columns,
                condition=self.condition,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        ]
        return self


class DownloadPlan(StrictModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    download_id: str = Field(
        default_factory=_generate_download_id,
        min_length=1,
        validation_alias=AliasChoices("download_id", "plan_id"),
        description="下载请求标识。兼容历史字段 plan_id，省略时自动生成",
    )
    table_name: str = Field(
        ...,
        min_length=1,
        description="表代码（如 TRD_Dalyr），由 csmar_list_tables 返回",
    )
    columns: list[str] = Field(
        ...,
        min_length=1,
        description="下载字段名列表（如 ['Stkcd', 'Trddt', 'Clsprc']）",
    )
    condition: str | None = Field(
        default=None,
        description="CSMAR 原生查询条件（如 'Stkcd=000001'），不填则下载全部",
    )
    start_date: str | None = Field(
        default=None,
        description="起始日期，格式 YYYY-MM-DD，注意：日期范围需在 5 年内",
    )
    end_date: str | None = Field(
        default=None,
        description="结束日期，格式 YYYY-MM-DD，注意：日期范围需在 5 年内",
    )

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        cleaned = [col.strip() for col in value if col and col.strip()]
        if not cleaned:
            raise ValueError("columns must contain at least one non-empty field name")
        return cleaned

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not _DATE_RE.match(value):
            raise ValueError("date must use YYYY-MM-DD format")
        return value

    @model_validator(mode="after")
    def validate_date_range(self) -> "DownloadPlan":
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class MaterializeDownloadsInput(StrictModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    downloads: list[DownloadPlan] | None = Field(
        default=None,
        min_length=1,
        validation_alias=AliasChoices("downloads", "plans"),
        description="下载请求列表。兼容历史字段 plans",
    )
    output_dir: str = Field(..., min_length=1, description="下载文件输出目录")
    download_id: str | None = Field(
        default=None,
        min_length=1,
        description="扁平单下载模式下可选，省略时自动生成",
    )
    table_name: str | None = Field(
        default=None,
        min_length=1,
        description="扁平单下载模式下的表代码",
    )
    columns: list[str] | None = Field(
        default=None,
        min_length=1,
        description="扁平单下载模式下的字段列表",
    )
    condition: str | None = Field(
        default=None,
        description="扁平单下载模式下的 CSMAR 原生查询条件",
    )
    start_date: str | None = Field(
        default=None,
        description="扁平单下载模式下的起始日期，格式 YYYY-MM-DD",
    )
    end_date: str | None = Field(
        default=None,
        description="扁平单下载模式下的结束日期，格式 YYYY-MM-DD",
    )

    @model_validator(mode="after")
    def normalize_downloads(self) -> "MaterializeDownloadsInput":
        has_batch = bool(self.downloads)
        has_flat_fields = any(
            value is not None
            for value in (
                self.download_id,
                self.table_name,
                self.columns,
                self.condition,
                self.start_date,
                self.end_date,
            )
        )

        if has_batch and has_flat_fields:
            raise ValueError(
                "参数格式错误：不要同时传入 downloads/plans 与扁平字段。"
                "请二选一：{\"downloads\": [...]} 或 {\"table_name\": \"FS_Combas\", \"columns\": [...], \"output_dir\": \"...\"}"
            )

        if has_batch:
            return self

        if not self.table_name:
            raise ValueError(
                "参数格式错误。请使用批量格式 {\"downloads\": [...], \"output_dir\": \"...\"}，"
                "或扁平格式 {\"table_name\": \"FS_Combas\", \"columns\": [...], \"output_dir\": \"...\"}。"
            )

        if not self.columns:
            raise ValueError(
                "扁平格式缺少 columns 字段。示例："
                "{\"table_name\": \"FS_Combas\", \"columns\": [\"Stkcd\"], \"output_dir\": \"D:/tmp/csmar\"}"
            )

        self.downloads = [
            DownloadPlan(
                download_id=self.download_id or _generate_download_id(),
                table_name=self.table_name,
                columns=self.columns,
                condition=self.condition,
                start_date=self.start_date,
                end_date=self.end_date,
            )
        ]
        return self


class DatabaseInfo(StrictModel):
    database_name: str = Field(..., description="数据库名称")


class ListDatabasesOutput(StrictModel):
    databases: list[str] = Field(default_factory=list, description="已购买的数据库名称列表")


class ListTablesOutput(StrictModel):
    database_name: str = Field(..., description="数据库名称")
    tables: list[TableInfo] = Field(default_factory=list, description="表信息列表，包含 table_code 和 table_name")


class FieldInfo(StrictModel):
    field_name: str = Field(..., description="字段名（英文代码）")


class DescribeTableOutput(StrictModel):
    table_name: str = Field(..., description="表代码")
    fields: list[str] = Field(default_factory=list, description="字段名列表")
    preview_rows: list[dict[str, Any]] = Field(default_factory=list, description="预览数据行")
    preview_truncated: bool = Field(default=False, description="预览数据是否被截断")


class ProbeResult(StrictModel):
    probe_id: str = Field(..., description="客户端标识")
    table_name: str = Field(..., description="表代码")
    columns_exist: bool = Field(..., description="请求的字段是否全部存在")
    missing_columns: list[str] = Field(default_factory=list, description="不存在的字段列表")
    match_count: int | None = Field(default=None, description="匹配的记录数")
    sample_rows: list[dict[str, Any]] = Field(default_factory=list, description="样本数据行")
    sample_truncated: bool = Field(default=False, description="样本数据是否被截断")
    accessible: bool = Field(..., description="表是否可访问")
    error_code: str | None = Field(default=None, description="错误代码")
    failure_reason: str | None = Field(default=None, description="失败原因")
    columns_valid: bool | None = Field(default=None, description="简化字段，等同 columns_exist")
    record_count: int | None = Field(default=None, description="简化字段，等同 match_count")
    sample: list[dict[str, Any]] = Field(default_factory=list, description="简化字段，等同 sample_rows")
    can_download: bool | None = Field(default=None, description="是否可继续执行下载")

    @model_validator(mode="after")
    def normalize_agent_friendly_fields(self) -> "ProbeResult":
        if self.columns_valid is None:
            self.columns_valid = self.columns_exist
        if self.record_count is None:
            self.record_count = self.match_count
        if not self.sample and self.sample_rows:
            self.sample = list(self.sample_rows)
        if self.can_download is None:
            self.can_download = self.accessible and self.columns_exist and self.error_code is None
        return self


class ProbeQueriesOutput(StrictModel):
    results: list[ProbeResult] = Field(default_factory=list, description="探测结果列表")
    warnings: list[str] = Field(default_factory=list, description="警告信息")


class DownloadArtifact(StrictModel):
    download_id: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("download_id", "plan_id"),
        description="下载请求标识",
    )
    plan_id: str | None = Field(default=None, description="兼容字段，等同 download_id")
    table_name: str = Field(..., description="表代码")
    status: Literal["success", "failed", "cached"] = Field(..., description="下载状态")
    zip_path: str | None = Field(default=None, description="ZIP 文件路径")
    extract_dir: str | None = Field(default=None, description="解压目录路径")
    files: list[str] = Field(default_factory=list, description="解压后的文件列表")
    retry_count: int = Field(default=0, description="重试次数")
    error_code: str | None = Field(default=None, description="错误代码")
    failure_reason: str | None = Field(default=None, description="失败原因")

    @model_validator(mode="after")
    def normalize_legacy_plan_id(self) -> "DownloadArtifact":
        if self.plan_id is None:
            self.plan_id = self.download_id
        return self


class MaterializeDownloadsOutput(StrictModel):
    artifacts: list[DownloadArtifact] = Field(default_factory=list, description="下载产物列表")
    warnings: list[str] = Field(default_factory=list, description="警告信息")
