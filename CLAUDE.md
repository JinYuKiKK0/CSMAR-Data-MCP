# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 仓库定位

CSMAR-Data-MCP 是面向 Agent 工作流的精简 MCP 服务器（FastMCP / stdio），把 CSMAR 上游 SDK 的元数据发现、查询预检、本地物化封装为 8 个 MCP 工具。本仓库既是独立 git 仓库，也作为 submodule 挂在上层 `Harness-Stata` 项目的 `packages/CSMAR-Data-MCP/` 下，被主 Agent 通过 stdio MCP 协议调用 —— **禁止跨仓库直接 import `csmar_mcp.services`**，所有外部使用都走 MCP 工具边界。

详细工具语义、缓存 TTL、Agent 调用最佳实践见 `README.md`，本文不重复，只记录架构与开发约定。

## 常用命令

```bash
# 安装（含 dev 工具链）
uv sync --group dev

# 启动 MCP server（stdio）
uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD

# Lint：默认 check-only（pre-push / CI 行为）
uv run python scripts/check.py
# Lint：自动修复 + 格式化（pre-commit 行为）
uv run python scripts/check.py --fix

# 测试：项目用 stdlib unittest，没有 pytest 配置
uv run python -m unittest discover -s tests
# 跑单个测试
uv run python -m unittest tests.test_services.MetadataServiceTests.test_xxx

# 启用本地 git 钩子（一次性，pre-commit framework）
uv run pre-commit install                       # commit 前 --fix
uv run pre-commit install --hook-type pre-push  # push 前 check-only
```

注意：`scripts/check.py` 只跑 ruff + pyright，**不跑测试**。CI（`.github/workflows/lint.yml`）也只跑 lint。运行 lint 与测试时不要加 `| tail` / `| head` 截断。

## 代码架构

四层架构，依赖方向严格自上而下：

```
server.py            （MCP 工具边界：FastMCP @mcp.tool 装饰器、参数校验、审计日志）
   │
   ├── presenters.py （CallToolResult 序列化；tool_error_boundary 装饰器；recoverable vs hard 错误分类）
   ├── runtime.py    （CLI 参数解析 → RuntimeSettings；lru_cache 单例的 get_client()）
   │
   ▼
client.py            （CsmarClient 门面，组合 services；上层唯一对外类型）
   │
   ▼
services/            （领域逻辑）
   ├── metadata.py   （list_databases / list_tables / read_schema / bulk_schema / search_field；
   │                   持旧缓存自愈：撞 not_purchased 时回退失效 databases 缓存）
   └── query.py      （probe_query / materialize_query；query_fingerprint / validation_id 串联）
   │
   ▼
infra/               （I/O 层）
   ├── csmar_gateway.py  （★ 唯一与 csmarapi/ 未类型 SDK 接触的边界；文件首行 `# pyright: basic`）
   └── state.py          （PersistentState：SQLite 缓存 + namespace TTL + 工具调用审计 trace）
   │
   ▼
core/                （纯领域类型与错误，无 I/O）
   ├── types.py      （CatalogRecord / ProbeSpec / ProbeResult / ValidationRecord 等 frozen dataclass）
   └── errors.py     （CsmarError：error_code + message + hint + 可选 retry_after / suggested_args_patch）
```

`csmarapi/` 是上游下发的未类型 SDK，pyright/ruff 均排除；它**只允许被 `csmar_gateway.py` import**，其它文件出现 `from csmarapi` 是架构违规。

### 关键设计点

- **缓存固定在包目录**：`PersistentState` 默认路径是 `csmar_mcp/csmar_mcp_cache/state.sqlite3`（随包不随 cwd），保证不同工作目录的 MCP 会话共享同一份缓存，是上游限流策略的核心前提。可用 `CSMAR_MCP_STATE_DIR` 环境变量覆盖。
- **分级 TTL**：`databases / tables / schema` 默认 30 天（元数据），`probes / validations / downloads` 默认 3 天（业务），`rate_limit_cooldowns` 固定 30 分钟。修改 TTL 时认准 `runtime.py` 的 `DEFAULT_*` 与 `CsmarClient.__init__` 的 `namespace_ttls` 装配。
- **错误分类决定 isError**：`presenters.AGENT_RECOVERABLE_CODES` 列出 Agent 可凭 hint 重试的业务错误（`not_purchased` / `invalid_condition` / `rate_limited` 等），其余 code 视作硬异常返回 `isError=True` 中断工作流。新增错误 code 时必须显式判断该归哪一类，**未列入集合等价于硬中断**。
- **工具审计**：每次工具调用通过 `_safe_log_trace` 写入 SQLite 的 `tool_trace` 表，包含 request payload、result summary、cached 标记、`query_fingerprint` 与 `validation_id`，用于后续配额分析。新增工具时必须接入这套审计。
- **运行时单例**：`runtime.get_client()` 用 `lru_cache(maxsize=1)`，进程内只有一个 `CsmarClient`。重新配置必须走 `configure_runtime()` 主动 `cache_clear()`，**绝不要直接 new `CsmarClient`** 绕过单例。
- **Probe → Materialize 双阶段**：probe 返回的 `validation_id` 是物化的唯一入口，`query_fingerprint` 用于跨调用的缓存键。这两个 id 的生成规则在 `services/query.py` 的 `build_cache_key` / `build_query_fingerprint`，修改时务必保证幂等。

## 编码约定

- Python 3.12，pyright **strict** 模式（`csmarapi/` 与 `csmar_gateway.py` 例外），ruff line-length=100、禁止父级相对 import（`flake8-tidy-imports.ban-relative-imports = "parents"`）。
- Pydantic 模型统一继承 `models.StrictModel`（`extra="forbid"` + `str_strip_whitespace=True`），输出走 `as_dict()` 走 `mode="json"`、剔除 None / 默认值 / 未设值。
- 工具实现模式：`@mcp.tool(...)` + `@tool_error_boundary(...)` + 显式 `_safe_log_trace` 调用 + `success/failure/invalid_arguments` 三选一返回。新增工具时严格按既有 7 个工具的模板复制。
- 测试不使用 pytest，用 stdlib `unittest`；测试包内 pyright 放宽（见 `pyproject.toml` 的 `executionEnvironments`）。
