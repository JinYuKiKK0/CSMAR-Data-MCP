# CSMAR MCP

面向 Agent 工作流的精简 MCP 服务器：提供 CSMAR 元数据发现、查询可行性探测，以及本地物化。

## 对外接口

### 工具

1. `csmar_list_databases`
   确定性枚举已购买的数据库。命中元数据缓存时零 API 调用。

2. `csmar_list_tables`
   确定性枚举指定数据库下的表。命中元数据缓存时零 API 调用；若持旧缓存撞上 `not_purchased` 会自动失效并重取一次。

3. `csmar_get_table_schema`
   返回纯表结构与字段元数据，不返回样本行。命中元数据缓存时零 API 调用。

4. `csmar_bulk_schema`
   一次性获取多个 `table_code` 的表结构（上限 20 个）。cache-first：已缓存的条目零 API 调用，只有真正的 miss 会并发（上限 4）打 CSMAR。**首选，避免重复调用 `csmar_get_table_schema`。**

5. `csmar_probe_query`
   对查询进行预检，返回 `validation_id`、`query_fingerprint`、行数、少量样本、无效列，以及物化可行性。

6. `csmar_materialize_query`
   按 `validation_id` 将先前预检过的查询物化为本地文件。

7. `csmar_refresh_cache` ⚠️ **危险工具**
   显式失效元数据缓存（`databases` / `tables` / `schema` / `all`，可选指定 `key`）。**仅在用户明确要求刷新时调用** —— 例如用户怀疑表结构变更或新购了数据库。调用后后续元数据查询会直接打受限流的 CSMAR API，绝不可在常规探索中预调用。

## 设计原则

- 每个工具职责单一。
- JSON 输出精简：只返回下一步所需字段。
- 面向修复的错误：`code`、`message`、`hint`，以及可选的 `retry_after_seconds`、`suggested_args_patch`。
- 日期区间只做格式与顺序校验，随后原样透传给 SDK。
- 查询的预检与物化通过 `validation_id` 串联。
- 运行时状态持久化在 SQLite 中，路径默认为 `<csmar_mcp 包目录>/csmar_mcp_cache/state.sqlite3`，跨工作目录的会话天然共享同一份缓存。

## 工具示例

### `csmar_list_databases`

```json
{}
```

### `csmar_list_tables`

```json
{
  "database_name": "股票市场交易"
}
```

### `csmar_get_table_schema`

```json
{
  "table_code": "FS_Combas"
}
```

### `csmar_bulk_schema`

```json
{
  "table_codes": ["BANK_Index", "BANK_Loan", "BANK_CreditRisks"]
}
```

### `csmar_refresh_cache`

```json
{
  "namespace": "schema",
  "key": "BANK_Loan"
}
```

### `csmar_probe_query`

```json
{
  "table_code": "FS_Combas",
  "columns": ["Stkcd", "Accper", "Typrep"],
  "condition": "Stkcd='000001'",
  "start_date": "2010-01-01",
  "end_date": "2024-12-31",
  "sample_rows": 2
}
```

### `csmar_materialize_query`

```json
{
  "validation_id": "validation_1234567890",
  "output_dir": "D:/tmp/csmar"
}
```

## 运行时默认值

- `lang = "0"`
- `belong = "0"`
- `poll_interval_seconds = 3`
- `poll_timeout_seconds = 900`
- `cache_ttl_minutes = 4320`（即 **3 天**，对业务查询缓存 `probes / validations / downloads` 生效）
- `metadata_ttl_days = 30`（元数据缓存 `databases / tables / schema` 的默认 TTL，可用 `CSMAR_MCP_METADATA_TTL_DAYS` 覆盖）
- `rate_limit_cooldown_minutes = 30`（上游限流冷却窗口，与业务缓存 TTL 解耦）
- `state_dir = <csmar_mcp 包目录>/csmar_mcp_cache/`（随包而非工作目录，天然跨会话共享；可用 `CSMAR_MCP_STATE_DIR` 环境变量显式覆盖）

## 缓存与限流策略

CSMAR 后端每日 API 配额非常严苛，MCP 的核心策略是**把绝大多数调用挡在本地缓存层**，远程调用只用于真正必须拉数的 `probe_query` / `materialize_query` 以及首次的元数据拉取。

**缓存分层**

| 命名空间 | 用途 | 默认 TTL | 说明 |
| --- | --- | --- | --- |
| `databases` / `tables` / `schema` | 元数据 | **30 天** | 表结构长期不变；cache hit 完全不打 CSMAR |
| `probes` / `validations` / `downloads` | 业务查询结果 | **3 天** | 覆盖同一查询在短期内的重复访问 |
| `rate_limit_cooldowns` | 上游限流冷却 | 30 分钟（固定） | 与业务 TTL 解耦，避免误锁 3 天 |

**跨目录共享**

缓存 SQLite 文件固定在 `csmar_mcp` 包目录下的 `csmar_mcp_cache/state.sqlite3`，不跟随 `cwd`。在任何目录启动 MCP 会话都共享同一份缓存 —— 这是把限流风险降到最低的关键前提。

**持旧缓存自愈**

若 Agent 拿着过期的 `databases` 缓存去请求某个未购库，`list_tables` / `get_table_schema` 会捕获 `not_purchased` / `database_not_found` / `table_not_found`，自动失效 `databases` 缓存并重取一次上游，单点配额消耗换来整条链路的一致性。

**Agent 最佳实践**

- 批量需要多个表结构时**优先用 `csmar_bulk_schema`**，合并为一次 tool call。
- 不要在无明确理由的情况下调用 `csmar_refresh_cache`，它会强制穿透到 CSMAR API。

## 环境要求

- Python >= 3.12
- [uv](https://docs.astral.sh/uv/)

## 快速开始

```bash
uv sync
uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
```

## MCP 配置

```json
{
  "mcpServers": {
    "csmar": {
      "command": "uv",
      "args": [
        "--directory",
        "D:\\Developments\\PythonProject\\CSMAR-Data-MCP",
        "run",
        "csmar-mcp",
        "--account",
        "YOUR_ACCOUNT",
        "--password",
        "YOUR_PASSWORD"
      ]
    }
  }
}
```

## 开发：lint 与钩子

- 安装开发依赖：`uv sync --group dev`
- 本地 lint：`uv run python scripts/check.py`（默认只检查；`--fix` 模式跑 ruff 自动修复 + 格式化）
- 启用本地 git 钩子（一次性，基于 pre-commit framework）：
  ```bash
  uv run pre-commit install                       # 装 pre-commit 钩子：commit 前跑 --fix
  uv run pre-commit install --hook-type pre-push  # 装 pre-push 钩子：push 前跑 check-only
  ```
  配置见仓内 `.pre-commit-config.yaml`。pre-commit framework 原生做 stash-and-restore，partial stage 安全。
- CI：`.github/workflows/lint.yml` 在 push 与 pull_request 上跑同一套 `scripts/check.py`
- 扫描范围：`csmar_mcp` 与 `tests`，遗留 SDK `csmarapi/` 排除在外

## 说明

- 服务器在鉴权过期时会自动重新登录并重试一次。
- 预检与物化流程尽量复用缓存，以缓解上游限流。
- 工具调用会审计到本地 SQLite，包含请求参数、结果摘要与上游错误元数据。
- 无效的 `database_name` 或 `table_code` 会返回面向修复的错误与可执行的修复建议。
- 工具响应不返回完整数据集。
