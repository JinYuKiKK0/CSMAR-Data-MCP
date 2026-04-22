# CSMAR MCP

面向 Agent 工作流的精简 MCP 服务器：提供 CSMAR 元数据发现、查询可行性探测，以及本地物化。

## 对外接口

### 工具

1. `csmar_list_databases`
   确定性枚举已购买的数据库。

2. `csmar_list_tables`
   确定性枚举指定数据库下的表。

3. `csmar_get_table_schema`
   返回纯表结构与字段元数据，不返回样本行。

4. `csmar_probe_query`
   对查询进行预检，返回 `validation_id`、`query_fingerprint`、行数、少量样本、无效列，以及物化可行性。

5. `csmar_materialize_query`
   按 `validation_id` 将先前预检过的查询物化为本地文件。

## 设计原则

- 每个工具职责单一。
- JSON 输出精简：只返回下一步所需字段。
- 面向修复的错误：`code`、`message`、`hint`，以及可选的 `retry_after_seconds`、`suggested_args_patch`。
- 日期区间只做格式与顺序校验，随后原样透传给 SDK。
- 查询的预检与物化通过 `validation_id` 串联。
- 运行时状态持久化在 SQLite 中，路径为 `WORKSPACE_DIR/.stata_agent/csmar_mcp/`。

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
- `cache_ttl_minutes = 30`
- `state_dir = WORKSPACE_DIR/.stata_agent/csmar_mcp/`

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

## Docker 部署

仓库内 `Dockerfile` + `docker-compose.yml` 提供容器化部署，容器默认通过 HTTP
（`streamable-http` 传输）对外暴露 MCP

### 前置

- 已安装 Docker 与 Docker Compose v2（`docker compose` 子命令可用）。
- 仓库根目录有可用的 `uv.lock`。镜像构建使用 `uv sync --frozen`，而 `uv.lock` 在
  `.gitignore` 中不入库，因此首次构建前需要本地先跑一次 `uv sync` 生成它。

### 配置

在仓库根创建 `.env`（`docker-compose.yml` 会加载它）：

```env
CSMAR_ACCOUNT=your_account
CSMAR_PASSWORD=your_password
```

可选覆盖（均有默认值，按需调整）：

| 变量                  | 默认                 | 说明                                                        |
| --------------------- | -------------------- | ----------------------------------------------------------- |
| `MCP_TRANSPORT`       | `streamable-http`    | MCP 传输；容器内一般不改                                    |
| `MCP_HOST`            | `0.0.0.0`            | 监听地址                                                    |
| `MCP_PORT`            | `8000`               | 监听端口；改了要同步调整 compose 端口映射                   |
| `CSMAR_MCP_STATE_DIR` | `/var/lib/csmar-mcp` | SQLite 缓存与审计目录；对应 compose 的 `csmar_state` 命名卷 |

### 构建与启动

```bash
docker compose up -d --build
```

### 客户端接入

容器跑的是 HTTP 形态 MCP，客户端通过 URL 连接而非 stdio 子进程。以兼容 HTTP
MCP 的客户端（如 Claude Desktop 的 `url` 配置，或其他支持 streamable-http
的 Agent）为例：

```json
{
  "mcpServers": {
    "csmar": {
      "url": "http://127.0.0.1:8000/mcp"
    }
  }
}
```

### 持久化

- `csmar_state` 命名卷保存 SQLite 状态（缓存、validation registry、限流冷却、审计
  trace）。升级镜像时卷会保留，历史缓存可复用。
- `docker compose down -v` 会连带删除卷，慎用。

### 钩子联动（可选）

仓库另附 `hooks/post-commit`，在检测到 `.env` 与 `docker` 存在时会自动跑
`docker compose up -d --build` 重建容器，适合自部署场景下每次提交后自动滚动。
启用方式同前：`git config core.hooksPath hooks`。

## 开发：lint 与钩子

- 安装开发依赖：`uv sync --group dev`
- 本地 lint：`uv run python scripts/check.py`（默认只检查；`--fix` 模式跑 ruff 自动修复 + 格式化）
- 启用仓库 git 钩子（一次性）：`git config core.hooksPath hooks`
  - `pre-commit`：自动修复可修项并把改动 re-add 回本次 commit（仅限原本已暂存的 py 文件）
  - `pre-push`：只读检查，作为绕过 commit 钩子的兜底
- CI：`.github/workflows/lint.yml` 在 push 与 pull_request 上跑同一套 `scripts/check.py`
- 扫描范围：`csmar_mcp` 与 `tests`，遗留 SDK `csmarapi/` 排除在外

## 说明

- 服务器在鉴权过期时会自动重新登录并重试一次。
- 预检与物化流程尽量复用缓存，以缓解上游限流。
- 工具调用会审计到本地 SQLite，包含请求参数、结果摘要与上游错误元数据。
- 无效的 `database_name` 或 `table_code` 会返回面向修复的错误与可执行的修复建议。
- 工具响应不返回完整数据集。
