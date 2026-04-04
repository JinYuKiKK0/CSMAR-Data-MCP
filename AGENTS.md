# 项目指南

## 项目定位

- 本项目是一个对接 CSMAR 数据库接口的 MCP 服务器。
- 主要职责：目录搜索、表结构检查、查询校验、下载物化到本地。
- 交互式工具不返回完整数据集（完整数据通过structuredContent返回），只返回元数据、计数、小样本和本地工件清单。

## Code Style

- 使用 Python 3.11+，新增/修改代码保持明确类型注解。
- 新增工具输入输出优先在 [csmar_mcp/models.py](csmar_mcp/models.py) 中定义 Pydantic 模型，默认遵循严格校验。
- 对外参数名统一使用 snake_case；工具输入保持单一契约，不维护历史兼容包装层。
- 成功返回保持极简；空字段不要输出；失败返回优先使用 `code`、`message`、`hint` 与最少量修复元数据。

## Architecture

- [csmar_mcp/server.py](csmar_mcp/server.py)：薄 MCP 入口；负责 tools 注册、入参校验后的调用编排与服务启动。
- [csmar_mcp/runtime.py](csmar_mcp/runtime.py)：CLI 参数解析、运行时固定默认值与 `CsmarClient` 单例装配。
- [csmar_mcp/presenters.py](csmar_mcp/presenters.py)：`CallToolResult`/`ToolError` 整形、错误 enrich 与对外返回包装。
- [csmar_mcp/client.py](csmar_mcp/client.py)：兼容 façade；组装内部 services，并在 core 类型与 MCP DTO 之间做转换。
- [csmar_mcp/models.py](csmar_mcp/models.py)：对外 MCP 请求/响应/错误模型与输入约束（日期格式、样本行数上限）。
- [csmar_mcp/core/](csmar_mcp/core/)：内部核心类型与统一错误对象；不依赖 MCP transport。
- [csmar_mcp/services/](csmar_mcp/services/)：应用服务层。`metadata.py` 负责目录/schema/搜索，`query.py` 负责 probe/materialize、缓存 key 与本地查询规则。
- [csmar_mcp/infra/](csmar_mcp/infra/)：基础设施层。`csmar_gateway.py` 负责上游 SDK 访问、登录/重登与响应归一化，`state.py` 负责 SQLite 持久化缓存、validation registry 与限流冷却状态。
- [csmarapi/](csmarapi/)：上游 SDK 兼容层，视为遗留边界；仅允许通过 [csmar_mcp/infra/csmar_gateway.py](csmar_mcp/infra/csmar_gateway.py) 访问，不在 tools 或 services 层直接散落调用。

## Build and Test

- 开发依赖安装：uv sync
- 本地运行：uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 生产/全局 MCP 配置优先：python -m csmar_mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 回归测试入口：`uv run python -m unittest discover -s tests -p "test_*.py" -v`
- 修改后至少执行一次启动级冒烟验证。

## Conventions

- 认证仅通过 CLI 参数 --account 与 --password，不新增 .env 读取路径。
- 运行时固定默认值：lang=0、belong=0、poll_interval_seconds=3、poll_timeout_seconds=900、cache_ttl_minutes=30。
- 当前对外工具面：
  - `csmar_list_databases`
  - `csmar_list_tables`
  - `csmar_search_tables`
  - `csmar_search_fields`
  - `csmar_get_table_schema`
  - `csmar_probe_query`
  - `csmar_materialize_query`
- 查询日期范围不做硬编码限制；仅校验 `YYYY-MM-DD` 格式与起止顺序，然后原样透传给 SDK。
- 预览/样本行数保持小上限，以节省上下文。
- 遇到上游限流时优先复用缓存并返回标准化 error_code，避免重复打上游。
- 错误码约定：auth_failed、not_purchased、table_not_found、field_not_found、invalid_condition、rate_limited、daily_limit_exceeded、download_failed、unzip_failed、upstream_error、invalid_arguments。

## 文档导航

- 详细启动、配置与调试：[README.md](README.md)
- 常见问题与调试记录：[notes/](notes/)
- 上游 Python SDK 背景：[CSMAR_PYTHON.md](CSMAR_PYTHON.md)
- MCP 客户端配置示例：[mcp.agent.config.example.json](mcp.agent.config.example.json)
