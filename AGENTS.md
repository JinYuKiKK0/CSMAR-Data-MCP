# 项目指南

## 项目定位

- 本项目是一个对接 CSMAR 数据库接口的 MCP 服务器。
- 主要职责：目录搜索、表结构检查、查询校验、下载物化到本地。
- 交互式工具不返回完整数据集，只返回元数据、计数、小样本和本地工件清单。

## Code Style

- 使用 Python 3.11+，新增/修改代码保持明确类型注解。
- 新增工具输入输出优先在 [csmar_mcp/models.py](csmar_mcp/models.py) 中定义 Pydantic 模型，默认遵循严格校验。
- 对外参数名统一使用 snake_case；工具输入保持单一契约，不维护历史兼容包装层。
- 成功返回保持极简；空字段不要输出；失败返回优先使用 `code`、`message`、`hint` 与最少量修复元数据。

## Architecture

- [csmar_mcp/server.py](csmar_mcp/server.py)：MCP tools 注册，对外错误整形，服务启动入口。
- [csmar_mcp/client.py](csmar_mcp/client.py)：CSMAR 上游调用、登录与重登、缓存、限流冷却、目录搜索、下载与解压。
- [csmar_mcp/models.py](csmar_mcp/models.py)：Lean V2 请求/响应/错误模型与输入约束（日期格式、样本行数上限）。
- [csmarapi/](csmarapi/)：上游 SDK 兼容层，尽量通过 [csmar_mcp/client.py](csmar_mcp/client.py) 访问，不在工具层直接散落调用。

## Build and Test

- 开发依赖安装：uv sync
- 本地运行：uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 生产/全局 MCP 配置优先：python -m csmar_mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 当前仓库未提供项目级自动化测试目录；修改后至少执行一次启动级冒烟验证。

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
- 错误码约定与 PLAN.md 保持一致：auth_failed、not_purchased、table_not_found、field_not_found、invalid_condition、rate_limited、daily_limit_exceeded、download_failed、unzip_failed、upstream_error、invalid_arguments。

## 文档导航

- 详细启动、配置与调试：[README.md](README.md)
- 设计决策与鲁棒性规则：[PLAN.md](PLAN.md)
- 上游 Python SDK 背景：[CSMAR_PYTHON.md](CSMAR_PYTHON.md)
- MCP 客户端配置示例：[mcp.agent.config.example.json](mcp.agent.config.example.json)
