# 项目指南

## 项目定位

- 本项目是一个对接 CSMAR 数据库接口的 MCP 服务器。
- 主要职责：目录发现、轻量查询探测、批量下载物化到本地。
- 交互式工具不返回完整数据集，只返回元数据、计数、小样本和本地工件清单。

## Code Style

- 使用 Python 3.11+，新增/修改代码保持明确类型注解。
- 新增工具输入输出优先在 [csmar_mcp/models.py](csmar_mcp/models.py) 中定义 Pydantic 模型，默认遵循严格校验。
- 对外参数名统一使用 snake_case；字段命名要与现有 MCP 工具保持一致。
- 修改工具参数时，同时维护两类调用入口：顶层扁平参数与 params 包裹参数。

## Architecture

- [csmar_mcp/server.py](csmar_mcp/server.py)：MCP 工具定义、参数归一化、错误格式化、服务启动入口。
- [csmar_mcp/client.py](csmar_mcp/client.py)：CSMAR 上游调用、登录与重登、缓存、限流冷却、下载与解压。
- [csmar_mcp/models.py](csmar_mcp/models.py)：请求/响应模型与输入约束（日期格式、行数上限、兼容字段）。
- [csmarapi/](csmarapi/)：上游 SDK 兼容层，尽量通过 [csmar_mcp/client.py](csmar_mcp/client.py) 访问，不在工具层直接散落调用。

## Build and Test

- 开发依赖安装：uv sync
- 本地运行：uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 生产/全局 MCP 配置优先：python -m csmar_mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
- 当前仓库未提供项目级自动化测试目录；修改后至少执行一次启动级冒烟验证。

## Conventions

- 认证仅通过 CLI 参数 --account 与 --password，不新增 .env 读取路径。
- 运行时固定默认值：lang=0、belong=0、poll_interval_seconds=3、poll_timeout_seconds=900、cache_ttl_minutes=30。
- 保持双轨参数兼容：
  - csmar_probe_queries：支持 queries 批量格式与扁平单查询格式。
  - csmar_materialize_downloads：支持 downloads/plans 批量格式与扁平单下载格式。
- 查询日期范围限制在 5 年内；预览/样本行数上限为 10。
- 遇到上游限流时优先复用缓存并返回标准化 error_code，避免重复打上游。
- 错误码约定与 PLAN.md 保持一致：auth_failed、not_purchased、table_not_found、field_not_found、invalid_condition、rate_limited、download_failed、unzip_failed、upstream_error。

## 文档导航

- 详细启动、配置与调试：[README.md](README.md)
- 设计决策与鲁棒性规则：[PLAN.md](PLAN.md)
- 上游 Python SDK 背景：[CSMAR_PYTHON.md](CSMAR_PYTHON.md)
- MCP 客户端配置示例：[mcp.agent.config.example.json](mcp.agent.config.example.json)
