# CSMAR MCP

基于 CSMAR 官方 Python SDK 封装的 MCP 服务器，提供以下工作流工具：

- csmar_list_databases - 列出数据库
- csmar_list_tables - 列出数据表
- csmar_describe_table - 描述表结构
- csmar_probe_queries - 探测查询
- csmar_materialize_downloads - 物化下载

## Agent 调用友好性优化

为降低 Agent 参数猜测失败率，`csmar_probe_queries` 和 `csmar_materialize_downloads` 已支持更直观的输入格式，并保持向后兼容。

调用入口兼容两种方式：

- 顶层参数方式：直接传 `table_name`、`columns` 等字段（推荐）
- `params` 包裹方式：传 `{"params": {...}}`（兼容历史调用）

### csmar_probe_queries

支持两种格式（任选其一）：

1. 批量格式（原有）

```json
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
```

2. 扁平单查询格式（新增）

```json
{
  "table_name": "FS_Combas",
  "columns": ["Stkcd", "ShortName", "Accper"],
  "condition": "Stkcd='000001'",
  "start_date": "2020-01-01",
  "end_date": "2024-12-31",
  "sample_rows": 5
}
```

说明：`probe_id` 现在可省略，服务端会自动生成。

### csmar_materialize_downloads

支持两种格式（任选其一）：

1. 批量格式（推荐 `downloads`，兼容旧字段 `plans`）

```json
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
```

2. 扁平单下载格式（新增）

```json
{
  "table_name": "FS_Combas",
  "columns": ["Stkcd", "ShortName", "Accper"],
  "condition": "Stkcd='000001'",
  "start_date": "2020-01-01",
  "end_date": "2024-12-31",
  "output_dir": "D:/tmp/csmar"
}
```

说明：

- `download_id` 现在可省略，服务端会自动生成
- 输入可用 `download_id`（推荐）或 `plan_id`（兼容）
- 输出主字段为 `download_id`，并保留 `plan_id` 兼容字段

### 上游冷却限制处理

CSMAR 对相同条件有冷却限制（常见为 30 分钟）。MCP 现在增加了本地冷却窗口处理：

- 当识别到上游“重复提交/限流”错误后，会记录该条件的本地冷却状态
- 冷却期内同条件请求会本地短路，避免重复打到上游
- 若存在缓存结果，会优先返回缓存结果并在 warnings 中说明

## 环境要求

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/)（Python 包管理器）

## 运行配置

命令行参数：

- `--account` - CSMAR 账号
- `--password` - CSMAR 密码

内置默认值：

- `lang = "0"`
- `belong = "0"`
- `poll_interval_seconds = 3`
- `poll_timeout_seconds = 900`
- `cache_ttl_minutes = 30`

## 快速开始

### 1. 安装 uv

```bash
# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 同步依赖

```bash
cd /path/to/CSMAR-Data-MCP
uv sync
```

### 3. 运行服务器

```bash
uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
```

## MCP 配置

### 配置到 Trae / Claude Desktop 等 MCP 客户端

将以下内容添加到 MCP 配置文件中：

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

**注意：** 请将 `D:\\Developments\\PythonProject\\CSMAR-Data-MCP` 替换为实际的项目路径。

### 工作原理

使用此配置时：

1. `uv` 自动创建独立的虚拟环境（`.venv`）
2. `uv` 自动安装所有依赖
3. `uv` 在独立环境中运行 `csmar-mcp`
4. 无需手动执行 `pip install`
5. 不会污染全局 Python 环境

## 使用 MCP Inspector 调试

```bash
npx -y @modelcontextprotocol/inspector
```

在 Inspector 中配置：

- Command: `uv`
- Args:
  - `--directory`
  - `D:\Developments\PythonProject\CSMAR-Data-MCP`
  - `run`
  - `csmar-mcp`
  - `--account`
  - `YOUR_ACCOUNT`
  - `--password`
  - `YOUR_PASSWORD`

## 注意事项

- 服务器会自动登录，并在认证过期时自动重试一次
- 查询探测和下载清单会缓存 30 分钟，以缓解 CSMAR 的频率限制
- 交互式工具仅返回元数据、计数、小样本和本地产物清单，不返回完整数据集
