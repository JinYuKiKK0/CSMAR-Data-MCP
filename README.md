# CSMAR MCP

Lean MCP server for CSMAR metadata discovery, probe validation, and local materialization for agent workflows.

## Public Surface

### Tools

1. `csmar_list_databases`
   Deterministically enumerate purchased databases.

2. `csmar_list_tables`
   Deterministically enumerate tables under a purchased database.

3. `csmar_search_tables`
  Discover candidate tables by business topic, table name, or table code (hard cap: 5 candidates).

4. `csmar_search_fields`
  Discover candidate fields by deterministic literal/similarity matching, optionally scoped by database and table.

5. `csmar_get_table_schema`
   Return pure table schema with field metadata. No preview rows.

6. `csmar_probe_query`
   Probe a query and return `validation_id`, `query_fingerprint`, row count, tiny sample, invalid columns, and materialization feasibility.

7. `csmar_materialize_query`
   Materialize a previously probed query by `validation_id` into local files.

### Non-Goals for Public Surface

- No public resources.
- No public prompts.
- No transport-layer tools like start/poll/unzip exposed to callers.

## Design Principles

- Single responsibility per tool.
- Lean JSON outputs: return only fields needed for next step.
- Repair-oriented errors: `code`, `message`, `hint`, plus optional `retry_after_seconds`, `suggested_args_patch`.
- Date ranges are validated for format and ordering only, then passed through to SDK.
- Query probe and materialization are linked by `validation_id`.
- Runtime state is persisted in SQLite under `WORKSPACE_DIR/.stata_agent/csmar_mcp/`.

## Tool Examples

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

### `csmar_search_tables`

```json
{
  "query": "balance sheet",
  "limit": 5
}
```

### `csmar_search_fields`

```json
{
  "query": "净利润",
  "database_name": "财务报表",
  "role_hint": "outcome",
  "frequency_hint": "annual",
  "limit": 10
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

## Runtime Defaults

- `lang = "0"`
- `belong = "0"`
- `poll_interval_seconds = 3`
- `poll_timeout_seconds = 900`
- `cache_ttl_minutes = 30`
- `state_dir = WORKSPACE_DIR/.stata_agent/csmar_mcp/`

## Environment

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/)

## Quick Start

```bash
uv sync
uv run csmar-mcp --account YOUR_ACCOUNT --password YOUR_PASSWORD
```

## MCP Configuration

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

## Notes

- The server logs in automatically and retries once when authentication expires.
- Probe and materialization flows reuse cache when possible to mitigate upstream rate limits.
- Tool calls are audit-logged to local SQLite, including request payload, result summary, and upstream error metadata.
- Invalid `database_name` or `table_code` returns repair-oriented errors with actionable suggestions.
- Tool responses avoid returning complete datasets.
