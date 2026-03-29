# CSMAR MCP

Lean MCP server for CSMAR metadata discovery, query validation, and local download materialization.

## Public Surface

### Tools

- `csmar_catalog_search`
  Use this first when you do not know the table code yet.
- `csmar_get_table_schema`
  Use this to inspect fields and request a tiny preview after you know the table code.
- `csmar_query_validate`
  Use this before downloading to confirm row count, sample rows, and download feasibility.
- `csmar_download_materialize`
  Use this only after validation succeeds to write files to a local directory.

### Resources

- `csmar://table/{table_code}/schema`
  Returns the full schema for a known table.
- `csmar://artifacts/{download_id}/manifest`
  Returns the full extracted file list for a completed download.

### Prompt

- `repair_csmar_request`
  Use this after a tool error to generate a concise retry plan.

## Design Principles

- Lean JSON: tool results only contain the minimum fields needed for the next decision.
- Agent-friendly errors: failures return `code`, `message`, `hint`, and only add `retry_after_seconds`, `candidate_values`, or `suggested_args_patch` when needed.
- No hard-coded time window: `start_date` and `end_date` are validated for format and ordering only, then passed through to the SDK.
- No legacy compatibility layer: the server exposes one contract per tool and does not support `params`, batch wrappers, or alias fields.

## Tool Examples

### `csmar_catalog_search`

```json
{
  "query": "balance sheet",
  "limit": 5
}
```

### `csmar_get_table_schema`

```json
{
  "table_code": "FS_Combas",
  "field_query": "Acc",
  "preview_columns": ["Stkcd", "Accper", "Typrep"],
  "preview_rows": 2
}
```

### `csmar_query_validate`

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

### `csmar_download_materialize`

```json
{
  "table_code": "FS_Combas",
  "columns": ["Stkcd", "Accper", "Typrep"],
  "condition": "Stkcd='000001'",
  "start_date": "2010-01-01",
  "end_date": "2024-12-31",
  "output_dir": "D:/tmp/csmar"
}
```

## Runtime Defaults

- `lang = "0"`
- `belong = "0"`
- `poll_interval_seconds = 3`
- `poll_timeout_seconds = 900`
- `cache_ttl_minutes = 30`

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
- Validation and download requests reuse local cache when possible to reduce repeated upstream calls.
- Tool responses never inline full datasets or full file manifests.
