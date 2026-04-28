from __future__ import annotations

import unittest
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

import csmar_mcp.server as server_module
from csmar_mcp.models import (
    BulkSchemaInput,
    RefreshCacheInput,
    SearchFieldInput,
)
from csmar_mcp.presenters import tool_error_boundary
from pydantic import ValidationError


class PresenterBoundaryTests(unittest.TestCase):
    def test_tool_error_boundary_calls_auditor_for_unexpected_exception(self) -> None:
        captured: list[tuple[str, dict[str, object], str]] = []

        def audit_callback(
            tool_name: str,
            request_payload: dict[str, object],
            error: Exception,
        ) -> None:
            captured.append((tool_name, request_payload, str(error)))

        @tool_error_boundary("fake_tool", on_unexpected_error=audit_callback)
        def failing_tool(table_code: str, limit: int = 5):
            raise RuntimeError("boom")

        result = failing_tool("FS_Combas", limit=3)

        self.assertTrue(result.isError)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][0], "fake_tool")
        self.assertEqual(captured[0][1], {"table_code": "FS_Combas", "limit": 3})
        self.assertEqual(captured[0][2], "boom")

    def test_safe_log_trace_emits_warning_when_audit_write_fails(self) -> None:
        class _FailingClient:
            def log_tool_trace(self, **kwargs: object) -> str:
                raise RuntimeError("sqlite locked")

        safe_log_trace = cast(Callable[..., None], server_module._safe_log_trace)

        with self.assertLogs("csmar_mcp.server", level="WARNING") as captured:
            safe_log_trace(
                cast(Any, _FailingClient()),
                tool_name="csmar_probe_query",
                request_payload={"table_code": "FS_Combas"},
                started_at=datetime.now(UTC),
                result_summary={"row_count": 1},
                cached=False,
            )

        self.assertTrue(any("tool trace" in line.lower() for line in captured.output))


class NewToolRegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def _tool(self, name: str) -> Any:
        tools = await server_module.mcp.list_tools()
        for tool in tools:
            if tool.name == name:
                return tool
        raise AssertionError(f"Tool {name!r} not registered")

    async def test_refresh_cache_description_flags_danger(self) -> None:
        tool = await self._tool("csmar_refresh_cache")
        self.assertIn("Danger", tool.description or "")
        self.assertTrue(tool.annotations.destructiveHint)

    async def test_bulk_schema_registered_as_read_only(self) -> None:
        tool = await self._tool("csmar_bulk_schema")
        self.assertIn("Cache-first", tool.description or "")
        self.assertTrue(tool.annotations.readOnlyHint)

    async def test_search_field_not_registered(self) -> None:
        tools = await server_module.mcp.list_tools()
        names = {tool.name for tool in tools}
        self.assertNotIn("csmar_search_field", names)


class NewToolInputValidationTests(unittest.TestCase):
    def test_bulk_schema_rejects_empty_list(self) -> None:
        with self.assertRaises(ValidationError):
            BulkSchemaInput.model_validate({"table_codes": []})

    def test_bulk_schema_rejects_over_twenty(self) -> None:
        with self.assertRaises(ValidationError):
            BulkSchemaInput.model_validate({"table_codes": [f"T{i}" for i in range(21)]})

    def test_refresh_cache_rejects_unknown_namespace(self) -> None:
        with self.assertRaises(ValidationError):
            RefreshCacheInput.model_validate({"namespace": "probes"})

    def test_search_field_requires_non_empty_keyword(self) -> None:
        with self.assertRaises(ValidationError):
            SearchFieldInput.model_validate({"keyword": ""})


if __name__ == "__main__":
    unittest.main()
