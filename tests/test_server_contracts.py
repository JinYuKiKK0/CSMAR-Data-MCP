from __future__ import annotations

import unittest
from datetime import datetime, timezone
from typing import Any, Callable, cast

import csmar_mcp.server as server_module
from csmar_mcp.presenters import tool_error_boundary


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

        safe_log_trace = cast(
            Callable[..., None], getattr(server_module, "_safe_log_trace")
        )

        with self.assertLogs("csmar_mcp.server", level="WARNING") as captured:
            safe_log_trace(
                cast(Any, _FailingClient()),
                tool_name="csmar_probe_query",
                request_payload={"table_code": "FS_Combas"},
                started_at=datetime.now(timezone.utc),
                result_summary={"row_count": 1},
                cached=False,
            )

        self.assertTrue(any("tool trace" in line.lower() for line in captured.output))


if __name__ == "__main__":
    unittest.main()
