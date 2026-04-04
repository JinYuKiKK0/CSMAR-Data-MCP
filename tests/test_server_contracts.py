from __future__ import annotations

import unittest
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


if __name__ == "__main__":
    unittest.main()
