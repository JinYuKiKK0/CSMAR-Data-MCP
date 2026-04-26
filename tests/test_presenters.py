from __future__ import annotations

import json
import unittest

from csmar_mcp.models import ToolError
from csmar_mcp.presenters import AGENT_RECOVERABLE_CODES, failure, success


class FailureIsErrorClassificationTests(unittest.TestCase):
    SOFT_FAILURE_CODES = (
        "database_not_found",
        "table_not_found",
        "field_not_found",
        "not_purchased",
        "invalid_condition",
        "invalid_arguments",
        "rate_limited",
    )

    HARD_EXCEPTION_CODES = (
        "auth_failed",
        "daily_limit_exceeded",
        "upstream_error",
        "download_failed",
        "unzip_failed",
    )

    def test_soft_failure_codes_produce_is_error_false(self) -> None:
        for code in self.SOFT_FAILURE_CODES:
            with self.subTest(code=code):
                result = failure(ToolError(code=code, message="m", hint="h"))
                self.assertFalse(
                    result.isError,
                    msg=f"expected isError=False for soft-failure code {code!r}",
                )

    def test_hard_exception_codes_produce_is_error_true(self) -> None:
        for code in self.HARD_EXCEPTION_CODES:
            with self.subTest(code=code):
                result = failure(ToolError(code=code, message="m", hint="h"))
                self.assertTrue(
                    result.isError,
                    msg=f"expected isError=True for hard-exception code {code!r}",
                )

    def test_unknown_code_is_treated_as_hard_exception(self) -> None:
        result = failure(ToolError(code="some_unknown_code", message="m", hint="h"))
        self.assertTrue(result.isError)

    def test_recoverable_codes_set_matches_documented_classification(self) -> None:
        self.assertEqual(AGENT_RECOVERABLE_CODES, frozenset(self.SOFT_FAILURE_CODES))


class SuccessContentShapeTests(unittest.TestCase):
    def test_success_content_contains_summary_and_payload_json(self) -> None:
        payload = {"databases": ["A", "B"], "count": 2}
        result = success(payload, "Returned 2 databases.")
        self.assertEqual(len(result.content), 2)
        self.assertEqual(result.content[0].text, "Returned 2 databases.")
        self.assertEqual(json.loads(result.content[1].text), payload)
        self.assertEqual(result.structuredContent, payload)
        self.assertFalse(result.isError)

    def test_success_preserves_unicode(self) -> None:
        payload = {"name": "中文数据库"}
        result = success(payload, "ok")
        self.assertIn("中文", result.content[1].text)


class FailureContentShapeTests(unittest.TestCase):
    def test_failure_content_contains_full_error_payload(self) -> None:
        error = ToolError(code="table_not_found", message="m", hint="call list_tables")
        result = failure(error)
        decoded = json.loads(result.content[0].text)
        self.assertEqual(decoded["code"], "table_not_found")
        self.assertEqual(decoded["hint"], "call list_tables")
        self.assertFalse(result.isError)

    def test_failure_hard_exception_still_marks_is_error(self) -> None:
        error = ToolError(code="auth_failed", message="m", hint="h")
        result = failure(error)
        self.assertTrue(result.isError)
        decoded = json.loads(result.content[0].text)
        self.assertEqual(decoded["code"], "auth_failed")
