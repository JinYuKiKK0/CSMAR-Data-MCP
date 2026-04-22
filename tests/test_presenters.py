from __future__ import annotations

import unittest

from csmar_mcp.models import ToolError
from csmar_mcp.presenters import AGENT_RECOVERABLE_CODES, failure


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
