from __future__ import annotations

from typing import Any


class CsmarError(Exception):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        hint: str | None = None,
        upstream_code: int | None = None,
        retry_after_seconds: int | None = None,
        suggested_args_patch: dict[str, Any] | None = None,
        raw_message: str | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.hint = hint
        self.upstream_code = upstream_code
        self.retry_after_seconds = retry_after_seconds
        self.suggested_args_patch = suggested_args_patch
        self.raw_message = raw_message
