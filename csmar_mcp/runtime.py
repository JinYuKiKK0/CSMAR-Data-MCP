from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from .client import CsmarClient


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    account: str
    password: str
    state_dir: Path | None = None
    metadata_ttl_days: int = 30


DEFAULT_LANG = "0"
DEFAULT_BELONG = "0"
DEFAULT_POLL_INTERVAL_SECONDS = 3
DEFAULT_POLL_TIMEOUT_SECONDS = 900
DEFAULT_CACHE_TTL_MINUTES = 30
DEFAULT_METADATA_TTL_DAYS = 30


_runtime_settings: RuntimeSettings | None = None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csmar-mcp",
        description="Run the CSMAR MCP server. Credentials must be passed via --account and --password.",
    )
    parser.add_argument("--account", default=None, help="CSMAR account")
    parser.add_argument("--password", default=None, help="CSMAR password")
    return parser


def parse_runtime_settings(argv: Sequence[str] | None = None) -> RuntimeSettings:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    account = args.account
    password = args.password
    if not account or not password:
        parser.error("Missing CSMAR credentials. Provide --account and --password.")
    raw_state_dir = os.getenv("CSMAR_MCP_STATE_DIR", "").strip()
    state_dir = Path(raw_state_dir).expanduser().resolve() if raw_state_dir else None

    raw_ttl_days = os.getenv("CSMAR_MCP_METADATA_TTL_DAYS", "").strip()
    metadata_ttl_days = DEFAULT_METADATA_TTL_DAYS
    if raw_ttl_days:
        try:
            parsed = int(raw_ttl_days)
        except ValueError as error:
            parser.error(
                f"CSMAR_MCP_METADATA_TTL_DAYS must be an integer, got {raw_ttl_days!r}: {error}"
            )
        if parsed < 1:
            parser.error("CSMAR_MCP_METADATA_TTL_DAYS must be >= 1")
        metadata_ttl_days = parsed

    return RuntimeSettings(
        account=account,
        password=password,
        state_dir=state_dir,
        metadata_ttl_days=metadata_ttl_days,
    )


def configure_runtime(settings: RuntimeSettings) -> None:
    global _runtime_settings
    _runtime_settings = settings
    get_settings.cache_clear()
    get_client.cache_clear()


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    if _runtime_settings is None:
        raise RuntimeError(
            "Runtime configuration is missing. Start the server with required CLI args, for example: "
            "--account ... --password ..."
        )
    return _runtime_settings


@lru_cache(maxsize=1)
def get_client() -> CsmarClient:
    settings = get_settings()
    return CsmarClient(
        account=settings.account,
        password=settings.password,
        lang=DEFAULT_LANG,
        belong=DEFAULT_BELONG,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        poll_timeout_seconds=DEFAULT_POLL_TIMEOUT_SECONDS,
        cache_ttl_minutes=DEFAULT_CACHE_TTL_MINUTES,
        state_dir=settings.state_dir,
        metadata_ttl_days=settings.metadata_ttl_days,
    )
