from __future__ import annotations

import argparse
from dataclasses import dataclass
from functools import lru_cache
from typing import Sequence

from .client import CsmarClient


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    account: str
    password: str


DEFAULT_LANG = "0"
DEFAULT_BELONG = "0"
DEFAULT_POLL_INTERVAL_SECONDS = 3
DEFAULT_POLL_TIMEOUT_SECONDS = 900
DEFAULT_CACHE_TTL_MINUTES = 30


_runtime_settings: RuntimeSettings | None = None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csmar-mcp",
        description=(
            "Run the CSMAR MCP server over stdio. Only account and password are accepted as runtime args; "
            "other settings are fixed in code."
        ),
    )
    parser.add_argument("--account", required=True, help="CSMAR account")
    parser.add_argument("--password", required=True, help="CSMAR password")
    return parser


def parse_runtime_settings(argv: Sequence[str] | None = None) -> RuntimeSettings:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    return RuntimeSettings(account=args.account, password=args.password)


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
    )
