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
DEFAULT_CACHE_TTL_MINUTES = 3 * 24 * 60  # 3 days — applies to probes/validations/downloads
DEFAULT_METADATA_TTL_DAYS = 30
DEFAULT_RATE_LIMIT_COOLDOWN_MINUTES = 30
ACCOUNT_ENV_VAR = "CSMAR_MCP_ACCOUNT"
PASSWORD_ENV_VAR = "CSMAR_MCP_PASSWORD"
DOTENV_FILENAME = ".env"
MISSING_CREDENTIALS_MESSAGE = (
    "Missing CSMAR credentials. Provide --account/--password or set "
    f"{ACCOUNT_ENV_VAR}/{PASSWORD_ENV_VAR} in the environment or .env."
)


_runtime_settings: RuntimeSettings | None = None


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="csmar-mcp",
        description=(
            "Run the CSMAR MCP server. Credentials can be passed via CLI args or the "
            f"{ACCOUNT_ENV_VAR}/{PASSWORD_ENV_VAR} environment variables."
        ),
    )
    parser.add_argument("--account", default=None, help="CSMAR account")
    parser.add_argument("--password", default=None, help="CSMAR password")
    return parser


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _load_dotenv_values() -> dict[str, str]:
    env_path = Path.cwd() / DOTENV_FILENAME
    if not env_path.is_file():
        return {}

    values: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        key, separator, raw_value = stripped.partition("=")
        if not separator:
            continue
        values[key.strip()] = _strip_optional_quotes(raw_value.strip())
    return values


def _resolve_secret_value(
    cli_value: str | None,
    *,
    env_var_name: str,
    dotenv_values: dict[str, str],
) -> str | None:
    if cli_value:
        return cli_value

    env_value = os.getenv(env_var_name, "").strip()
    if env_value:
        return env_value

    dotenv_value = dotenv_values.get(env_var_name, "").strip()
    if dotenv_value:
        return dotenv_value
    return None


def resolve_credentials(account: str | None, password: str | None) -> tuple[str | None, str | None]:
    dotenv_values = _load_dotenv_values()
    resolved_account = _resolve_secret_value(
        account,
        env_var_name=ACCOUNT_ENV_VAR,
        dotenv_values=dotenv_values,
    )
    resolved_password = _resolve_secret_value(
        password,
        env_var_name=PASSWORD_ENV_VAR,
        dotenv_values=dotenv_values,
    )
    return resolved_account, resolved_password


def parse_runtime_settings(argv: Sequence[str] | None = None) -> RuntimeSettings:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    account, password = resolve_credentials(args.account, args.password)
    if not account or not password:
        parser.error(MISSING_CREDENTIALS_MESSAGE)
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
            "Runtime configuration is missing. Start the server with CLI credentials or set "
            f"{ACCOUNT_ENV_VAR}/{PASSWORD_ENV_VAR}."
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
        rate_limit_cooldown_minutes=DEFAULT_RATE_LIMIT_COOLDOWN_MINUTES,
    )
