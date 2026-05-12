from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch


def load_runtime_module(package_module: types.ModuleType) -> types.ModuleType:
    client_module = types.ModuleType("csmar_mcp.client")
    client_module.CsmarClient = object

    runtime_path = Path(__file__).resolve().parents[1] / "csmar_mcp" / "runtime.py"
    spec = importlib.util.spec_from_file_location("csmar_mcp.runtime", runtime_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load csmar_mcp.runtime for tests.")

    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "csmar_mcp": package_module,
            "csmar_mcp.client": client_module,
            "csmar_mcp.runtime": module,
        },
    ):
        spec.loader.exec_module(module)
    return module


def load_test_api_module(runtime_module: types.ModuleType) -> types.ModuleType:
    package_module = types.ModuleType("csmar_mcp")
    package_module.__path__ = [str(Path(__file__).resolve().parents[1] / "csmar_mcp")]
    client_module = types.ModuleType("csmar_mcp.client")
    client_module.CsmarClient = object

    uvicorn_module = types.ModuleType("uvicorn")
    uvicorn_module.run = lambda *args, **kwargs: None

    pydantic_module = types.ModuleType("pydantic")

    class ValidationError(Exception):
        pass

    pydantic_module.ValidationError = ValidationError

    starlette_applications = types.ModuleType("starlette.applications")
    starlette_requests = types.ModuleType("starlette.requests")
    starlette_responses = types.ModuleType("starlette.responses")
    starlette_routing = types.ModuleType("starlette.routing")

    class Starlette:
        def __init__(self, routes: list[object] | None = None) -> None:
            self.routes = routes or []

    class Request:
        query_params: dict[str, str]

    class JSONResponse:
        def __init__(self, body: Any, status_code: int = 200) -> None:
            self.body = body
            self.status_code = status_code

    class Route:
        def __init__(self, path: str, endpoint: Any, methods: list[str] | None = None) -> None:
            self.path = path
            self.endpoint = endpoint
            self.methods = methods or []

    starlette_applications.Starlette = Starlette
    starlette_requests.Request = Request
    starlette_responses.JSONResponse = JSONResponse
    starlette_routing.Route = Route

    errors_module = types.ModuleType("csmar_mcp.core.errors")

    class CsmarError(Exception):
        def __init__(
            self,
            error_code: str,
            message: str,
            *,
            upstream_code: int | None = None,
            raw_message: str | None = None,
        ) -> None:
            super().__init__(message)
            self.error_code = error_code
            self.message = message
            self.upstream_code = upstream_code
            self.raw_message = raw_message

    errors_module.CsmarError = CsmarError

    models_module = types.ModuleType("csmar_mcp.models")

    class _Model:
        @classmethod
        def model_validate(cls, value: Any) -> Any:
            return value

    models_module.GetTableSchemaInput = _Model
    models_module.ListTablesInput = _Model
    models_module.MaterializeQueryInput = _Model
    models_module.ProbeQueryInput = _Model

    test_api_path = Path(__file__).resolve().parents[1] / "csmar_mcp" / "test_api.py"
    spec = importlib.util.spec_from_file_location("csmar_mcp.test_api", test_api_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load csmar_mcp.test_api for tests.")

    module = importlib.util.module_from_spec(spec)
    with patch.dict(
        sys.modules,
        {
            "csmar_mcp": package_module,
            "csmar_mcp.client": client_module,
            "csmar_mcp.runtime": runtime_module,
            "csmar_mcp.test_api": module,
            "csmar_mcp.core.errors": errors_module,
            "csmar_mcp.models": models_module,
            "pydantic": pydantic_module,
            "starlette.applications": starlette_applications,
            "starlette.requests": starlette_requests,
            "starlette.responses": starlette_responses,
            "starlette.routing": starlette_routing,
            "uvicorn": uvicorn_module,
        },
    ):
        spec.loader.exec_module(module)
    return module


PACKAGE_MODULE = types.ModuleType("csmar_mcp")
PACKAGE_MODULE.__path__ = [str(Path(__file__).resolve().parents[1] / "csmar_mcp")]
RUNTIME_MODULE = load_runtime_module(PACKAGE_MODULE)
TEST_API_MODULE = load_test_api_module(RUNTIME_MODULE)
RuntimeSettings = RUNTIME_MODULE.RuntimeSettings
configure_runtime = RUNTIME_MODULE.configure_runtime
get_client = RUNTIME_MODULE.get_client
get_settings = RUNTIME_MODULE.get_settings
parse_runtime_settings = RUNTIME_MODULE.parse_runtime_settings


@contextmanager
def working_directory(path: str) -> object:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


class RuntimeSettingsTests(unittest.TestCase):
    def tearDown(self) -> None:
        RUNTIME_MODULE._runtime_settings = None
        get_client.cache_clear()
        get_settings.cache_clear()

    def test_resolve_credentials_reads_environment(self) -> None:
        with patch.dict(
            os.environ,
            {"CSMAR_MCP_ACCOUNT": "env-user", "CSMAR_MCP_PASSWORD": "env-pass"},
            clear=True,
        ):
            account, password = RUNTIME_MODULE.resolve_credentials(None, None)

        self.assertEqual(account, "env-user")
        self.assertEqual(password, "env-pass")

    def test_resolve_credentials_reads_dotenv_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / ".env"
            env_file.write_text(
                "CSMAR_MCP_ACCOUNT=dotenv-user\nCSMAR_MCP_PASSWORD=dotenv-pass\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True), working_directory(temp_dir):
                account, password = RUNTIME_MODULE.resolve_credentials(None, None)

        self.assertEqual(account, "dotenv-user")
        self.assertEqual(password, "dotenv-pass")

    def test_resolve_credentials_prefers_cli_values(self) -> None:
        with patch.dict(
            os.environ,
            {"CSMAR_MCP_ACCOUNT": "env-user", "CSMAR_MCP_PASSWORD": "env-pass"},
            clear=True,
        ):
            account, password = RUNTIME_MODULE.resolve_credentials("cli-user", "cli-pass")

        self.assertEqual(account, "cli-user")
        self.assertEqual(password, "cli-pass")

    def test_parse_runtime_settings_with_state_dir_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            expected_state_dir = (Path(temp_dir) / "state").resolve()
            with patch.dict(
                os.environ,
                {
                    "CSMAR_MCP_ACCOUNT": "env-user",
                    "CSMAR_MCP_PASSWORD": "env-pass",
                    "CSMAR_MCP_STATE_DIR": str(expected_state_dir),
                },
                clear=True,
            ):
                settings = parse_runtime_settings([])

        self.assertEqual(settings.account, "env-user")
        self.assertEqual(settings.password, "env-pass")
        self.assertEqual(settings.state_dir, expected_state_dir)

    def test_parse_runtime_settings_errors_without_credentials(self) -> None:
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit) as raised:
            parse_runtime_settings([])

        self.assertEqual(raised.exception.code, 2)

    def test_get_client_passes_state_dir_to_csmar_client(self) -> None:
        configure_runtime(
            RuntimeSettings(
                account="acc",
                password="pwd",
                state_dir=Path("/tmp/csmar-state"),
            )
        )

        with patch.object(RUNTIME_MODULE, "CsmarClient") as mock_client:
            get_client.cache_clear()
            get_client()

        _, kwargs = mock_client.call_args
        self.assertEqual(kwargs["state_dir"], Path("/tmp/csmar-state"))

    def test_metadata_ttl_env_overrides_default(self) -> None:
        with patch.dict(
            os.environ,
            {
                "CSMAR_MCP_ACCOUNT": "env-user",
                "CSMAR_MCP_PASSWORD": "env-pass",
                "CSMAR_MCP_METADATA_TTL_DAYS": "7",
            },
            clear=True,
        ):
            settings = parse_runtime_settings([])

        self.assertEqual(settings.metadata_ttl_days, 7)

    def test_metadata_ttl_default_is_30_days(self) -> None:
        with patch.dict(
            os.environ,
            {"CSMAR_MCP_ACCOUNT": "env-user", "CSMAR_MCP_PASSWORD": "env-pass"},
            clear=True,
        ):
            settings = parse_runtime_settings([])

        self.assertEqual(settings.metadata_ttl_days, 30)


class TestApiParseArgsTests(unittest.TestCase):
    def test_reads_credentials_from_environment(self) -> None:
        with patch.dict(
            os.environ,
            {"CSMAR_MCP_ACCOUNT": "env-user", "CSMAR_MCP_PASSWORD": "env-pass"},
            clear=True,
        ):
            args = TEST_API_MODULE._parse_args([])

        self.assertEqual(args.account, "env-user")
        self.assertEqual(args.password, "env-pass")
        self.assertEqual(args.host, "127.0.0.1")
        self.assertEqual(args.port, 8000)

    def test_reads_credentials_from_dotenv_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_file = Path(temp_dir) / ".env"
            env_file.write_text(
                "CSMAR_MCP_ACCOUNT=dotenv-user\nCSMAR_MCP_PASSWORD=dotenv-pass\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True), working_directory(temp_dir):
                args = TEST_API_MODULE._parse_args(["--host", "0.0.0.0", "--port", "9001"])

        self.assertEqual(args.account, "dotenv-user")
        self.assertEqual(args.password, "dotenv-pass")
        self.assertEqual(args.host, "0.0.0.0")
        self.assertEqual(args.port, 9001)

    def test_cli_credentials_override_environment(self) -> None:
        with patch.dict(
            os.environ,
            {"CSMAR_MCP_ACCOUNT": "env-user", "CSMAR_MCP_PASSWORD": "env-pass"},
            clear=True,
        ):
            args = TEST_API_MODULE._parse_args(
                ["--account", "cli-user", "--password", "cli-pass"]
            )

        self.assertEqual(args.account, "cli-user")
        self.assertEqual(args.password, "cli-pass")

    def test_errors_when_no_credentials_are_available(self) -> None:
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(SystemExit) as raised:
            TEST_API_MODULE._parse_args([])

        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
