from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from csmar_mcp import infra
from csmar_mcp.runtime import RuntimeSettings
from csmar_mcp.runtime import configure_runtime
from csmar_mcp.runtime import get_client
from csmar_mcp.runtime import parse_runtime_settings


class RuntimeSettingsTests(unittest.TestCase):
    def tearDown(self) -> None:
        get_client.cache_clear()

    def test_parse_runtime_settings_without_state_dir_env(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            settings = parse_runtime_settings(["--account", "acc", "--password", "pwd"])

        self.assertEqual(settings.account, "acc")
        self.assertEqual(settings.password, "pwd")
        self.assertIsNone(settings.state_dir)

    def test_parse_runtime_settings_with_state_dir_env(self) -> None:
        with patch.dict("os.environ", {"CSMAR_MCP_STATE_DIR": "~/tmp/csmar-state"}, clear=True):
            settings = parse_runtime_settings(["--account", "acc", "--password", "pwd"])

        self.assertIsNotNone(settings.state_dir)
        assert settings.state_dir is not None
        self.assertEqual(settings.state_dir, Path("~/tmp/csmar-state").expanduser().resolve())

    def test_get_client_passes_state_dir_to_csmar_client(self) -> None:
        configure_runtime(
            RuntimeSettings(
                account="acc",
                password="pwd",
                state_dir=Path("/tmp/csmar-state"),
            )
        )

        with patch("csmar_mcp.runtime.CsmarClient") as mock_client:
            get_client.cache_clear()
            get_client()

        _, kwargs = mock_client.call_args
        self.assertEqual(kwargs["state_dir"], Path("/tmp/csmar-state"))

    def test_inmemory_state_alias_is_not_exported(self) -> None:
        self.assertNotIn("InMemoryState", infra.__all__)


if __name__ == "__main__":
    unittest.main()