from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone

from csmar_mcp.core.errors import CsmarError
from csmar_mcp.core.types import CatalogRecord, FieldSchemaRecord, ProbeSpec
from csmar_mcp.infra.state import PersistentState
from csmar_mcp.services.metadata import MetadataService
from csmar_mcp.services.query import QueryService


class FakeGateway:
    def __init__(self) -> None:
        self.query_count_called = False

    def list_databases(self) -> list[str]:
        return ["财务报表", "银行财务"]

    def list_tables(self, database_name: str) -> list[CatalogRecord]:
        data = {
            "财务报表": [
                CatalogRecord(database_name="财务报表", table_code="FS_Combas", table_name="资产负债表"),
                CatalogRecord(database_name="财务报表", table_code="FS_Income", table_name="利润表"),
                CatalogRecord(database_name="财务报表", table_code="FS_CashFlow", table_name="现金流量表"),
                CatalogRecord(database_name="财务报表", table_code="FS_Notes", table_name="附注"),
                CatalogRecord(database_name="财务报表", table_code="FS_Ratios", table_name="财务比率"),
                CatalogRecord(database_name="财务报表", table_code="FS_Main", table_name="主表"),
            ],
            "银行财务": [
                CatalogRecord(database_name="银行财务", table_code="BANK_Index", table_name="银行指标"),
            ],
        }
        return data[database_name]

    def list_field_schema_items(self, table_code: str) -> list[FieldSchemaRecord]:
        data = {
            "FS_Combas": [
                FieldSchemaRecord(field_name="Stkcd", field_label="证券代码"),
                FieldSchemaRecord(field_name="Accper", field_label="会计期间"),
            ],
            "FS_Income": [
                FieldSchemaRecord(field_name="Revenue", field_label="营业收入"),
            ],
            "FS_CashFlow": [
                FieldSchemaRecord(field_name="NetCashFlow", field_label="净现金流"),
            ],
            "FS_Notes": [
                FieldSchemaRecord(field_name="NoteItem", field_label="附注项目"),
            ],
            "FS_Ratios": [
                FieldSchemaRecord(field_name="ROE", field_label="净资产收益率"),
            ],
            "FS_Main": [
                FieldSchemaRecord(field_name="MainItem", field_label="主表项目"),
            ],
            "BANK_Index": [
                FieldSchemaRecord(field_name="ROAA", field_label="总资产收益率"),
                FieldSchemaRecord(field_name="CapitalRatio", field_label="资本充足率"),
            ],
        }
        return data[table_code]

    def query_count(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        self.query_count_called = True
        return 12

    def query_sample(
        self,
        *,
        table_code: str,
        columns: list[str],
        sample_rows: int,
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict[str, object]]:
        return [{"Stkcd": "000001"}]


class MetadataServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.gateway = FakeGateway()
        self.state = PersistentState(cache_ttl_minutes=30, state_dir=self.temp_dir.name)
        self.service = MetadataService(self.gateway, self.state)

    def tearDown(self) -> None:
        self.state.close()
        self.temp_dir.cleanup()

    def test_search_tables_returns_exact_code_match_first(self) -> None:
        results = self.service.search_tables("BANK_Index")
        self.assertEqual(results[0].table_code, "BANK_Index")
        self.assertEqual(results[0].why_matched, "exact table code match")

    def test_search_tables_hard_caps_candidates_to_five(self) -> None:
        results = self.service.search_tables("财务报表", limit=50)
        self.assertEqual(len(results), 5)

    def test_search_fields_no_longer_expands_semantic_aliases(self) -> None:
        results = self.service.search_fields("return on assets", table_code="BANK_Index")
        self.assertEqual(results, [])

    def test_search_fields_supports_table_scope_matching(self) -> None:
        results = self.service.search_fields("BANK_Index")
        self.assertTrue(any(item.table_code == "BANK_Index" for item in results))


class QueryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.gateway = FakeGateway()
        self.state = PersistentState(cache_ttl_minutes=30, state_dir=self.temp_dir.name)
        self.metadata = MetadataService(self.gateway, self.state)
        self.service = QueryService(self.gateway, self.metadata, self.state)

    def tearDown(self) -> None:
        self.state.close()
        self.temp_dir.cleanup()

    def test_probe_invalid_columns_short_circuits_without_query_count(self) -> None:
        result = self.service.probe_query(
            ProbeSpec(
                table_code="FS_Combas",
                columns=("NOT_REAL",),
                condition=None,
                start_date=None,
                end_date=None,
                sample_rows=0,
            )
        )
        self.assertEqual(result.invalid_columns, ("NOT_REAL",))
        self.assertFalse(result.can_materialize)
        self.assertFalse(self.gateway.query_count_called)

    def test_materialize_missing_validation_id_raises_invalid_arguments(self) -> None:
        with self.assertRaises(CsmarError) as context:
            self.service.materialize_query("missing", "D:/tmp/csmar")
        self.assertEqual(context.exception.error_code, "invalid_arguments")

    def test_local_condition_error_returns_suggested_patch(self) -> None:
        error = self.service.local_condition_error("Stkcd=='000001'")
        self.assertIsNotNone(error)
        self.assertEqual(error.error_code, "invalid_condition")
        self.assertEqual(error.suggested_args_patch, {"condition": "Stkcd='000001'"})


class PersistentStateTests(unittest.TestCase):
    def test_cache_persists_across_instances(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            state_one = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            state_one.set_cached("databases", "all", ["财务报表"])
            state_one.close()

            state_two = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            self.assertEqual(state_two.get_cached("databases", "all"), ["财务报表"])
            state_two.close()

    def test_rate_limit_cooldown_persists_across_instances(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            state_one = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            state_one.mark_rate_limited("probe_key")
            state_one.close()

            state_two = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            remaining = state_two.get_rate_limit_remaining_seconds("probe_key")
            self.assertIsNotNone(remaining)
            self.assertGreater(remaining, 0)
            state_two.close()

    def test_tool_trace_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            state = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            started_at = datetime.now(timezone.utc)
            completed_at = datetime.now(timezone.utc)
            state.add_tool_trace(
                trace_id="trace_test001",
                tool_name="csmar_probe_query",
                request_payload={"table_code": "FS_Combas"},
                result_summary={"row_count": 12},
                error=None,
                query_fingerprint="abc123",
                validation_id="validation_123",
                cached=False,
                started_at=started_at,
                completed_at=completed_at,
            )

            trace = state.get_tool_trace("trace_test001")
            self.assertIsNotNone(trace)
            assert trace is not None
            self.assertEqual(trace["tool_name"], "csmar_probe_query")
            self.assertEqual(trace["request_payload"], {"table_code": "FS_Combas"})
            self.assertEqual(trace["result_summary"], {"row_count": 12})
            self.assertEqual(trace["query_fingerprint"], "abc123")
            self.assertEqual(trace["validation_id"], "validation_123")
            state.close()


if __name__ == "__main__":
    unittest.main()
