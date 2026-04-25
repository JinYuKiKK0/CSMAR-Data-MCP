from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta

from csmar_mcp.core.errors import CsmarError
from csmar_mcp.core.types import CatalogRecord, FieldSchemaRecord, ProbeSpec, ValidationRecord
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
                CatalogRecord(
                    database_name="财务报表", table_code="FS_Combas", table_name="资产负债表"
                ),
                CatalogRecord(
                    database_name="财务报表", table_code="FS_Income", table_name="利润表"
                ),
                CatalogRecord(
                    database_name="财务报表", table_code="FS_CashFlow", table_name="现金流量表"
                ),
                CatalogRecord(database_name="财务报表", table_code="FS_Notes", table_name="附注"),
                CatalogRecord(
                    database_name="财务报表", table_code="FS_Ratios", table_name="财务比率"
                ),
                CatalogRecord(database_name="财务报表", table_code="FS_Main", table_name="主表"),
            ],
            "银行财务": [
                CatalogRecord(
                    database_name="银行财务", table_code="BANK_Index", table_name="银行指标"
                ),
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


class FailingMaterializeGateway(FakeGateway):
    def __init__(self, *, error_code: str) -> None:
        super().__init__()
        self.error_code = error_code
        self.start_package_calls = 0

    def start_package(
        self,
        *,
        table_code: str,
        columns: list[str],
        condition: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> str:
        self.start_package_calls += 1
        raise CsmarError(self.error_code, f"materialize {self.error_code}")


class MetadataServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.gateway = FakeGateway()
        self.state = PersistentState(cache_ttl_minutes=30, state_dir=self.temp_dir.name)
        self.service = MetadataService(self.gateway, self.state)

    def tearDown(self) -> None:
        self.state.close()
        self.temp_dir.cleanup()

    def test_list_databases_caches_result(self) -> None:
        first = self.service.list_databases()
        self.gateway.list_databases_calls = getattr(self.gateway, "list_databases_calls", 0)
        second = self.service.list_databases()
        self.assertEqual(first, second)

    def test_list_tables_returns_catalog_records(self) -> None:
        records = self.service.list_tables("财务报表")
        codes = [record.table_code for record in records]
        self.assertIn("FS_Combas", codes)

    def test_read_table_schema_returns_fields(self) -> None:
        fields = self.service.read_table_schema("BANK_Index")
        field_names = {field.field_name for field in fields}
        self.assertEqual(field_names, {"ROAA", "CapitalRatio"})

    def test_bulk_read_schema_mixes_cache_and_live(self) -> None:
        # Prime cache for FS_Income only.
        self.service.read_table_schema("FS_Income")

        results = self.service.bulk_read_schema(["FS_Income", "FS_Combas"])
        by_code = {code: (fields, source, error) for code, fields, source, error in results}

        self.assertEqual(by_code["FS_Income"][1], "cache")
        self.assertIsNone(by_code["FS_Income"][2])
        self.assertEqual(by_code["FS_Combas"][1], "live")
        self.assertIsNone(by_code["FS_Combas"][2])

    def test_search_field_in_cache_matches_field_code(self) -> None:
        self.service.list_tables("银行财务")
        self.service.read_table_schema("BANK_Index")

        hits = self.service.search_field_in_cache("ROAA")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["field_code"], "ROAA")
        self.assertEqual(hits[0]["table_code"], "BANK_Index")
        self.assertEqual(hits[0]["database"], "银行财务")

    def test_search_field_returns_empty_when_cache_cold(self) -> None:
        hits = self.service.search_field_in_cache("ROAA")
        self.assertEqual(hits, [])

    def test_search_field_respects_database_filter(self) -> None:
        self.service.list_tables("财务报表")
        self.service.list_tables("银行财务")
        self.service.read_table_schema("FS_Combas")
        self.service.read_table_schema("BANK_Index")

        # Substring 'a' appears in field codes of both FS and BANK tables; database filter should scope.
        hits_bank = self.service.search_field_in_cache("a", database="银行财务")
        table_codes = {hit["table_code"] for hit in hits_bank}
        self.assertEqual(table_codes, {"BANK_Index"})


class MetadataSelfHealingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state = PersistentState(cache_ttl_minutes=30, state_dir=self.temp_dir.name)

    def tearDown(self) -> None:
        self.state.close()
        self.temp_dir.cleanup()

    def test_list_tables_retries_after_not_purchased(self) -> None:
        # Seed a stale databases cache.
        self.state.set_cached("databases", "all", ["stale_db", "real_db"])

        class FlakyGateway:
            def __init__(self) -> None:
                self.calls = 0

            def list_databases(self) -> list[str]:
                return ["real_db"]

            def list_tables(self, database_name: str) -> list[CatalogRecord]:
                self.calls += 1
                if self.calls == 1:
                    raise CsmarError("not_purchased", "no access")
                return [CatalogRecord(database_name=database_name, table_code="X", table_name="X")]

            def list_field_schema_items(self, table_code: str) -> list[FieldSchemaRecord]:
                return []

        gateway = FlakyGateway()
        service = MetadataService(gateway, self.state)
        records = service.list_tables("real_db")
        self.assertEqual([record.table_code for record in records], ["X"])
        self.assertEqual(gateway.calls, 2)
        # databases cache should have been invalidated (and therefore absent until next fetch).
        self.assertIsNone(self.state.get_cached("databases", "all"))


class NamespaceTTLTests(unittest.TestCase):
    def test_metadata_namespace_uses_custom_ttl(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            state = PersistentState(
                cache_ttl_minutes=1,
                state_dir=state_dir,
                namespace_ttls={"schema": timedelta(days=30)},
            )
            # Force timestamp far enough in the past that short TTL would expire it,
            # but the metadata TTL should keep it alive.
            state.set_cached("schema", "FS_Combas", ["field"])
            # Manipulate created_at to two hours ago.
            state._conn.execute(
                "UPDATE cache_entries SET created_at = ? WHERE namespace = ? AND cache_key = ?",
                ((datetime.now(UTC) - timedelta(hours=2)).timestamp(), "schema", "FS_Combas"),
            )
            state._conn.commit()
            self.assertIsNotNone(state.get_cached("schema", "FS_Combas"))

            # Short-TTL namespace should still expire.
            state.set_cached("probes", "p1", ["row"])
            state._conn.execute(
                "UPDATE cache_entries SET created_at = ? WHERE namespace = ? AND cache_key = ?",
                ((datetime.now(UTC) - timedelta(hours=2)).timestamp(), "probes", "p1"),
            )
            state._conn.commit()
            self.assertIsNone(state.get_cached("probes", "p1"))
            state.close()

    def test_clear_namespace_counts_rows(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            state = PersistentState(cache_ttl_minutes=30, state_dir=state_dir)
            state.set_cached("schema", "A", ["x"])
            state.set_cached("schema", "B", ["y"])
            state.set_cached("tables", "db", ["z"])
            self.assertEqual(state.clear_namespace("schema"), 2)
            self.assertIsNone(state.get_cached("schema", "A"))
            self.assertIsNotNone(state.get_cached("tables", "db"))
            state.close()

    def test_persistent_state_requires_state_dir(self) -> None:
        with self.assertRaises(ValueError):
            PersistentState(cache_ttl_minutes=30)


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

    def test_build_cache_key_is_order_insensitive_for_columns(self) -> None:
        key_one = self.service.build_cache_key(
            table_code="FS_Combas",
            columns=("Accper", "Stkcd", "Accper"),
            condition="Stkcd='000001'",
            start_date="2018-01-01",
            end_date="2020-12-31",
        )
        key_two = self.service.build_cache_key(
            table_code="FS_Combas",
            columns=("Stkcd", "Accper"),
            condition="Stkcd='000001'",
            start_date="2018-01-01",
            end_date="2020-12-31",
        )
        self.assertEqual(key_one, key_two)

    def test_probe_query_does_not_store_query_specs_namespace(self) -> None:
        result = self.service.probe_query(
            ProbeSpec(
                table_code="FS_Combas",
                columns=("Stkcd",),
                condition="Stkcd='000001'",
                start_date="2018-01-01",
                end_date="2020-12-31",
                sample_rows=0,
            )
        )

        self.assertIsNone(self.state.get_cached("query_specs", result.query_fingerprint))

    def test_materialize_query_checks_cooldown_before_start_package(self) -> None:
        failing_gateway = FailingMaterializeGateway(error_code="upstream_error")
        metadata = MetadataService(failing_gateway, self.state)
        service = QueryService(failing_gateway, metadata, self.state)
        validation_id = "validation_cooldown"
        query_fingerprint = "fp_cooldown"
        self.state.set_cached(
            "validations",
            validation_id,
            ValidationRecord(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                table_code="FS_Combas",
                columns=("Stkcd",),
                condition="1=1",
                start_date="2018-01-01",
                end_date="2020-12-31",
                row_count=12,
                can_materialize=True,
            ),
        )
        query_cache_key = service.build_cache_key(
            table_code="FS_Combas",
            columns=("Stkcd",),
            condition="1=1",
            start_date="2018-01-01",
            end_date="2020-12-31",
        )
        self.state.mark_rate_limited(query_cache_key)

        with self.assertRaises(CsmarError) as context:
            service.materialize_query(validation_id, "D:/tmp/csmar", max_retries=0)

        self.assertEqual(context.exception.error_code, "rate_limited")
        self.assertIsNotNone(context.exception.retry_after_seconds)
        self.assertEqual(failing_gateway.start_package_calls, 0)

    def test_materialize_query_rate_limited_fails_fast_without_retries(self) -> None:
        failing_gateway = FailingMaterializeGateway(error_code="rate_limited")
        metadata = MetadataService(failing_gateway, self.state)
        service = QueryService(failing_gateway, metadata, self.state)
        validation_id = "validation_rate_limited"
        query_fingerprint = "fp_rate_limited"
        self.state.set_cached(
            "validations",
            validation_id,
            ValidationRecord(
                validation_id=validation_id,
                query_fingerprint=query_fingerprint,
                table_code="FS_Combas",
                columns=("Stkcd",),
                condition="1=1",
                start_date="2018-01-01",
                end_date="2020-12-31",
                row_count=12,
                can_materialize=True,
            ),
        )

        with self.assertRaises(CsmarError) as context:
            service.materialize_query(validation_id, "D:/tmp/csmar", max_retries=2)

        self.assertEqual(context.exception.error_code, "rate_limited")
        self.assertEqual(failing_gateway.start_package_calls, 1)
        query_cache_key = service.build_cache_key(
            table_code="FS_Combas",
            columns=("Stkcd",),
            condition="1=1",
            start_date="2018-01-01",
            end_date="2020-12-31",
        )
        self.assertIsNotNone(self.state.get_rate_limit_remaining_seconds(query_cache_key))


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
            started_at = datetime.now(UTC)
            completed_at = datetime.now(UTC)
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
