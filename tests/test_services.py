from __future__ import annotations

import unittest

from csmar_mcp.core.errors import CsmarError
from csmar_mcp.core.types import CatalogRecord, FieldSchemaRecord, ProbeSpec
from csmar_mcp.infra.state import InMemoryState
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
        self.gateway = FakeGateway()
        self.state = InMemoryState(cache_ttl_minutes=30)
        self.service = MetadataService(self.gateway, self.state)

    def test_search_tables_returns_exact_code_match_first(self) -> None:
        results = self.service.search_tables("BANK_Index")
        self.assertEqual(results[0].table_code, "BANK_Index")
        self.assertEqual(results[0].why_matched, "exact table code match")

    def test_search_fields_uses_semantic_aliases(self) -> None:
        results = self.service.search_fields("roa", table_code="BANK_Index")
        self.assertEqual(results[0].field_name, "ROAA")


class QueryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gateway = FakeGateway()
        self.state = InMemoryState(cache_ttl_minutes=30)
        self.metadata = MetadataService(self.gateway, self.state)
        self.service = QueryService(self.gateway, self.metadata, self.state)

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


if __name__ == "__main__":
    unittest.main()
