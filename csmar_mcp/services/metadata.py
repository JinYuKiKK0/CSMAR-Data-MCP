from __future__ import annotations

from csmar_mcp.core.types import CatalogRecord, FieldSchemaRecord
from csmar_mcp.infra.csmar_gateway import CsmarGateway
from csmar_mcp.infra.state import PersistentState


class MetadataService:
    def __init__(self, gateway: CsmarGateway, state: PersistentState) -> None:
        self._gateway = gateway
        self._state = state

    def list_databases(self) -> list[str]:
        cached = self._state.get_cached("databases", "all")
        if cached is not None:
            return list(cached)

        databases = self._gateway.list_databases()
        self._state.set_cached("databases", "all", databases)
        return databases

    def list_tables(self, database_name: str) -> list[CatalogRecord]:
        cache_key = database_name.strip()
        cached = self._state.get_cached("tables", cache_key)
        if cached is not None:
            return list(cached)

        table_records = self._gateway.list_tables(database_name)
        self._state.set_cached("tables", cache_key, table_records)
        return table_records

    def list_field_schema_items(self, table_code: str) -> list[FieldSchemaRecord]:
        cache_key = table_code.strip()
        cached = self._state.get_cached("schema", cache_key)
        if cached is not None:
            return list(cached)

        fields = self._gateway.list_field_schema_items(table_code)
        self._state.set_cached("schema", cache_key, fields)
        return fields

    def read_table_schema(self, table_code: str) -> list[FieldSchemaRecord]:
        return self.list_field_schema_items(table_code)
