from __future__ import annotations

from typing import Any

from csmar_mcp.core.errors import CsmarError
from csmar_mcp.core.types import CatalogRecord, FieldSchemaRecord
from csmar_mcp.infra.csmar_gateway import CsmarGateway
from csmar_mcp.infra.state import PersistentState

_METADATA_NOT_FOUND_CODES: frozenset[str] = frozenset(
    {"not_purchased", "database_not_found", "table_not_found"}
)


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

        try:
            table_records = self._gateway.list_tables(database_name)
        except CsmarError as error:
            if error.error_code in _METADATA_NOT_FOUND_CODES:
                self._invalidate_database_catalog()
                table_records = self._gateway.list_tables(database_name)
            else:
                raise

        self._state.set_cached("tables", cache_key, table_records)
        return table_records

    def list_field_schema_items(self, table_code: str) -> list[FieldSchemaRecord]:
        cache_key = table_code.strip()
        cached = self._state.get_cached("schema", cache_key)
        if cached is not None:
            return list(cached)

        return list(self._fetch_schema_live(cache_key))

    def _fetch_schema_live(self, table_code: str) -> list[FieldSchemaRecord]:
        try:
            fields = self._gateway.list_field_schema_items(table_code)
        except CsmarError as error:
            if error.error_code in _METADATA_NOT_FOUND_CODES:
                self._invalidate_database_catalog()
                self._state.delete_cached("schema", table_code)
                fields = self._gateway.list_field_schema_items(table_code)
            else:
                raise

        self._state.set_cached("schema", table_code, fields)
        return fields

    def bulk_read_schema(
        self, table_codes: list[str]
    ) -> list[tuple[str, list[FieldSchemaRecord] | None, str, CsmarError | None]]:
        results: list[tuple[str, list[FieldSchemaRecord] | None, str, CsmarError | None]] = []
        misses: list[str] = []
        for raw_code in table_codes:
            code = raw_code.strip()
            cached = self._state.get_cached("schema", code)
            if cached is not None:
                results.append((code, list(cached), "cache", None))
            else:
                misses.append(code)
                results.append((code, None, "live", None))

        if misses:
            from concurrent.futures import ThreadPoolExecutor

            def _fetch(code: str) -> tuple[str, list[FieldSchemaRecord] | None, CsmarError | None]:
                try:
                    return code, list(self._fetch_schema_live(code)), None
                except CsmarError as error:
                    return code, None, error

            with ThreadPoolExecutor(max_workers=4) as pool:
                fetched = list(pool.map(_fetch, misses))

            fetched_by_code = {code: (fields, error) for code, fields, error in fetched}
            for index, (code, _, source, _) in enumerate(results):
                if source == "live" and code in fetched_by_code:
                    fields, error = fetched_by_code[code]
                    results[index] = (code, fields, "live", error)

        return results

    def search_field_in_cache(
        self,
        keyword: str,
        database: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        needle = keyword.strip().lower()
        if not needle:
            return []

        allowed_codes: set[str] | None = None
        if database is not None:
            records = self._state.get_cached("tables", database.strip())
            if records is None:
                return []
            allowed_codes = {record.table_code for record in records}

        table_index = self._build_table_index()
        schema_entries = self._state.list_cached("schema")

        hits: list[dict[str, Any]] = []
        for table_code, fields in schema_entries:
            if allowed_codes is not None and table_code not in allowed_codes:
                continue
            catalog = table_index.get(table_code, (None, table_code))
            database_name, table_name = catalog

            table_code_match = needle in table_code.lower()
            table_name_match = needle in table_name.lower() if table_name else False

            for field in fields:
                field_code = field.field_name or ""
                if needle in field_code.lower() or table_code_match or table_name_match:
                    hits.append(
                        {
                            "database": database_name or "",
                            "table_code": table_code,
                            "table_name": table_name or "",
                            "field_code": field_code,
                            "field_label": field.field_label,
                            "data_type": field.data_type,
                            "field_key": field.field_key,
                            "nullable": field.nullable,
                        }
                    )
                    if len(hits) >= limit:
                        return hits
        return hits

    def _build_table_index(self) -> dict[str, tuple[str, str]]:
        index: dict[str, tuple[str, str]] = {}
        for _, records in self._state.list_cached("tables"):
            for record in records:
                index[record.table_code] = (record.database_name, record.table_name)
        return index

    def _invalidate_database_catalog(self) -> None:
        self._state.delete_cached("databases", "all")

    def read_table_schema(self, table_code: str) -> list[FieldSchemaRecord]:
        return self.list_field_schema_items(table_code)
