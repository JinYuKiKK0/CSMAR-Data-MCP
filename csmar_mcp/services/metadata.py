from __future__ import annotations

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
    ) -> list[tuple[str, str | None, list[FieldSchemaRecord] | None, str, CsmarError | None]]:
        name_by_code = self._collect_cached_table_names()
        results: list[
            tuple[str, str | None, list[FieldSchemaRecord] | None, str, CsmarError | None]
        ] = []
        misses: list[str] = []
        for raw_code in table_codes:
            code = raw_code.strip()
            cached = self._state.get_cached("schema", code)
            if cached is not None:
                results.append((code, name_by_code.get(code), list(cached), "cache", None))
            else:
                misses.append(code)
                results.append((code, name_by_code.get(code), None, "live", None))

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
            for index, (code, name, _, source, _) in enumerate(results):
                if source == "live" and code in fetched_by_code:
                    fields, error = fetched_by_code[code]
                    results[index] = (code, name, fields, "live", error)

        return results

    def _collect_cached_table_names(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for _db_key, records in self._state.list_cached("tables"):
            if not isinstance(records, list):
                continue
            for record in records:  # pyright: ignore[reportUnknownVariableType]
                if isinstance(record, CatalogRecord):
                    mapping.setdefault(record.table_code, record.table_name)
        return mapping

    def _invalidate_database_catalog(self) -> None:
        self._state.delete_cached("databases", "all")

    def read_table_schema(self, table_code: str) -> list[FieldSchemaRecord]:
        return self.list_field_schema_items(table_code)
