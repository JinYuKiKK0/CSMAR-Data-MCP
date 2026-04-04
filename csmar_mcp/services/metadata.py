from __future__ import annotations

from difflib import SequenceMatcher

from ..core.errors import CsmarError
from ..core.types import CatalogRecord, FieldMatch, FieldSchemaRecord, TableMatch
from ..infra.csmar_gateway import CsmarGateway
from ..infra.state import PersistentState


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

    def search_tables(self, query: str, database_name: str | None = None, limit: int = 5) -> list[TableMatch]:
        normalized_query = query.strip().lower()
        effective_limit = max(1, min(limit, 5))
        databases = [database_name] if database_name else self.list_databases()
        matches: list[TableMatch] = []

        for db_name in databases:
            for record in self.list_tables(db_name):
                scored = self._score_table_match(record, normalized_query)
                if scored is None:
                    continue
                score, why_matched = scored
                matches.append(
                    TableMatch(
                        table_code=record.table_code,
                        table_name=record.table_name,
                        database_name=record.database_name,
                        why_matched=why_matched,
                        score=score,
                    )
                )

        matches.sort(key=lambda item: (-item.score, item.table_code))
        return matches[:effective_limit]

    def search_fields(
        self,
        query: str,
        database_name: str | None = None,
        table_code: str | None = None,
        role_hint: str | None = None,
        frequency_hint: str | None = None,
        limit: int = 20,
    ) -> list[FieldMatch]:
        table_candidates = self._resolve_table_candidates(database_name=database_name, table_code=table_code)
        normalized_query = query.strip().lower()

        matches: list[FieldMatch] = []
        for table in table_candidates:
            schema_fields = self.list_field_schema_items(table.table_code)
            for field in schema_fields:
                scored = self._score_field_match(field, normalized_query, role_hint, frequency_hint)
                scope_scored = self._score_field_scope_match(table, normalized_query)
                if scored is not None and scope_scored is not None:
                    scored = (
                        round(min(100.0, scored[0] + 2.0), 2),
                        f"{scored[1]}; {scope_scored[1]}",
                    )
                if scored is None:
                    continue

                score, why_matched = scored
                matches.append(
                    FieldMatch(
                        field_name=field.field_name,
                        field_label=field.field_label,
                        field_description=field.field_description,
                        data_type=field.data_type,
                        frequency_tags=field.frequency_tags,
                        role_tags=field.role_tags,
                        table_code=table.table_code,
                        table_name=table.table_name,
                        database_name=table.database_name,
                        why_matched=why_matched,
                        score=score,
                    )
                )

        matches.sort(key=lambda item: (-item.score, item.table_code, item.field_name))
        return matches[:limit]

    def _resolve_table_candidates(
        self,
        *,
        database_name: str | None,
        table_code: str | None,
    ) -> list[CatalogRecord]:
        if table_code:
            if database_name:
                candidates = [record for record in self.list_tables(database_name) if record.table_code == table_code]
            else:
                candidates = self._find_table_records(table_code)

            if not candidates:
                raise CsmarError(
                    "table_not_found",
                    "The table_code was not found.",
                    hint="Use csmar_search_tables to find a valid table_code, then retry.",
                )
            return candidates

        if database_name:
            return self.list_tables(database_name)

        candidates: list[CatalogRecord] = []
        for db_name in self.list_databases():
            candidates.extend(self.list_tables(db_name))
        return candidates

    def _find_table_records(self, table_code: str) -> list[CatalogRecord]:
        normalized = table_code.strip().lower()
        if not normalized:
            return []

        matches: list[CatalogRecord] = []
        for db_name in self.list_databases():
            for record in self.list_tables(db_name):
                if record.table_code.lower() == normalized:
                    matches.append(record)
        return matches

    def _score_table_match(self, record: CatalogRecord, query: str) -> tuple[float, str] | None:
        code = record.table_code.lower()
        name = record.table_name.lower()
        database = record.database_name.lower()

        if query == code:
            return 100.0, "exact table code match"
        if query == name:
            return 98.0, "exact table name match"
        if query in code:
            return 94.0, "table code contains query"
        if query in name:
            return 90.0, "table name contains query"
        if query in database:
            return 75.0, "database name contains query"

        ratio = max(
            SequenceMatcher(None, query, code).ratio(),
            SequenceMatcher(None, query, name).ratio(),
            SequenceMatcher(None, query, database).ratio(),
        )
        if ratio < 0.35:
            return None
        return round(60.0 + ratio * 30.0, 2), "similar to query text"

    def _score_field_match(
        self,
        field: FieldSchemaRecord,
        query: str,
        role_hint: str | None,
        frequency_hint: str | None,
    ) -> tuple[float, str] | None:
        best_match = self._score_single_field_match(field, query)
        if best_match is None:
            return None

        score, reason = best_match
        field_label = (field.field_label or "").lower()
        field_description = (field.field_description or "").lower()
        role_blob = " ".join(field.role_tags or ()).lower()
        frequency_blob = " ".join(field.frequency_tags or ()).lower()

        reasons = [reason]

        if role_hint:
            normalized_role_hint = role_hint.strip().lower()
            if normalized_role_hint and (
                normalized_role_hint in role_blob
                or normalized_role_hint in field_label
                or normalized_role_hint in field_description
            ):
                score += 4.0
                reasons.append("role hint matched")
            elif normalized_role_hint:
                score -= 1.0

        if frequency_hint:
            normalized_frequency_hint = frequency_hint.strip().lower()
            if normalized_frequency_hint and (
                normalized_frequency_hint in frequency_blob
                or normalized_frequency_hint in field_label
                or normalized_frequency_hint in field_description
            ):
                score += 4.0
                reasons.append("frequency hint matched")
            elif normalized_frequency_hint:
                score -= 1.0

        return round(max(0.0, min(100.0, score)), 2), "; ".join(reasons)

    def _score_single_field_match(
        self,
        field: FieldSchemaRecord,
        query: str,
    ) -> tuple[float, str] | None:
        field_name = field.field_name.lower()
        field_label = (field.field_label or "").lower()
        field_description = (field.field_description or "").lower()
        data_type = (field.data_type or "").lower()

        if query == field_name:
            return 100.0, "exact field name match"
        if field_label and query == field_label:
            return 98.0, "exact field label match"
        if query in field_name:
            return 94.0, "field name contains query"
        if field_label and query in field_label:
            return 91.0, "field label contains query"
        if field_description and query in field_description:
            return 87.0, "field description contains query"

        ratio = max(
            SequenceMatcher(None, query, field_name).ratio(),
            SequenceMatcher(None, query, field_label).ratio() if field_label else 0.0,
            SequenceMatcher(None, query, field_description).ratio() if field_description else 0.0,
            SequenceMatcher(None, query, data_type).ratio() if data_type else 0.0,
        )
        if ratio < 0.34:
            return None
        return round(60.0 + ratio * 30.0, 2), "similar to query text"

    def _score_field_scope_match(self, table: CatalogRecord, query: str) -> tuple[float, str] | None:
        table_code = table.table_code.lower()
        table_name = table.table_name.lower()
        database_name = table.database_name.lower()

        if query == table_code:
            return 85.0, "exact table code match"
        if query == table_name:
            return 83.0, "exact table name match"
        if query == database_name:
            return 80.0, "exact database name match"
        if query in table_code:
            return 78.0, "table code contains query"
        if query in table_name:
            return 76.0, "table name contains query"
        if query in database_name:
            return 74.0, "database name contains query"

        ratio = max(
            SequenceMatcher(None, query, table_code).ratio(),
            SequenceMatcher(None, query, table_name).ratio(),
            SequenceMatcher(None, query, database_name).ratio(),
        )
        if ratio < 0.35:
            return None
        return round(58.0 + ratio * 24.0, 2), "similar to table scope text"
