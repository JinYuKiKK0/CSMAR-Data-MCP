from __future__ import annotations

from difflib import SequenceMatcher, get_close_matches

from ..core.errors import CsmarError
from ..core.types import CatalogRecord, FieldMatch, FieldSchemaRecord, TableMatch
from ..infra.csmar_gateway import CsmarGateway
from ..infra.state import InMemoryState

_SEMANTIC_QUERY_ALIASES: dict[str, tuple[str, ...]] = {
    "roa": ("return on assets", "return on asset", "roaa", "asset return"),
    "roaa": ("roa", "return on assets", "asset return"),
    "资本充足率": (
        "capital adequacy ratio",
        "capital adequacy",
        "capital ratio",
        "car",
    ),
    "拨备覆盖率": (
        "loan loss reserve coverage",
        "loan loss provision coverage",
        "allowance coverage",
        "provision coverage",
        "llr coverage",
    ),
    "不良贷款率": (
        "non performing loan ratio",
        "non-performing loan ratio",
        "npl ratio",
        "npl",
    ),
    "净息差": ("net interest margin", "nim"),
    "净利差": ("net interest spread", "nis"),
}


class MetadataService:
    def __init__(self, gateway: CsmarGateway, state: InMemoryState) -> None:
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

    def search_tables(self, query: str, database_name: str | None = None, limit: int = 10) -> list[TableMatch]:
        normalized_query = query.strip().lower()
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
        return matches[:limit]

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

    def suggest_tables(self, table_code: str, database_name: str | None = None, limit: int = 5) -> list[str]:
        records = self.search_tables(table_code, database_name=database_name, limit=max(limit, 10))
        suggestions = [f"{item.table_code} ({item.table_name})" for item in records]
        if suggestions:
            return suggestions[:limit]

        databases = [database_name] if database_name else self.list_databases()
        code_pool: list[str] = []
        for db_name in databases:
            code_pool.extend(record.table_code for record in self.list_tables(db_name))

        return get_close_matches(table_code, code_pool, n=limit, cutoff=0.3)

    def suggest_databases(self, database_name: str, limit: int = 5) -> list[str]:
        normalized_query = database_name.strip().lower()
        if not normalized_query:
            return []

        matches: list[tuple[float, str]] = []
        for candidate in self.list_databases():
            lowered_candidate = candidate.lower()
            score = max(
                SequenceMatcher(None, normalized_query, lowered_candidate).ratio(),
                SequenceMatcher(None, normalized_query, lowered_candidate.replace("数据库", "")).ratio(),
            )
            if normalized_query in lowered_candidate or lowered_candidate in normalized_query:
                score = max(score, 0.9)
            if score >= 0.45:
                matches.append((score, candidate))

        matches.sort(key=lambda item: (-item[0], item[1]))
        suggestions = self._deduplicate([candidate for _, candidate in matches])
        if suggestions:
            return suggestions[:limit]

        return get_close_matches(database_name, self.list_databases(), n=limit, cutoff=0.45)

    def suggest_fields(self, table_code: str, columns: list[str], limit: int = 5) -> list[str]:
        field_pool = [item.field_name for item in self.list_field_schema_items(table_code)]
        suggestions: list[str] = []
        for column in columns:
            for candidate in get_close_matches(column, field_pool, n=limit, cutoff=0.5):
                if candidate not in suggestions:
                    suggestions.append(candidate)
        return suggestions[:limit]

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
                    candidate_values=self.suggest_tables(table_code, database_name=database_name),
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
        best_match = self._score_semantic_field_match(field, query)
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

    def _score_semantic_field_match(
        self,
        field: FieldSchemaRecord,
        query: str,
    ) -> tuple[float, str] | None:
        best_match: tuple[float, str] | None = None
        for search_term in self._expand_semantic_queries(query):
            scored = self._score_single_field_match(field, search_term)
            if scored is None:
                continue

            score, reason = scored
            if search_term != query:
                reason = f"{reason}; semantic alias matched"

            if best_match is None or score > best_match[0]:
                best_match = (score, reason)

        return best_match

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

    def _expand_semantic_queries(self, query: str) -> list[str]:
        normalized_query = query.strip().lower()
        expanded: list[str] = []
        seen: set[str] = set()

        def add_term(value: str) -> None:
            cleaned = value.strip().lower()
            if not cleaned or cleaned in seen:
                return
            seen.add(cleaned)
            expanded.append(cleaned)

        add_term(normalized_query)

        for source, aliases in _SEMANTIC_QUERY_ALIASES.items():
            normalized_source = source.strip().lower()
            alias_pool = (normalized_source, *aliases)
            if not any(term in normalized_query for term in alias_pool):
                continue
            for term in alias_pool:
                add_term(term)

        return expanded

    def _deduplicate(self, values: list[str]) -> list[str]:
        unique_values: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            unique_values.append(value)
        return unique_values
