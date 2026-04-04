from __future__ import annotations

import copy
import json
import pickle
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


class PersistentState:
    def __init__(self, cache_ttl_minutes: int = 30, state_dir: str | Path | None = None) -> None:
        self._lock = threading.RLock()
        self._cache_ttl = timedelta(minutes=max(1, cache_ttl_minutes))
        base_dir = Path(state_dir) if state_dir is not None else Path.cwd() / ".stata_agent" / "csmar_mcp"
        self._state_dir = base_dir.expanduser().resolve()
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = self._state_dir / "state.sqlite3"
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def get_cached(self, namespace: str, key: str) -> Any | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT created_at, payload FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                (namespace, key),
            ).fetchone()
            if row is None:
                return None

            created_at = datetime.fromtimestamp(float(row["created_at"]), tz=timezone.utc)
            if self._now() - created_at > self._cache_ttl:
                self._conn.execute(
                    "DELETE FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                    (namespace, key),
                )
                self._conn.commit()
                return None

            value = pickle.loads(row["payload"])
            return copy.deepcopy(value)

    def has_cached(self, namespace: str, key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT created_at FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                (namespace, key),
            ).fetchone()
            if row is None:
                return False

            created_at = datetime.fromtimestamp(float(row["created_at"]), tz=timezone.utc)
            if self._now() - created_at > self._cache_ttl:
                self._conn.execute(
                    "DELETE FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                    (namespace, key),
                )
                self._conn.commit()
                return False

            return True

    def set_cached(self, namespace: str, key: str, value: Any) -> None:
        with self._lock:
            payload = pickle.dumps(copy.deepcopy(value), protocol=pickle.HIGHEST_PROTOCOL)
            self._conn.execute(
                """
                INSERT INTO cache_entries(namespace, cache_key, created_at, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(namespace, cache_key)
                DO UPDATE SET created_at = excluded.created_at, payload = excluded.payload
                """,
                (namespace, key, self._now().timestamp(), payload),
            )
            self._conn.commit()

    def delete_cached(self, namespace: str, key: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM cache_entries WHERE namespace = ? AND cache_key = ?",
                (namespace, key),
            )
            self._conn.commit()

    def mark_rate_limited(self, key: str) -> None:
        with self._lock:
            expires_at = (self._now() + self._cache_ttl).timestamp()
            self._conn.execute(
                """
                INSERT INTO rate_limit_cooldowns(cache_key, expires_at)
                VALUES (?, ?)
                ON CONFLICT(cache_key)
                DO UPDATE SET expires_at = excluded.expires_at
                """,
                (key, expires_at),
            )
            self._conn.commit()

    def get_rate_limit_remaining_seconds(self, key: str) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT expires_at FROM rate_limit_cooldowns WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row is None:
                return None

            remaining_seconds = int(float(row["expires_at"]) - self._now().timestamp())
            if remaining_seconds <= 0:
                self._conn.execute("DELETE FROM rate_limit_cooldowns WHERE cache_key = ?", (key,))
                self._conn.commit()
                return None

            return remaining_seconds

    def add_tool_trace(
        self,
        *,
        trace_id: str,
        tool_name: str,
        request_payload: dict[str, Any],
        result_summary: dict[str, Any] | None,
        error: dict[str, Any] | None,
        query_fingerprint: str | None,
        validation_id: str | None,
        cached: bool,
        started_at: datetime,
        completed_at: datetime,
        upstream_code: int | None = None,
        raw_message: str | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO tool_audit_log(
                    trace_id,
                    tool_name,
                    request_payload,
                    result_summary,
                    error,
                    query_fingerprint,
                    validation_id,
                    cached,
                    started_at,
                    completed_at,
                    upstream_code,
                    raw_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    tool_name,
                    self._to_json(request_payload),
                    self._to_json(result_summary),
                    self._to_json(error),
                    query_fingerprint,
                    validation_id,
                    1 if cached else 0,
                    self._to_iso_datetime(started_at),
                    self._to_iso_datetime(completed_at),
                    upstream_code,
                    raw_message,
                ),
            )
            self._conn.commit()

    def get_tool_trace(self, trace_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM tool_audit_log WHERE trace_id = ?", (trace_id,)).fetchone()
            if row is None:
                return None

            return {
                "trace_id": row["trace_id"],
                "tool_name": row["tool_name"],
                "request_payload": self._from_json(row["request_payload"]),
                "result_summary": self._from_json(row["result_summary"]),
                "error": self._from_json(row["error"]),
                "query_fingerprint": row["query_fingerprint"],
                "validation_id": row["validation_id"],
                "cached": bool(row["cached"]),
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "upstream_code": row["upstream_code"],
                "raw_message": row["raw_message"],
            }

    def _initialize_schema(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    namespace TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY(namespace, cache_key)
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS rate_limit_cooldowns (
                    cache_key TEXT PRIMARY KEY,
                    expires_at REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tool_audit_log (
                    trace_id TEXT PRIMARY KEY,
                    tool_name TEXT NOT NULL,
                    request_payload TEXT NOT NULL,
                    result_summary TEXT,
                    error TEXT,
                    query_fingerprint TEXT,
                    validation_id TEXT,
                    cached INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    upstream_code INTEGER,
                    raw_message TEXT
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tool_audit_tool_name ON tool_audit_log(tool_name, started_at DESC)"
            )
            self._conn.commit()

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _to_iso_datetime(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _to_json(self, value: dict[str, Any] | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    def _from_json(self, value: str | None) -> dict[str, Any] | None:
        if value is None:
            return None
        return json.loads(value)

