from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(slots=True)
class CacheEntry:
    created_at: datetime
    value: Any


class InMemoryState:
    def __init__(self, cache_ttl_minutes: int = 30) -> None:
        self._lock = threading.RLock()
        self._cache_ttl = timedelta(minutes=max(1, cache_ttl_minutes))
        self._caches: dict[str, dict[str, CacheEntry]] = {}
        self._rate_limit_cooldowns: dict[str, datetime] = {}

    def get_cached(self, namespace: str, key: str) -> Any | None:
        with self._lock:
            cache = self._caches.get(namespace)
            if cache is None:
                return None

            entry = cache.get(key)
            if entry is None:
                return None

            if self._now() - entry.created_at > self._cache_ttl:
                cache.pop(key, None)
                return None

            return copy.deepcopy(entry.value)

    def set_cached(self, namespace: str, key: str, value: Any) -> None:
        with self._lock:
            cache = self._caches.setdefault(namespace, {})
            cache[key] = CacheEntry(created_at=self._now(), value=copy.deepcopy(value))

    def delete_cached(self, namespace: str, key: str) -> None:
        with self._lock:
            cache = self._caches.get(namespace)
            if cache is not None:
                cache.pop(key, None)

    def mark_rate_limited(self, key: str) -> None:
        with self._lock:
            self._rate_limit_cooldowns[key] = self._now() + self._cache_ttl

    def get_rate_limit_remaining_seconds(self, key: str) -> int | None:
        with self._lock:
            expires_at = self._rate_limit_cooldowns.get(key)
            if expires_at is None:
                return None

            remaining_seconds = int((expires_at - self._now()).total_seconds())
            if remaining_seconds <= 0:
                self._rate_limit_cooldowns.pop(key, None)
                return None

            return remaining_seconds

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)
