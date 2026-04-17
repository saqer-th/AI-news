from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
}

RSS_HEADERS = {
    **DEFAULT_BROWSER_HEADERS,
    "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
}

_SESSION_LOCAL = threading.local()
_PATH_LOCKS: dict[str, threading.Lock] = {}
_PATH_LOCKS_GUARD = threading.Lock()


class TTLMemoryCache:
    def __init__(self, maxsize: int = 1024, ttl_seconds: int = 3600) -> None:
        self.maxsize = max(1, int(maxsize))
        self.ttl_seconds = max(1, int(ttl_seconds))
        self._entries: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._lock = threading.Lock()

    def _purge_expired_locked(self) -> None:
        now = time.monotonic()
        expired_keys = [key for key, (expires_at, _) in self._entries.items() if expires_at <= now]
        for key in expired_keys:
            self._entries.pop(key, None)

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            self._purge_expired_locked()
            if key not in self._entries:
                return default
            expires_at, value = self._entries.pop(key)
            if expires_at <= time.monotonic():
                return default
            self._entries[key] = (expires_at, value)
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._purge_expired_locked()
            self._entries.pop(key, None)
            self._entries[key] = (time.monotonic() + self.ttl_seconds, value)
            while len(self._entries) > self.maxsize:
                self._entries.popitem(last=False)


def _build_retry(total: int = 3, backoff_factor: float = 0.6) -> Retry:
    return Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"HEAD", "GET", "OPTIONS", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )


def get_http_session(
    session_name: str = "default",
    *,
    pool_connections: int = 10,
    pool_maxsize: int = 20,
) -> requests.Session:
    sessions = getattr(_SESSION_LOCAL, "sessions", None)
    if sessions is None:
        sessions = {}
        _SESSION_LOCAL.sessions = sessions

    cache_key = (session_name, int(pool_connections), int(pool_maxsize))
    session = sessions.get(cache_key)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=_build_retry(),
            pool_connections=max(1, int(pool_connections)),
            pool_maxsize=max(1, int(pool_maxsize)),
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(DEFAULT_BROWSER_HEADERS)
        sessions[cache_key] = session
    return session


def request_with_retry(
    method: str,
    url: str,
    *,
    session_name: str = "default",
    headers: dict[str, str] | None = None,
    timeout: int | float = 10,
    **kwargs,
) -> requests.Response:
    session = get_http_session(session_name=session_name)
    merged_headers = dict(DEFAULT_BROWSER_HEADERS)
    if headers:
        merged_headers.update(headers)
    return session.request(method=method.upper(), url=url, headers=merged_headers, timeout=timeout, **kwargs)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_cached_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def load_json_cache(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        pass
    return {}


def _get_path_lock(path: Path) -> threading.Lock:
    key = str(path.absolute())
    with _PATH_LOCKS_GUARD:
        lock = _PATH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _PATH_LOCKS[key] = lock
    return lock


def prune_json_cache(cache: dict[str, Any], *, max_entries: int, ttl_seconds: int) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc).timestamp() - max(1, int(ttl_seconds))
    valid_items: list[tuple[float, str, Any]] = []

    for key, value in cache.items():
        if not isinstance(value, dict):
            continue
        cached_at = parse_cached_at(value.get("cached_at"))
        if cached_at is None:
            continue
        timestamp = cached_at.timestamp()
        if timestamp < cutoff:
            continue
        valid_items.append((timestamp, key, value))

    valid_items.sort(key=lambda item: item[0], reverse=True)
    return {key: value for _, key, value in valid_items[: max(1, int(max_entries))]}


def save_json_cache(path: Path, cache: dict[str, Any], *, max_entries: int, ttl_seconds: int) -> None:
    lock = _get_path_lock(path)
    with lock:
        existing = load_json_cache(path)
        merged = dict(existing)
        merged.update(cache)
        pruned = prune_json_cache(merged, max_entries=max_entries, ttl_seconds=ttl_seconds)
        path.parent.mkdir(parents=True, exist_ok=True)

        temp_path = path.with_suffix(f"{path.suffix}.tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            temp_path.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp_path, path)
        finally:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass


def stable_hash(*parts: Any) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(str(part).encode("utf-8", errors="ignore"))
        digest.update(b"\x1f")
    return digest.hexdigest()
