# data/cache_handler.py
"""
Async‑клієнт Redis з підтримкою:
* Heroku (rediss://, self‑signed TLS);
* локального Redis (redis:// або host/port);
* автоматичних повторів на ConnectionError/TimeoutError;
* Rich‑логування (DEBUG);
* redis‑py==4.5.5.

# === FINAL | 2025‑04‑16
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from functools import wraps
from typing import Optional
from urllib.parse import (
    parse_qs,
    urlencode,
    urlparse,
    urlunparse,
)

import pandas as pd
from redis.asyncio import Redis
from redis.exceptions import RedisError
from rich.console import Console
from rich.logging import RichHandler

# ────────────────────────── logging ──────────────────────────
logger = logging.getLogger("cache_handler")
logger.setLevel(logging.DEBUG)

console = Console()
rich_handler = RichHandler(console=console, show_path=False)
rich_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False
# ──────────────────────────────────────────────────────────────

CACHE_STATUS_VALID = "VALID"
CACHE_STATUS_NODATA = "NO_DATA"
DEFAULT_TTL = 3_600
MAX_RETRIES = 3
BASE_DELAY = 0.5


# ╭──────────────────── helper: retry decorator ───────────────────╮
def with_retry(func):
    """Повторити MAX_RETRIES разів із backoff при мережевих помилках."""

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        delay = BASE_DELAY
        for attempt in range(1, MAX_RETRIES + 2):  # 1 + MAX_RETRIES
            try:
                return await func(self, *args, **kwargs)
            except (ConnectionError, TimeoutError) as exc:
                logger.warning(
                    "[Redis][RETRY %d/%d] %s: %s",
                    attempt,
                    MAX_RETRIES,
                    func.__name__,
                    exc,
                )
                try:
                    self.client.connection_pool.disconnect(inuse_connections=True)
                except Exception:  # noqa: BLE001
                    pass
                if attempt > MAX_RETRIES:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

    return wrapper


# ╰───────────────────────────────────────────────────────────────╯


class SimpleCacheHandler:
    """
    * Підтримує rediss:// (Heroku) та redis:// / host/port (локально).
    * Один клієнт на весь процес.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        redis_url : str | None
            Повна URI‑стрічка. Якщо не задано — бере REDIS_URL або host/port.
        host, port, db
            Пара для локального підключення (ігнорується, якщо є redis_url).
        """
        self.client: Redis | None = None

        if redis_url is None:
            redis_url = os.getenv("REDIS_URL")
        if redis_url is None and host:
            redis_url = f"redis://{host}:{port or 6379}/{db}"
        if redis_url is None:
            redis_url = "redis://localhost:6379/0"

        self._init_from_url(redis_url)

    # ───────────────────────── internal ──────────────────────────
    @staticmethod
    def _ensure_tls_query(url: str) -> str:
        """Додає ssl_cert_reqs=none&ssl_check_hostname=false для rediss://."""
        parsed = urlparse(url)
        if parsed.scheme != "rediss":
            return url
        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.setdefault("ssl_cert_reqs", ["none"])
        qs.setdefault("ssl_check_hostname", ["false"])
        return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

    def _init_from_url(self, redis_url: str) -> None:
        redis_url = self._ensure_tls_query(redis_url)
        parsed = urlparse(redis_url)

        try:
            self.client = Redis.from_url(
                redis_url,
                decode_responses=True,
                health_check_interval=30,
                retry_on_error=[ConnectionError, TimeoutError],
                socket_keepalive=True,
            )
            logger.info(
                "[Redis][INIT] Connected to %s://%s:%s (tls=%s)",
                parsed.scheme,
                parsed.hostname,
                parsed.port,
                parsed.scheme == "rediss",
            )
        except RedisError as exc:
            logger.exception("[Redis][INIT] Redis‑помилка: %s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[Redis][INIT] Невідома помилка: %s", exc)

    # ───────────────────────── public API ─────────────────────────
    @with_retry
    async def store_in_cache(
        self,
        *,
        symbol: str,
        interval: str,
        data_json: str,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
    ) -> None:
        key = self._format_key(symbol, interval, prefix)
        await self.client.setex(key, ttl, data_json)
        logger.debug("[Redis][SET] %s (TTL=%s c)", key, ttl)

    @with_retry
    async def fetch_from_cache(
        self, *, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> Optional[pd.DataFrame | dict]:
        key = self._format_key(symbol, interval, prefix)
        data_json = await self.client.get(key)
        status = CACHE_STATUS_VALID if data_json else CACHE_STATUS_NODATA
        logger.debug("[Redis][GET] %s → %s", key, status)
        if not data_json:
            return None
        data = json.loads(data_json)
        if isinstance(data, list):
            df = pd.DataFrame(data)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        return data

    @with_retry
    async def delete_from_cache(
        self, *, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> bool:
        key = self._format_key(symbol, interval, prefix)
        deleted = await self.client.delete(key)
        logger.debug("[Redis][DEL] %s → %s", key, bool(deleted))
        return bool(deleted)

    @with_retry
    async def is_key_exists(
        self, *, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> bool:
        key = self._format_key(symbol, interval, prefix)
        exists = await self.client.exists(key)
        logger.debug("[Redis][EXISTS] %s → %s", key, bool(exists))
        return bool(exists)

    @with_retry
    async def get_remaining_ttl(
        self, *, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> int:
        key = self._format_key(symbol, interval, prefix)
        ttl = await self.client.ttl(key)
        logger.debug("[Redis][TTL] %s → %s", key, ttl)
        return ttl

    # ───────────────────────── utilities ─────────────────────────
    @staticmethod
    def _format_key(symbol: str, interval: str, prefix: Optional[str]) -> str:
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"
