"""
data/cache_handler.py
────────────────────────────────────────────────────────────────────────────
Універсальний асинхронний клієнт для Redis.

✔  Підтримка Heroku (`rediss://`, self‑signed TLS) і локального Redis.  
✔  Автоматичні повтори (back‑off + jitter) при мережевих збоях.  
✔  Ліміт пулу з’єднань (MAX_CONNECTIONS = 18) — безпечна межа Heroku Mini.  
✔  Детальне Rich‑логування (DEBUG).  
✔  Додаткові утиліти `ping`, `close`, `set_json`, `get_json`.  
✔  Сумісність із redis‑py 4.5.5 — жодних «свіжих» параметрів.

Типові сценарії помилок, які перехоплюються і лікуються:
    • TCP обрив | `ConnectionResetError`
    • TLS handshake | `ConnectionError`
    • `Too many connections` від сервера Redis
    • `TimeoutError` клієнта
    • `BusyLoadingError` (перезапуск Redis)
    • Некоректний або прострочений self‑signed сертифікат

Автор: AiOne_t • Оновлено 2025‑04‑18
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
from functools import wraps
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
from redis.asyncio import Redis
from redis.exceptions import (
    AuthenticationError,
    BusyLoadingError,
    ConnectionError as RedisConnectionError,
    RedisError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)
from rich.console import Console
from rich.logging import RichHandler

# ─────────────────────────── ЛОГУВАННЯ ────────────────────────────
logger = logging.getLogger("cache_handler")
logger.setLevel(logging.DEBUG)

console = Console()
rich_handler = RichHandler(console=console, show_path=False, omit_repeated_times=False)
rich_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False
# ───────────────────────────────────────────────────────────────────

# ──────────── Константи та налаштування ────────────
CACHE_STATUS_VALID = "VALID"
CACHE_STATUS_NODATA = "NO_DATA"

DEFAULT_TTL: int = 60 * 60          # 1 год
MAX_RETRIES: int = 3                # спроби при збої
BASE_DELAY: float = 0.4             # початкова затримка між спробами
MAX_CONNECTIONS: int = 18           # ліміт для Heroku Mini (18/20 allowed)
HEALTH_CHECK_SEC: int = 30          # періодичний PING
# ───────────────────────────────────────────────────

# ╭─ утиліта‑декоратор з автоматичними повторами ─╮
def with_retry(func):
    """
    Декоратор для методів, що працюють із Redis.

    * Повтор до MAX_RETRIES із back‑off + jitter.
    * Перехоплює як built‑in (`ConnectionError`, `TimeoutError`),
      так і redis‑py (`RedisConnectionError`, `RedisTimeoutError`) винятки.
    * Після кожного збою закриває ВСІ з’єднання пулу (`disconnect()`),
      щоб не накопичувалися «завислі» конекшени.
    """

    retriable = (
        ConnectionError,            # built‑in
        TimeoutError,               # built‑in
        ConnectionResetError,       # TCP RST
        RedisConnectionError,       # redis‑py (включає Too many connections)
        RedisTimeoutError,          # redis‑py
    )

    @wraps(func)
    async def wrapper(self: "SimpleCacheHandler", *args, **kwargs):
        delay = BASE_DELAY
        for attempt in range(1, MAX_RETRIES + 2):               # 1 + MAX_RETRIES
            try:
                return await func(self, *args, **kwargs)
            except retriable as exc:
                logger.warning(
                    "[Redis][RETRY %d/%d] %s: %s",
                    attempt, MAX_RETRIES, func.__name__, exc
                )
                # «Лікуємо» пул: закриваємо всі існуючі конекшени
                try:
                    await self.client.connection_pool.disconnect(inuse_connections=True)
                except Exception:
                    pass

                if attempt > MAX_RETRIES:
                    raise

                # jitter, щоб розсинхронізувати паралельні корутини
                await asyncio.sleep(delay + random.uniform(0, delay / 4))
                delay *= 2

    return wrapper
# ╰───────────────────────────────────────────────╯


class SimpleCacheHandler:
    """
    Асинхронний клієнт Redis (Singleton per process).

    Параметри
    ---------
    redis_url : str | None
        Повна URI‑стрічка. Якщо не задано — читається з ENV `REDIS_URL`.
    host, port, db : опціональна трійка для локального Redis
        (ігнорується, якщо є `redis_url`).
    """

    # ─────────────────── ініціалізація ────────────────────
    def __init__(
        self,
        redis_url: str | None = None,
        *,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
    ) -> None:
        # 1. Визначаємо URI
        if redis_url is None:
            redis_url = os.getenv("REDIS_URL")
        if not redis_url and host:
            redis_url = f"redis://{host}:{port or 6379}/{db}"
        if not redis_url:
            redis_url = "redis://localhost:6379/0"

        # 2. Створюємо клієнт
        self.client: Redis = self._create_client(redis_url)

    # ─────────────────── внутрішні методи ────────────────────
    @staticmethod
    def _add_tls_params(url: str) -> str:
        """
        Для `rediss://…` додає:
        • `ssl_cert_reqs=none`         – вимикає валідацію сертифіката  
        • `ssl_check_hostname=false`   – вимикає перевірку CN
        """
        parsed = urlparse(url)
        if parsed.scheme != "rediss":
            return url

        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.setdefault("ssl_cert_reqs", ["none"])
        qs.setdefault("ssl_check_hostname", ["false"])
        return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))

    def _create_client(self, redis_url: str) -> Redis:
        """Створює Redis‑клієнт з обмеженим пулом та захистом TLS."""
        redis_url = self._add_tls_params(redis_url)
        parsed = urlparse(redis_url)

        try:
            client = Redis.from_url(
                redis_url,
                decode_responses=True,
                health_check_interval=HEALTH_CHECK_SEC,
                socket_keepalive=True,
                max_connections=MAX_CONNECTIONS,
                retry_on_error=[RedisConnectionError, RedisTimeoutError],
            )
            logger.info(
                "[Redis][INIT] Connected to %s://%s:%s (TLS=%s, pool=%s)",
                parsed.scheme, parsed.hostname, parsed.port,
                parsed.scheme == "rediss", MAX_CONNECTIONS
            )
            return client

        # Деталізуємо типові критичні кейси:
        except (AuthenticationError, ResponseError) as exc:
            logger.critical("[Redis][AUTH] Creds/config error: %s", exc)
            sys.exit(1)
        except BusyLoadingError as exc:
            logger.error("[Redis][BUSY] Redis is loading data: %s", exc)
            raise
        except RedisError as exc:
            logger.exception("[Redis][INIT] Redis‑error: %s", exc)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("[Redis][INIT] Unknown error: %s", exc)
            raise

    # ────────────────────── ПУБЛІЧНЕ API ──────────────────────
    # ----------- базові операції (JSON‑рядок як payload) -----------
    @with_retry
    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        data_json: str,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
    ) -> None:
        """Зберегти рядок JSON під ключем `[prefix:]symbol:interval`."""
        key = self._format_key(symbol, interval, prefix)
        await self.client.setex(key, ttl, data_json)
        logger.debug("[Redis][SET] %s (TTL=%s c)", key, ttl)

    @with_retry
    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> Optional[pd.DataFrame | dict]:
        """
        Повертає:
        • DataFrame — якщо збережено список об'єктів;  
        • dict / raw — для інших форматів;  
        • None — якщо ключа нема.
        """
        key = self._format_key(symbol, interval, prefix)
        data_json = await self.client.get(key)
        status = CACHE_STATUS_VALID if data_json else CACHE_STATUS_NODATA
        logger.debug("[Redis][GET] %s → %s", key, status)

        if not data_json:
            return None

        try:
            data = json.loads(data_json)
        except json.JSONDecodeError as exc:
            logger.error("[Redis][DECODE] %s: %s", key, exc)
            return None

        if isinstance(data, list):
            df = pd.DataFrame(data)
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        return data

    @with_retry
    async def delete_from_cache(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> bool:
        key = self._format_key(symbol, interval, prefix)
        deleted = await self.client.delete(key)
        logger.debug("[Redis][DEL] %s → %s", key, bool(deleted))
        return bool(deleted)

    # ----------- інформаційні операції -----------
    @with_retry
    async def is_key_exists(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> bool:
        key = self._format_key(symbol, interval, prefix)
        exists = await self.client.exists(key)
        logger.debug("[Redis][EXISTS] %s → %s", key, bool(exists))
        return bool(exists)

    @with_retry
    async def get_remaining_ttl(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> int:
        """
        Повертає TTL у секундах:
        • >=0 – скільки залишилось;  
        • -1  – безстроковий;  
        • -2  – помилка.
        """
        key = self._format_key(symbol, interval, prefix)
        ttl = await self.client.ttl(key)
        logger.debug("[Redis][TTL] %s → %s", key, ttl)
        return ttl

    # ----------- корисні утиліти -----------
    @with_retry
    async def ping(self) -> bool:
        """Health‑check з’єднання (`PING`)."""
        try:
            pong = await self.client.ping()
            logger.debug("[Redis][PING] → %s", pong)
            return bool(pong)
        except RedisError as exc:
            logger.error("[Redis][PING] Error: %s", exc)
            return False

    async def close(self) -> None:
        """Graceful‑shutdown: закриває клієнт і пул."""
        await self.client.close()
        await self.client.connection_pool.disconnect(inuse_connections=True)
        logger.info("[Redis][CLOSE] Пул з’єднань закрито.")

    # ----------- обгортки для роботи з Python‑об’єктами -----------
    async def set_json(
        self,
        symbol: str,
        interval: str,
        obj: Any,
        *,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
        ensure_ascii: bool = False,
    ) -> None:
        """Серіалізує `obj` у JSON та кладе в кеш."""
        await self.store_in_cache(
            symbol,
            interval,
            json.dumps(obj, ensure_ascii=ensure_ascii),
            ttl=ttl,
            prefix=prefix,
        )

    async def get_json(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> Optional[Any]:
        """Повертає Python‑об’єкт, якщо у кеші збережено JSON."""
        data = await self.fetch_from_cache(symbol, interval, prefix)
        if isinstance(data, (dict, list)):
            return data
        return None

    # ─────────────────────── утиліти ───────────────────────
    @staticmethod
    def _format_key(symbol: str, interval: str, prefix: Optional[str]) -> str:
        """Повертає ключ формату `prefix:symbol:interval` або `symbol:interval`."""
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"
