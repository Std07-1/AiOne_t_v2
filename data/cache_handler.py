"""
data/cache_handler.py
────────────────────────────────────────────────────────────────────────────
Універсальний асинхронний клієнт для Redis:

1. Підтримує:
   • Heroku (`rediss://`, self‑signed TLS)  
   • Локальний Redis (`redis://` або host/port)  
2. Вбудований повтор із back‑off при мережевих збоях (до MAX_RETRIES).  
3. Обмежує пул з’єднань (max_connections) → безпечний для Heroku Mini.  
4. Детальне Rich‑логування з рівнем DEBUG.  
5. Додаткові методи‑утиліти (`ping`, `close`, `get_json`, `set_json`).  
6. Працює з redis‑py 4.5.5, тож ми не використовуємо параметрів,
   доданих у пізніших версіях.

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
    ConnectionError as RedisConnError,
    RedisError,
    ResponseError,
    TimeoutError as RedisTimeoutError,
)
from rich.console import Console
from rich.logging import RichHandler

# ──────────────────────────── ЛОГУВАННЯ ────────────────────────────
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
# ────────────────────────────────────────────────────────────────────

# ──────────── Константи та налаштування, легко редагувати ───────────
CACHE_STATUS_VALID = "VALID"
CACHE_STATUS_NODATA = "NO_DATA"

DEFAULT_TTL: int = 60 * 60  # 1 год за замовчуванням
MAX_RETRIES: int = 3        # скільки разів намагатись повторити
BASE_DELAY: float = 0.4     # початкова затримка (сек) перед повтором
MAX_CONNECTIONS: int = 10   # safe‑ліміт для плану Heroku Mini (18 max)
HEALTH_CHECK_SEC: int = 30  # Redis Ping кожні N сек для підтримання з’єднання
# ────────────────────────────────────────────────────────────────────


# ╭─ утиліта‑декоратор з автоматичними повторами ─╮
def with_retry(func):
    """
    Декоратор для методів, що роблять I/O з Redis.

    * Повторює виклик до MAX_RETRIES разів (експоненційний back‑off + jitter).
    * Перехоплює як системні (`ConnectionError`, `TimeoutError`),
      так і redis‑py винятки.
    * На кожному збої намагається «вилікувати» пул (`disconnect()`).
    """

    retriable = (
        ConnectionError,
        TimeoutError,
        ConnectionResetError,
        RedisConnError,
        RedisTimeoutError,
    )

    @wraps(func)
    async def wrapper(self: "SimpleCacheHandler", *args, **kwargs):
        delay = BASE_DELAY
        for attempt in range(1, MAX_RETRIES + 2):  # 1 + MAX_RETRIES
            try:
                return await func(self, *args, **kwargs)
            except retriable as exc:
                logger.warning(
                    "[Redis][RETRY %d/%d] %s: %s",
                    attempt,
                    MAX_RETRIES,
                    func.__name__,
                    exc,
                )
                # «Лікуємо» пул з’єднань
                try:
                    self.client.connection_pool.disconnect(inuse_connections=True)
                except Exception:
                    pass

                if attempt > MAX_RETRIES:
                    raise

                # jitter — щоб уникнути «шторму» паралельних повторів
                await asyncio.sleep(delay + random.uniform(0, delay / 4))
                delay *= 2

    return wrapper


# ╰───────────────────────────────────────────────╯


class SimpleCacheHandler:
    """
    Єдиний асинхронний клієнт Redis на процес.

    Parameters
    ----------
    redis_url : str | None
        Повна URI‑стрічка; якщо не задано — читається з ENV `REDIS_URL`.
    host, port, db : опціональна трійка для локального Redis.
    """

    # ────────────────────── ініціалізація ──────────────────────
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
        Для `rediss://…` додаємо параметри:
        * `ssl_cert_reqs=none`
        * `ssl_check_hostname=false`
        щоб обійти self‑signed сертифікат Heroku.
        """
        parsed = urlparse(url)
        if parsed.scheme != "rediss":
            return url

        qs = parse_qs(parsed.query, keep_blank_values=True)
        qs.setdefault("ssl_cert_reqs", ["none"])
        qs.setdefault("ssl_check_hostname", ["false"])
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def _create_client(self, redis_url: str) -> Redis:
        """
        Створює підключення з обмеженим пулом і health‑check‑пінгами.
        """
        redis_url = self._add_tls_params(redis_url)
        parsed = urlparse(redis_url)

        try:
            client = Redis.from_url(
                redis_url,
                decode_responses=True,
                health_check_interval=HEALTH_CHECK_SEC,
                socket_keepalive=True,
                max_connections=MAX_CONNECTIONS,
                retry_on_error=[RedisConnError, RedisTimeoutError],
            )
            logger.info(
                "[Redis][INIT] Connected to %s://%s:%s (TLS=%s, pool=%s)",
                parsed.scheme,
                parsed.hostname,
                parsed.port,
                parsed.scheme == "rediss",
                MAX_CONNECTIONS,
            )
            return client
        except (AuthenticationError, ResponseError) as exc:
            logger.critical("[Redis][AUTH] Неавторизовано або config‑помилка: %s", exc)
            sys.exit(1)
        except BusyLoadingError as exc:
            logger.error("[Redis][BUSY] Redis завантажує дані: %s", exc)
            raise
        except RedisError as exc:
            logger.exception("[Redis][INIT] Redis‑помилка: %s", exc)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("[Redis][INIT] Невідома помилка: %s", exc)
            raise

    # ───────────────────── публічне API ──────────────────────
    @with_retry
    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        data_json: str,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
    ) -> None:
        """
        Зберегти JSON‑рядок у Redis під ключем:

            [prefix:]<symbol>:<interval>
        """
        key = self._format_key(symbol, interval, prefix)
        await self.client.setex(key, ttl, data_json)
        logger.debug("[Redis][SET] %s (TTL=%s с)", key, ttl)

    @with_retry
    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> Optional[pd.DataFrame | dict]:
        """
        Повертає:
        • DataFrame — якщо збережено список об’єктів;  
        • dict / raw type — якщо інший формат;  
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
        """Повертає TTL у секундах або -1 (безстроковий) / -2 (помилка)."""
        key = self._format_key(symbol, interval, prefix)
        ttl = await self.client.ttl(key)
        logger.debug("[Redis][TTL] %s → %s", key, ttl)
        return ttl

    # ───────────── додаткові корисні методи ─────────────
    @with_retry
    async def ping(self) -> bool:
        """Перевірка з’єднання; повертає True/False."""
        try:
            pong = await self.client.ping()
            logger.debug("[Redis][PING] → %s", pong)
            return bool(pong)
        except RedisError as exc:
            logger.error("[Redis][PING] Помилка: %s", exc)
            return False

    async def close(self) -> None:
        """Акуратно закриває пул з’єднань (для graceful‑shutdown)."""
        await self.client.close()
        await self.client.connection_pool.disconnect(inuse_connections=True)
        logger.info("[Redis][CLOSE] З’єднання закрито.")

    # Швидкі обгортки для зберігання / зчитування JSON‑об’єктів
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
        data = await self.fetch_from_cache(symbol, interval, prefix)
        if isinstance(data, (dict, list)):
            return data
        return None

    # ─────────────────────── утиліти ───────────────────────
    @staticmethod
    def _format_key(symbol: str, interval: str, prefix: Optional[str]) -> str:
        """Формує ключ «prefix:symbol:interval» або «symbol:interval»."""
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"
