# data/cache_handler.py
"""
Асинхронний клієнт Redis із підтримкою Heroku (TLS → rediss://) та локального
режиму (redis://). Додає:
* вимкнену перевірку сертифіката для self‑signed TLS на Heroku;
* уніфіковане DEBUG‑логування (VALID / NO_DATA / ERROR);
* сумісність із redis==4.5.5 і Rich‑логуванням.

# === Оновлено 2025‑04‑16
"""

from __future__ import annotations

import json
import logging
import os
import ssl
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
from redis.asyncio import Redis
from redis.exceptions import RedisError
from rich.console import Console
from rich.logging import RichHandler

# ──────────────────────────  логування  ──────────────────────────
logger = logging.getLogger("cache_handler")
logger.setLevel(logging.DEBUG)                         # детальне логування

console = Console()
rich_handler = RichHandler(console=console, show_path=False)
rich_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False
# ──────────────────────────────────────────────────────────────────

# Константи
CACHE_STATUS_VALID = "VALID"
CACHE_STATUS_NODATA = "NO_DATA"
DEFAULT_TTL = 3_600  # сек


class SimpleCacheHandler:
    """
    Легковаговий async‑клієнт Redis.

    Підтримує:
    * `redis://`  → локальний Redis без TLS;
    * `rediss://` → Heroku Redis із TLS (self‑signed).
    """

    # ------------------------------------------------------------------ #
    # ініціалізація
    # ------------------------------------------------------------------ #
    def __init__(self, redis_url: Optional[str] = None) -> None:
        """
        Ініціалізує клієнт Redis за URL або за змінною середовища `REDIS_URL`.
        """
        self.client: Redis | None = None
        redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._init_from_url(redis_url)

    # ------------------------------------------------------------------ #
    # внутрішні допоміжні методи
    # ------------------------------------------------------------------ #
    def _init_from_url(self, redis_url: str) -> None:
        """Створює клієнт Redis з коректними TLS‑параметрами."""
        parsed = urlparse(redis_url)

        # redis‑py 4.5.5 розпізнає TLS за схемою rediss://
        tls_kwargs = (
            {
                "ssl_cert_reqs": ssl.CERT_NONE,     # вимкнути перевірку сертифіката
                "ssl_check_hostname": False,        # вимкнути перевірку host‑name
            }
            if parsed.scheme == "rediss"
            else {}
        )

        try:
            self.client = Redis.from_url(
                redis_url,
                decode_responses=True,
                health_check_interval=30,           # ping кожні 30 с
                retry_on_error=[ConnectionError, TimeoutError],
                **tls_kwargs,
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

    # ------------------------------------------------------------------ #
    # публічне API
    # ------------------------------------------------------------------ #
    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        data_json: str,
        ttl: int = DEFAULT_TTL,
        prefix: Optional[str] = None,
    ) -> None:
        """Записує дані у Redis з ключем `prefix:symbol:interval`."""
        if not self.client:
            logger.error("[Redis][ERROR] client is not initialised")
            return
        key = self._format_key(symbol, interval, prefix)
        try:
            await self.client.setex(key, ttl, data_json)
            logger.debug("[Redis][SET] %s (TTL=%s c)", key, ttl)
        except Exception as exc:
            logger.error("[Redis][SET] %s failed: %s", key, exc)

    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Отримує дані з Redis → DataFrame або dict.

        Логує статус кешу (VALID / NO_DATA) та TTL.
        """
        if not self.client:
            logger.error("[Redis][ERROR] client is not initialised")
            return None
        key = self._format_key(symbol, interval, prefix)
        try:
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
        except Exception as exc:
            logger.error("[Redis][GET] %s failed: %s", key, exc)
            return None

    async def delete_from_cache(
        self, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> bool:
        """Видаляє ключ із Redis та повертає успішність операції."""
        if not self.client:
            logger.error("[Redis][ERROR] client is not initialised")
            return False
        key = self._format_key(symbol, interval, prefix)
        try:
            result = await self.client.delete(key)
            logger.debug("[Redis][DEL] %s → %s", key, bool(result))
            return bool(result)
        except Exception as exc:
            logger.error("[Redis][DEL] %s failed: %s", key, exc)
            return False

    async def is_key_exists(
        self, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> bool:
        """Перевіряє наявність ключа у Redis."""
        if not self.client:
            logger.error("[Redis][ERROR] client is not initialised")
            return False
        key = self._format_key(symbol, interval, prefix)
        try:
            exists = await self.client.exists(key)
            logger.debug("[Redis][EXISTS] %s → %s", key, bool(exists))
            return bool(exists)
        except Exception as exc:
            logger.error("[Redis][EXISTS] %s failed: %s", key, exc)
            return False

    async def get_remaining_ttl(
        self, symbol: str, interval: str, prefix: Optional[str] = None
    ) -> int:
        """
        Повертає, скільки TTL залишилось:
        * >=0 — секунди до протухання;
        * -1   — безстроковий;
        * -2   — помилка.
        """
        if not self.client:
            logger.error("[Redis][ERROR] client is not initialised")
            return -2
        key = self._format_key(symbol, interval, prefix)
        try:
            ttl = await self.client.ttl(key)
            logger.debug("[Redis][TTL] %s → %s", key, ttl)
            return ttl
        except Exception as exc:
            logger.error("[Redis][TTL] %s failed: %s", key, exc)
            return -2

    # ------------------------------------------------------------------ #
    # утиліти
    # ------------------------------------------------------------------ #
    @staticmethod
    def _format_key(symbol: str, interval: str, prefix: Optional[str] = None) -> str:
        """Формує ключ у форматі `prefix:symbol:interval`."""
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"
