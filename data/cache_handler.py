# cache_handler.py
import json
import pandas as pd
import logging
from typing import Optional
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler
import ssl
from redis import Redis
from urllib.parse import urlparse

# ──────────────────────────  логування  ──────────────────────────
logger = logging.getLogger("cache_handler")
logger.setLevel(logging.INFO)

console = Console()
rich_handler = RichHandler(console=console, show_path=False)
rich_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False

# ──────────────────────────────────────────────────────────────────

class SimpleCacheHandler:
    """
    Легковаговий async-клієнт Redis із коректною підтримкою SSL/TLS для Heroku Redis.
    """

    # --------- базовий конструктор -----------------------------------------
    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0) -> None:
        """
        Ініціалізує Redis-клієнт без SSL (локально).
        """
        self.client = Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
            # також БЕЗ ssl
        )

    # --------- фабрики ------------------------------------------------------
    @classmethod
    def from_url(cls, redis_url: str) -> "SimpleCacheHandler":
        """
        Створює Redis-клієнт з підтримкою rediss:// (Heroku).
        Redis >= 5.0 не підтримує ssl/ssl_context напряму — все обробляється всередині from_url.
        """
        redis_client = Redis.from_url(
            redis_url,
            decode_responses=True
            # БЕЗ ssl / ssl_context — все вже вбудовано в Redis.from_url
        )

        inst = cls.__new__(cls)
        inst.client = redis_client
        return inst
    

    # --------- основні методи ----------------------------------------------
    async def store_in_cache(
        self,
        symbol: str,
        interval: str,
        data_json: str,
        ttl: int = 3600,
        prefix: Optional[str] = None
    ) -> None:
        """
        Запис даних у Redis зі сформованим ключем prefix:symbol:interval.
        """
        key = self._format_key(symbol, interval, prefix)
        try:
            await self.client.setex(key, ttl, data_json)
            logger.debug(f"[Redis][SET] Ключ='{key}' збережено. TTL={ttl} сек.")
        except Exception as e:
            logger.error(f"[Redis][ERROR] Не вдалося зберегти '{key}': {e}")

    async def fetch_from_cache(
        self,
        symbol: str,
        interval: str,
        prefix: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Отримує дані з Redis у вигляді JSON-списку → DataFrame,
        або dict (якщо зберігався інший формат).
        """
        key = self._format_key(symbol, interval, prefix)
        try:
            data_json = await self.client.get(key)
            if data_json is None:
                logger.debug(f"[Redis][NO_DATA] Ключ='{key}' відсутній.")
                return None

            data = json.loads(data_json)
            # Якщо це список (списки словників) - конвертуємо в DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

                    # Логування інформації про отримані дані
                    last_ts = df["timestamp"].max()
                    now_utc = pd.Timestamp.utcnow()
                    age = (now_utc - last_ts)

                    ttl = await self.client.ttl(key)
                    max_age_str = f"{ttl} сек" if ttl > 0 else "∞"
                    #logger.debug(
                    #    f"[Redis][GET:LIST] Ключ='{key}', записів={len(df)}, "
                    #    f"останній ts={last_ts}, вік={age}, TTL={max_age_str}"
                    #)
                else:
                    logger.debug(f"[Redis][GET:LIST] Ключ='{key}', записів={len(df)} без 'timestamp'")
                return df
            else:
                # Якщо це dict чи інший формат
                #logger.debug(f"[Redis][GET:DICT] Ключ='{key}', Тип даних={type(data)}")
                return data

        except Exception as e:
            logger.error(f"[Redis][ERROR] fetch_from_cache: ключ='{key}', помилка={e}")
            return None

    async def delete_from_cache(self, symbol: str, interval: str, prefix: Optional[str] = None) -> bool:
        key = self._format_key(symbol, interval, prefix)
        try:
            result = await self.client.delete(key)
            if result == 1:
                logger.debug(f"[Redis][DEL] Ключ='{key}' успішно видалено.")
                return True
            else:
                logger.debug(f"[Redis][DEL] Ключ='{key}' не знайдено.")
                return False
        except Exception as e:
            logger.error(f"[Redis][ERROR] delete_from_cache: ключ='{key}', {e}")
            return False

    async def is_key_exists(self, symbol: str, interval: str, prefix: Optional[str] = None) -> bool:
        key = self._format_key(symbol, interval, prefix)
        try:
            exists = await self.client.exists(key)
            logger.debug(f"[Redis][EXISTS] Ключ='{key}', існує={bool(exists)}.")
            return bool(exists)
        except Exception as e:
            logger.error(f"[Redis][ERROR] is_key_exists: ключ='{key}', {e}")
            return False

    async def get_remaining_ttl(self, symbol: str, interval: str, prefix: Optional[str] = None) -> int:
        """
        Отримує скільки лишилось TTL для ключа, або -1 якщо безстроковий, або -2 при помилці.
        """
        key = self._format_key(symbol, interval, prefix)
        try:
            ttl = await self.client.ttl(key)
            if ttl >= 0:
                logger.debug(f"[Redis][TTL] Ключ='{key}', залишилось {ttl} сек.")
            elif ttl == -1:
                logger.debug(f"[Redis][TTL] Ключ='{key}', безстроковий.")
            else:
                logger.debug(f"[Redis][TTL] Ключ='{key}', не знайдено (або помилка).")
            return ttl
        except Exception as e:
            logger.error(f"[Redis][ERROR] get_remaining_ttl: ключ='{key}', {e}")
            return -2

    # --------- утиліти ------------------------------------------------------
    @staticmethod
    def _format_key(symbol: str, interval: str, prefix: Optional[str] = None) -> str:
        return f"{prefix}:{symbol}:{interval}" if prefix else f"{symbol}:{interval}"