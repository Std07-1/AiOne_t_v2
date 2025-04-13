# raw_data.py

import aiohttp
import asyncio
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
import json

from monitor.asset_selector.utils import standardize_format, get_ttl_for_interval
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn
)

# Отримуємо логгер для модуля raw_data
logger = logging.getLogger("raw_data")
logger.setLevel(logging.INFO)

# Налаштовуємо обробник через RichHandler для більш красивого форматування
console = Console()
rich_handler = RichHandler(console=console, level=logging.INFO, show_path=False)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
rich_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False

# Глобальний семафор на 10 паралельних завантажень
SEMAPHORE = asyncio.Semaphore(25)

# ------------- constants ---------------------------------
MAX_PARALLEL_KLINES = 10                    # одночасних REST‑запитів
_KLINE_SEM = asyncio.Semaphore(MAX_PARALLEL_KLINES)

async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: dict = None,
    max_retries: int = 3,
    backoff_sec: float = 2.0,
    timeout_sec: float = 10.0
) -> str:
    """
    Виконує GET-запит із повторними спробами.
    При помилках з'єднання / таймаутах / невдалих статусах
    робимо до max_retries спроб із паузою backoff_sec.
    Повертає text-респонс або піднімає виняток після вичерпання спроб.
    """
    for attempt in range(1, max_retries + 1):
        try:
            #logger.debug(f"[fetch_with_retry] Запит {url}, спроба={attempt}, params={params}")
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec)
            ) as resp:
                text_resp = await resp.text()
                if resp.status == 200:
                    return text_resp
                else:
                    logger.warning(
                        f"[fetch_with_retry] {url} (спроба={attempt}) => "
                        f"статус={resp.status}, body={text_resp}"
                    )
        except (aiohttp.ClientConnectorError, aiohttp.ClientPayloadError, asyncio.TimeoutError) as e:
            logger.warning(
                f"[fetch_with_retry] {url} (спроба={attempt}) => "
                f"Помилка з'єднання/таймаут: {e}"
            )

        if attempt < max_retries:
            logger.debug(f"[fetch_with_retry] Очікуємо {backoff_sec:.1f} c перед повтором...")
            await asyncio.sleep(backoff_sec)

    # Якщо дійшли сюди - всі спроби провалилися
    raise aiohttp.ClientError(f"[fetch_with_retry] Не вдалося отримати {url} після {max_retries} спроб.")


def parse_futures_exchange_info(text_resp: str) -> dict:
    """
    Перетворює JSON-рядок від Binance (fapi/v1/exchangeInfo) на словник
    формату {"symbols": [...]}, де 'symbols' – список інформації про пари.
    """
    try:
        parsed = json.loads(text_resp)
    except json.JSONDecodeError as e:
        logger.error(f"[FUTURES EXCHANGE] JSON decode error: {e}")
        return {"symbols": []}

    # Перевіримо, чи є ключ 'symbols'
    if "symbols" not in parsed or not isinstance(parsed["symbols"], list):
        logger.warning("[FUTURES EXCHANGE] У відповіді немає поля 'symbols' або воно не є списком.")
        return {"symbols": []}

    # Формуємо DataFrame для логу
    df_symbols = pd.DataFrame(parsed["symbols"])
    logger.debug(f"[FUTURES EXCHANGE] Знайдено {len(df_symbols)} символ(ів).")

    # Повертаємо словник зі списком символів
    return {"symbols": df_symbols.to_dict("records")}

# ───────────────────────────── helper ─────────────────────────────
def _prepare_kline(
    df: pd.DataFrame,
    *,
    resample_to: str | None = None,        # напр. "1h", "4h" або None
) -> pd.DataFrame:
    """
    • Приводить dtypes (float32) → економія RAM  
    • За потреби агрегує дрібні свічки у більший таймфрейм.
    """
    if df.empty:
        return df

    # -- 1. memory friendly dtypes ---------------------------------
    df = df.astype({
        "open":   "float32",
        "high":   "float32",
        "low":    "float32",
        "close":  "float32",
        "volume": "float32",
    })
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

    # -- 2. optional resample --------------------------------------
    if resample_to:
        df = (
            df.set_index("timestamp")
              .resample(resample_to, label="right", closed="right")
              .agg({
                  "open":  "first",
                  "high":  "max",
                  "low":   "min",
                  "close": "last",
                  "volume":"sum",
              })
              .dropna()
              .reset_index()
        )
    return df


class OptimizedDataFetcher:
    def __init__(
        self,
        cache_handler,
        session: aiohttp.ClientSession,
        binance_api_url: str = "https://api.binance.com"
    ):
        """
        :param cache_handler: об'єкт для кешування (SimpleCacheHandler).
        :param session:       уже створений aiohttp.ClientSession (shared).
        :param binance_api_url: рядок, напр. "https://api.binance.com" (для Spot).
        """
        self.cache_handler = cache_handler
        self.session = session
        self.binance_api_url = binance_api_url

    async def _worker_for_symbol(
        self,
        symbol: str,
        interval: str,
        limit: int,
        min_candles: int
    ) -> tuple[str, Optional[pd.DataFrame]]:
        """
        Допоміжна функція, що виконує завантаження даних для окремого символу з обмеженням паралелізму.
        """
        async with _KLINE_SEM:
            df = await self.get_data(symbol, interval, limit, min_candles)
            return symbol, df

    async def get_data_batch(
        self,
        symbols: list[str],
        interval: str = "1d",
        limit: int = 24,
        min_candles: int = 24
    ) -> dict[str, pd.DataFrame]:
        """
        Повертає dict {symbol: DataFrame} для списку symbols.
        Використовується комбінований індикатор завантаження з розширеним прогрес-баром,
        який показує spinner, стрічку прогресу, відсоток, час виконання, очікуваний час до завершення,
        а також динамічне повідомлення з останнім оброблюваним символом.
        Оновлення опису відбувається кожного batch_size завдань.
        """
        total = len(symbols)
        results = {}
        start_time = asyncio.get_event_loop().time()
        batch_size = 10  # оновлювати опис кожні 10 завдань

        with Progress(
            SpinnerColumn(spinner_name="dots"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            TextColumn("Останній: {task.description}"),
            console=console,
            transient=True,
        ) as progress:
            task_id = progress.add_task("Обробка символів...", total=total)
            i = 0
            tasks = [
                asyncio.create_task(self._worker_for_symbol(sym, interval, limit, min_candles))
                for sym in symbols
            ]
            for future in asyncio.as_completed(tasks):
                sym, df = await future
                i += 1
                progress.advance(task_id, 1)
                # Якщо завершено batch_size завдань або це останнє завдання — оновлюємо опис із розрахунком часу
                if i % batch_size == 0 or i == total:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    avg_time = elapsed / i if i else 0
                    remaining = avg_time * (total - i)
                    progress.update(
                        task_id,
                        description=f"[bold green]{sym}[/bold green] | ≈ {remaining:.1f} сек"
                    )
                if isinstance(df, pd.DataFrame) and not df.empty:
                    results[sym] = df
        return results

    
    async def get_data(
        self,
        symbol: str,
        interval: str,
        limit: int = 1000,
        min_candles: int = 24  # мінімально потрібна кількість свічок
    ) -> Optional[pd.DataFrame]:
        """
        1) Читає кеш (якщо є).
        2) Якщо дані актуальні -> інкрементальне оновлення.
        3) Якщо застарілі/немає -> повне завантаження + збереження.
        4) Перевірка на достатню кількість свічок.
        """
        max_age_seconds = get_ttl_for_interval(interval)
        cache_key_prefix = "candles"

        # Отримуємо дані з кешу
        df_cached = await self.cache_handler.fetch_from_cache(
            symbol, interval, prefix=cache_key_prefix
        )

        # Зменшуємо dtypes + resample ДЛЯ df_cached
        if isinstance(df_cached, pd.DataFrame) and not df_cached.empty:
            df_cached = _prepare_kline(
                df_cached,
                resample_to=interval if interval.endswith("m") else None,
            )

        if df_cached is not None:
            df_cached = standardize_format(df_cached, timezone="UTC")

            if self._is_data_actual(df_cached, max_age_seconds, interval):
                logger.debug(
                    f"[GET_DATA] Кеш актуальний для {symbol} ({interval}). "
                    "Інкрементальне оновлення."
                )
                df_updated = await self._incremental_update(
                    symbol, interval, df_cached, cache_key_prefix, max_age_seconds
                )

                # Перевірка після інкрементального оновлення
                if len(df_updated) < min_candles:
                    logger.info(
                        f"[{symbol}] Недостатньо історичних даних після оновлення ({len(df_updated)} свічок)."
                    )
                    return None

                return df_updated

        # Якщо даних немає в кеші або застарілі — отримуємо повні дані
        logger.debug(f"[GET_DATA] Повне завантаження {symbol} ({interval}).")
        df_full = await self._fetch_binance_data(symbol, interval, limit)
        
        # Приводимо dtypes + optional resample ДЛЯ df_full
        if df_full is not None and not df_full.empty:
            df_full = _prepare_kline(
                df_full,
                resample_to=interval if interval.endswith("m") else None,
            )

        if df_full is None or len(df_full) < min_candles:
            logger.warning(
                f"[{symbol}] Недостатньо історичних даних для аналізу ({len(df_full) if df_full is not None else 0} свічок)."
            )
            return None

        await self.cache_handler.store_in_cache(
            symbol=symbol,
            interval=interval,
            data_json=df_full.to_json(orient="records", date_format="iso"),
            ttl=max_age_seconds,
            prefix=cache_key_prefix
        )

        return df_full

    async def _incremental_update(
        self,
        symbol: str,
        interval: str,
        df_cached: pd.DataFrame,
        cache_key_prefix: str,
        ttl: int
    ) -> pd.DataFrame:
        """
        Завантажує нові свічки після останньої мітки (timestamp), оновлює Redis.
        """
        last_cached_timestamp = df_cached["timestamp"].max()
        last_ts = int(last_cached_timestamp.timestamp() * 1000) + 1

        df_new = await self._fetch_binance_data(symbol, interval, limit=1000, start_time=last_ts)
        if df_new.empty:
            logger.debug(f"[INCREMENTAL] {symbol} ({interval}) немає нових даних.")
            return df_cached

        df_merged = (
            pd.concat([df_cached, df_new])
            .drop_duplicates(subset="timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        await self.cache_handler.store_in_cache(
            symbol, interval,
            data_json=df_merged.to_json(orient="records", date_format="iso"),
            ttl=ttl, prefix=cache_key_prefix
        )
        logger.debug(f"[INCREMENTAL] Інкрементальне оновлення {symbol} ({interval}) завершено.")
        return df_merged

    async def _fetch_binance_data(
        self,
        symbol: str,
        interval: str,
        limit: int,
        start_time: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Завантаження даних із Binance Futures (fapi).
        Тут використовуємо одну session + Semaphore + retry.
        """
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_time:
            params["startTime"] = start_time

        # Обмежуємо паралелізм
        async with SEMAPHORE:
            text_resp = await fetch_with_retry(
                session=self.session,
                url=url,
                params=params,
                max_retries=3,
                backoff_sec=2.0,
                timeout_sec=10.0
            )

        # Тепер manually parse JSON -> DataFrame
        try:
            parsed = json.loads(text_resp)
        except json.JSONDecodeError as e:
            logger.error(f"[FETCH] JSON decode error для {symbol} ({interval}): {e}")
            return pd.DataFrame()

        # parsed – це список списків (klines)
        if not parsed or not isinstance(parsed, list):
            logger.debug(f"[FETCH] Порожній або некоректний респонс для {symbol} ({interval}).")
            return pd.DataFrame()

        # Створюємо DataFrame
        columns = [
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
        df = pd.DataFrame(parsed, columns=columns)
        if df.empty:
            logger.debug(f"[FETCH] Порожній DF після перетворення для {symbol} ({interval}).")
            return df

        # Конвертуємо timestamp і числові поля
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].astype(float)

        logger.debug(
            f"[FETCH] Отримано {len(df)} свічок для {symbol} ({interval}) з Binance."
        )
        return df[["timestamp", "open", "high", "low", "close", "volume"]]

    @staticmethod
    def after_midnight_utc() -> bool:
        """
        Чи час у UTC у проміжку 00:00..00:05 => для примусового оновлення daily.
        """
        now_utc = datetime.now(timezone.utc)
        return now_utc.hour == 0 and now_utc.minute < 5

    def _is_data_actual(self, df: pd.DataFrame, max_age_seconds: int, interval: str) -> bool:
        """
        Якщо '1d' та після опівночі => False;
        інакше порівнює (now_utc - останній timestamp) з max_age_seconds.
        """
        last_ts = df["timestamp"].max()
        current_time_utc = datetime.now(timezone.utc)

        # Для інтервалу 1d та після опівночі
        if interval == "1d" and self.after_midnight_utc():
            logger.debug("[AFTER_MIDNIGHT] Примусове оновлення (00:00..00:05 UTC).")
            return False

        age_seconds = (current_time_utc - last_ts).total_seconds()
        age_td = timedelta(seconds=age_seconds)
        actual = age_seconds < max_age_seconds
        max_age_td = timedelta(seconds=max_age_seconds)

        status_str = "OK ✅" if actual else "EXPIRED ❌"
        last_ts_str = last_ts.strftime('%Y-%m-%d %H:%M:%S %Z')

        logger.debug(
            f"Перевірка актуальності кешу:\n"
            f"  ├─ Остання мітка      : {last_ts_str}\n"
            f"  ├─ Вік даних          : {str(age_td)} ({age_seconds:.1f} сек)\n"
            f"  ├─ Макс. допустимий   : {str(max_age_td)} ({max_age_seconds} сек)\n"
            f"  └─ Актуальні          : {actual} ({status_str})"
        )
        return actual

    async def get_exchange_info(self) -> dict:
        """
        Спотова exchangeInfo (не обов'язково), якщо треба.
        """
        endpoint = f"{self.binance_api_url}/api/v3/exchangeInfo"
        text_resp = await fetch_with_retry(self.session, endpoint, max_retries=2, timeout_sec=5.0)

        # parse json
        try:
            parsed = json.loads(text_resp)
        except json.JSONDecodeError:
            logger.error("[SPOT EXCHANGE] JSON decode error.")
            return {}
        logger.debug("[SPOT EXCHANGE] exchangeInfo (spot) завантажено.")
        return parsed

    async def get_futures_exchange_info(self) -> dict:
        """
        Завантажує і парсить Binance Futures exchangeInfo, повертає словник {"symbols": [...]}
        """
        futures_api = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        text_resp = await fetch_with_retry(self.session, futures_api, max_retries=2, timeout_sec=5.0)
        parsed_info = parse_futures_exchange_info(text_resp)

        logger.debug("[FUTURES EXCHANGE] Отримано exchangeInfo. "
                     f"symbols={len(parsed_info.get('symbols', []))}")
        return parsed_info
