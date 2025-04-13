# monitor/asset_selector/optimized_asset_filter.py
"""
Швидкий + абсолютний фільтр USDT‑M‑ф’ючерсів Binance.

Головні можливості
------------------
* Один REST‑прохід: /ticker/24hr  ➜  базові метрики
* Паралельний збір Open Interest з обмеженим семафором
* Опціональні динамічні пороги (75‑й / 70‑й перцентилі)
* Кешування exchangeInfo у Redis (3 год)
* Pydantic‑валідація вхідних параметрів і кеш‑схеми
* Детальне логування всіх етапів
* Фінальний ранж за liquidity_score (quoteVol + OI)

Вихід: відсортований список тікерів, готовий для hierarchical_asset_selection.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta
from typing import Dict, List, Tuple

import aiohttp
import numpy as np
import pandas as pd
from pydantic import BaseModel, ValidationError, confloat, conint
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
)
from aiohttp import ClientResponseError

logger = logging.getLogger("optimized_asset_filter")

# --------------------------------------------------------------------------- #
#                               ── CONSTANTS ──                               #
# --------------------------------------------------------------------------- #
OI_SEMAPHORE = asyncio.Semaphore(10)  # макс. 10 паралельних запитів OI


# --------------------------------------------------------------------------- #
#                                ── MODELS ──                                 #
# --------------------------------------------------------------------------- #
class SymbolInfo(BaseModel):
    """Схема одного елемента exchangeInfo["symbols"] (спрощена)."""

    symbol: str
    status: str
    baseAsset: str
    quoteAsset: str


class FilterParams(BaseModel):
    """Вхідні пороги з жорсткою валідацією."""

    min_quote_volume: confloat(ge=0) = 1_000_000.0
    min_price_change: confloat(ge=0) = 3.0
    min_open_interest: confloat(ge=0) = 500_000.0
    max_symbols: conint(ge=1) = 30
    dynamic: bool = False


# --------------------------------------------------------------------------- #
#                               ── HELPERS ──                                 #
# --------------------------------------------------------------------------- #
class FuturesFilterError(Exception):
    """Кастомний виняток для помилок фільтрації/обробки даних."""


def _is_retryable(exc: BaseException) -> bool:
    """Повторюємо лише network / 5xx; 400/404 – не ретраїмо."""
    if isinstance(exc, aiohttp.ClientConnectionError):
        return True
    if isinstance(exc, aiohttp.ClientResponseError) and exc.status >= 500:
        return True
    return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
    before_sleep=lambda s: logger.warning(
        "Retry %s: %s", s.attempt_number, s.outcome.exception()
    ),
)
async def _fetch_json(session: aiohttp.ClientSession, url: str) -> list | dict:
    """
    Функція виконує HTTP GET запит до вказаного URL і повертає JSON.
    Якщо виникає помилка (наприклад, HTTP 451 або інша),
    логгує повідомлення та повертає порожній словник.
    
    Аргументи:
        session: об'єкт aiohttp.ClientSession для виконання запиту.
        url: URL-адреса запиту.
        
    Повертає:
        JSON дані у вигляді списку або словника, або {} при помилці.
    """
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            # Якщо статус відповіді не успішний (4xx, 5xx), кидаємо виключення
            resp.raise_for_status()
            # Повертаємо отриманий JSON
            return await resp.json()
    except ClientResponseError as e:
        if e.status == 451:
            # Якщо статус 451 (доступ заборонено з юридичних причин),
            # логгуємо помилку і не повторюємо запит
            logger.error("HTTP 451: Доступ заблоковано для URL %s", url)
        else:
            logger.error("HTTP помилка %s для URL %s", e.status, url)
    except Exception as exc:
        # Логування інших неочікуваних помилок з детальним traceback
        logger.error("Неочікувана помилка при запиті до URL %s: %s", url, exc, exc_info=True)
    # Повертаємо порожній словник, якщо виникла помилка
    return {}  


async def _fetch_open_interest(
    session: aiohttp.ClientSession, symbols: List[str]
) -> pd.DataFrame:
    """
    Паралельно запитує /fapi/v1/openInterest для кожного символа
    з семафорним обмеженням.
    """
    async def _oi(sym: str) -> Tuple[str, float]:
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}"
        async with OI_SEMAPHORE:
            try:
                data = await _fetch_json(session, url)     # 400/404 не ретраїться
                return sym, float(data["openInterest"])
            except aiohttp.ClientResponseError as exc:
                if exc.status in (400, 404):
                    logger.info("OI %s → %s (контракту немає)", sym, exc.status)
                    return sym, 0.0
                raise
            except Exception as exc:                       # network / 5xx
                logger.warning("OI network %s: %s", sym, exc)
                return sym, 0.0

    res = await asyncio.gather(*(_oi(s) for s in symbols))
    return pd.DataFrame(res, columns=["symbol", "openInterest"])

async def _fetch_exchange_info(cache_handler, session) -> list[dict]:
    """
    Повертає exchangeInfo (USDT‑M Futures) з Redis‑кешу
    або робить REST‑запит до Binance і кешує результат на 3 год.
    """
    key = "binance_futures_exchange_info"

    # ── 1. спроба взяти з кешу ───────────────────────────────────────────────
    cached = await cache_handler.fetch_from_cache(
        symbol=key,
        interval="global",
        prefix="meta"
    )

    # беремо з кешу **лише** якщо це bytes / str (JSON),
    # ігноруємо DataFrame, dict, None тощо
    if isinstance(cached, (bytes, str)):
        try:
            raw = json.loads(cached)
            _ = [SymbolInfo(**item) for item in raw]   # pydantic validation
            return raw
        except (ValidationError, json.JSONDecodeError):
            logger.warning("Кеш exchangeInfo пошкоджений – очищаю.")
            await cache_handler.delete_from_cache(
                symbol=key,
                interval="global",
                prefix="meta"
            )

    # ── 2. REST‑запит до Binance ────────────────────────────────────────────
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    data = await _fetch_json(session, url)
    symbols_raw = data["symbols"] if isinstance(data, dict) else data
    validated = [SymbolInfo(**s).dict() for s in symbols_raw]

    # ── 3. зберігаємо у кеш (JSON‑рядком) на 3 год ──────────────────────────
    await cache_handler.store_in_cache(
        symbol=key,
        interval="global",
        data_json=json.dumps(validated),
        ttl=3 * 3600,
        prefix="meta"
    )
    return validated


# ──────────────────────────────  CORE FILTER  ──────────────────────────────
async def quick_absolute_filter(
    session: aiohttp.ClientSession,
    *,
    cache_handler,
    params: Dict | FilterParams,
) -> List[str]:
    p = params if isinstance(params, FilterParams) else FilterParams(**params)

    # 1) /ticker/24hr ---------------------------------------------------------
    ticker_df = pd.DataFrame(
        await _fetch_json(session, "https://fapi.binance.com/fapi/v1/ticker/24hr")
    )
    if ticker_df.empty:
        logger.error("ticker/24hr повернув порожній набір.")
        return []

    # 2) exchangeInfo  --------------------------------------------------------
    ex_symbols = await _fetch_exchange_info(cache_handler, session)
    valid_futs = {
        s["symbol"]
        for s in ex_symbols
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }

    # 3) (optional) dynamic thresholds ---------------------------------------
    if p.dynamic:
        clean = ticker_df[
            pd.to_numeric(ticker_df["quoteVolume"], errors="coerce").notna()
            & pd.to_numeric(ticker_df["priceChangePercent"], errors="coerce").notna()
        ]
        if clean.empty:
            logger.error("Немає даних для dynamic thresholds.")
            return []
        p.min_quote_volume = float(np.nanpercentile(clean["quoteVolume"].astype(float), 75))
        p.min_price_change = float(
            np.nanpercentile(clean["priceChangePercent"].abs().astype(float), 70)
        )
        logger.info("Dynamic thresholds → vol≥%.0f Δ%%≥%.2f",
                    p.min_quote_volume, p.min_price_change)

    # 4) базовий фільтр -------------------------------------------------------
    ticker_df["quoteVolume"] = pd.to_numeric(ticker_df["quoteVolume"], errors="coerce")
    ticker_df["priceChangePercent"] = pd.to_numeric(
        ticker_df["priceChangePercent"], errors="coerce"
    )

    mask = (
        ticker_df["symbol"].isin(valid_futs)
        & (ticker_df["quoteVolume"] >= p.min_quote_volume)
        & (ticker_df["priceChangePercent"].abs() >= p.min_price_change)
    )
    pre_df = ticker_df[mask]
    if pre_df.empty:
        logger.warning("Після базових фільтрів символів немає.")
        return []

    # 5) Open Interest --------------------------------------------------------
    oi_df = await _fetch_open_interest(session, pre_df["symbol"].tolist())
    merged = pre_df.merge(oi_df, on="symbol", how="inner", validate="one_to_one")
    if merged.empty:
        logger.warning("Після merge з OI — 0.")
        return []

    merged["openInterest"] = pd.to_numeric(
        merged["openInterest"], errors="coerce"
    ).fillna(0)
    merged = merged[merged["openInterest"] >= p.min_open_interest]
    if merged.empty:
        logger.warning("Жоден не пройшов поріг OI.")
        return []

    # 6) liquidity_score + сортування ----------------------------------------
    merged["liquidity_score"] = (
        0.6 * merged["quoteVolume"] / merged["quoteVolume"].max()
        + 0.4 * merged["openInterest"] / merged["openInterest"].max()
    )

    final: list[str] = (
        merged.sort_values(["liquidity_score", "quoteVolume"],
                           ascending=[False, False])
        .head(p.max_symbols)["symbol"]
        .tolist()
    )

    # 7) логування ------------------------------------------------------------
    logger.info(
        "Етапи фільтрації:\n"
        "  1. Початково:  %d\n"
        "  2. Після бази: %d\n"
        "  3. Після OI:   %d\n"
        "  4. Фінал:      %d",
        len(ticker_df), len(pre_df), len(merged), len(final)
    )
    if not final:
        logger.warning("Фінальний список символів порожній.")
    elif len(final) < p.max_symbols:
        logger.info("Знайдено лише %d із %d запитуваних символів.",
                    len(final), p.max_symbols)
    else:
        logger.debug("Фінальний список: %s", final)

    return final


# ──────────────────────────────  PUBLIC API  ─────────────────────────────── #
async def get_prefiltered_symbols(
    session: aiohttp.ClientSession,
    cache_handler,
    thresholds: Dict[str, float],
    *,
    dynamic: bool = False,
) -> List[str]:
    """
    Обгортка для використання у main.py / pipeline.

    Example
    -------
    >>> fast_syms = await get_prefiltered_symbols(
    ...     session, cache, {
    ...         "MIN_QUOTE_VOLUME": 1_500_000,
    ...         "MIN_PRICE_CHANGE": 2.5,
    ...         "MIN_OPEN_INTEREST": 600_000,
    ...         "MAX_SYMBOLS": 80,
    ...     }, dynamic=False
    ... )
    """
    params = FilterParams(
        min_quote_volume=thresholds.get("MIN_QUOTE_VOLUME", 1_000_000.0),
        min_price_change=thresholds.get("MIN_PRICE_CHANGE", 3.0),
        min_open_interest=thresholds.get("MIN_OPEN_INTEREST", 500_000.0),
        max_symbols=int(thresholds.get("MAX_SYMBOLS", 80)),
        dynamic=dynamic,
    )
    return await quick_absolute_filter(
        session=session,
        cache_handler=cache_handler,
        params=params,
    )



