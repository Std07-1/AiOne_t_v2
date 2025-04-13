"""
Модуль asset_selector.py

Реалізація першого етапу торгової системи за методом Герчика для моніторингу ф'ючерсних активів.
Використовуються адаптивні пороги, включаючи:
  - Ліквідність (середньоденний обсяг торгів)
  - Волатильність (annualized)
  - Динаміка ціни (тренд: long, short або neutral)
  - Структура руху (впорядкованість)
  - Відносна сила активу (RS) з динамічною оптимізацією
  - Open Interest (через Binance Futures)
  - Фільтрація за ціною (щоб уникнути низьколіквідних активів)

Модуль працює протягом американської торгової сесії, генерує звіт аналізу та повертає фінальний список активів у єдиному форматі.

Автор: [Std07.1]
Дата: [16.03.25]
"""

import asyncio
import logging
from rich.console import Console
from rich.logging import RichHandler

# Отримуємо консоль Rich
console = Console()
from datetime import datetime
from typing import Any, Dict, List
import json
import aiohttp
import numpy as np
import pandas as pd

from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher
from . import config, analyzers, filters, utils

from monitor.asset_selector.utils import dumps_safe   
from monitor.indicators.volatility_analysis import analyze_volatility
# Оновлено: імпортуємо новий швидкий фільтр 
from monitor.asset_selector.optimized_asset_filter import get_prefiltered_symbols

logger = logging.getLogger("asset_selector")
logger.setLevel(logging.DEBUG)

# Тепер усі виклики logger.debug/info/warning/error будуть відображатись красиво.
if not logger.handlers:
    rich_handler = RichHandler(console=console, level=logging.DEBUG, show_path=False)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    rich_handler.setFormatter(formatter)
    logger.addHandler(rich_handler)
logger.propagate = False


async def _fetch_open_interest(
    symbol: str,
    cache_handler: SimpleCacheHandler,
    data_fetcher: OptimizedDataFetcher
) -> float:
    """
    Отримує Open Interest для символу через Binance Futures API.
    Кешуємо OI на 3600 сек у 'futures_oi:OI_{symbol}:global'.
    Обробляємо HTTP 400 як "Bad Request" і повертаємо 0.0.
    """
    session = data_fetcher.session
    url = "https://fapi.binance.com/fapi/v1/openInterest"
    params = {"symbol": symbol}
    cache_key_symbol = f"OI_{symbol}"

    # 1) Спроба зчитати з кешу
    oi_cached = await cache_handler.fetch_from_cache(
        symbol=cache_key_symbol,
        interval="global",
        prefix="futures_oi"
    )
    if isinstance(oi_cached, float):
        logger.debug(
            f"[OPEN_INT][CACHE] {symbol} => {utils.format_open_interest(oi_cached)}"
        )
        return oi_cached

    try:
        logger.debug(f"[OPEN_INT] Запит OI для {symbol}")
        async with session.get(url, params=params) as resp:
            text_resp = await resp.text()
            # Якщо статус 400 – повертаємо 0.0 і не ініціюємо повторну спробу
            if resp.status == 400:
                logger.warning(f"[OPEN_INT] {symbol} повернув 400 (Bad Request). Пропускаємо OI.")
                return 0.0
            if resp.status != 200:
                logger.error(
                    f"[OPEN_INT] {symbol} Помилка {resp.status}, body={text_resp}"
                )
                return 0.0

            data = json.loads(text_resp)
            raw_oi = data.get("openInterest")
            if raw_oi is None:
                logger.debug(f"[OPEN_INT] {symbol} openInterest=null => 0.0")
                return 0.0

            try:
                oi = float(raw_oi)
            except (TypeError, ValueError):
                logger.warning(
                    f"[OPEN_INT] {symbol} Не вдалося конвертувати '{raw_oi}' в float => 0.0"
                )
                oi = 0.0

            if oi < 0:
                logger.warning(
                    f"[OPEN_INT] {symbol} openInterest від'ємне ({oi}), встановлюємо 0.0"
                )
                oi = 0.0

            logger.debug(
                f"[OPEN_INT] {symbol} Fetched OI= ({utils.format_open_interest(oi)})"
            )

            # 3) Записуємо в кеш
            await cache_handler.store_in_cache(
                symbol=cache_key_symbol,
                interval="global",
                data_json=str(oi),
                ttl=3600,
                prefix="futures_oi"
            )
            return oi

    except Exception as e:
        logger.warning(f"[Open Interest] Помилка отримання OI для {symbol}: {e}")
        return 0.0


async def _get_data_and_oi(
    sym: str,
    data_fetcher: OptimizedDataFetcher,
    cache_handler: SimpleCacheHandler,
    session: aiohttp.ClientSession
) -> Dict[str, Any]:
    """
    Асинхронна функція:
      1) Викликає get_data(...) для 1d (24 свічки).
      2) Рахує середньоденний обсяг, волатильність (annual).
      3) Викликає _fetch_open_interest, повертає всі дані у dict.
    Якщо дані відсутні - повертає порожній dict.
    """
    df = await data_fetcher.get_data(sym, interval="1d", limit=24, min_candles=24)
    if df is None or df.empty:
        logger.debug(f"[_get_data_and_oi] {sym}: денні дані відсутні.")
        return {}

    # Стандартизуємо часові мітки
    df = utils.standardize_format(df, timezone="UTC")

    # Обчислюємо середньоденний обсяг та волатильність
    avg_vol = analyzers.calculate_daily_avg_volume_in_usd(df)
    vol_daily = analyzers.calculate_volatility(df)
    vol_annual = vol_daily * np.sqrt(252)
    # Поточна ціна (останнє значення close)
    current_price = df["close"].iloc[-1]

    # Open Interest
    oi = await _fetch_open_interest(sym, cache_handler, data_fetcher)

    return {
        "avg_volume": avg_vol,
        "volatility": vol_annual,
        "open_interest": oi,
        "current_price": current_price
    }


async def fetch_candidates(
    data_fetcher: OptimizedDataFetcher,
    cache_handler: SimpleCacheHandler,
    thresholds: Dict[str, float],
    prefiltered_symbols: List[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Повертає список кандидатів із базовими метриками.
    Дані 1‑денних свічок завантажуються пачкою через
    OptimizedDataFetcher.get_data_batch (≤ 10 паралельних запитів).
    """
    logger.debug("[fetch_candidates] Початок аналізу біржової інформації.")

    # ── 1. Список символів ---------------------------------------------------
    if prefiltered_symbols:
        symbols = prefiltered_symbols
        logger.info("[fetch_candidates] Використовуємо pre‑filter (%d)", len(symbols))
    else:
        fut_info = await data_fetcher.get_futures_exchange_info()
        raw = fut_info.get("symbols", [])
        symbols = [
            s["symbol"] for s in raw
            if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
        ]
        logger.debug("[fetch_candidates] Виявлено %d USDT‑пар (Futures).", len(symbols))

    if not symbols:
        return []

    # ── 2. Забираємо 1d‑свічки пачкою ---------------------------------------
    klines = await data_fetcher.get_data_batch(symbols, interval="1d", limit=24, min_candles=24)

    current_candidate_data: Dict[str, Dict] = {}
    for sym, df in klines.items():
        if df is None or df.empty:
            continue
        avg_volume   = analyzers.calculate_daily_avg_volume_in_usd(df)
        vol_daily    = analyzers.calculate_volatility(df)
        vol_annual   = vol_daily * np.sqrt(252)
        structure_sc = analyzers.calculate_structure_score(df["close"])
        price_change = df["close"].pct_change().iloc[-1] if len(df) >= 2 else 0.0
        current_price = df["close"].iloc[-1]
        vol_inc      = analyzers.is_volume_growing(df)
        vol_mean, vol_std = df["volume"].mean(), (df["volume"].std() or 1.0)
        vol_anom     = (df["volume"].iloc[-1] - vol_mean) / vol_std
        oi           = await _fetch_open_interest(sym, cache_handler, data_fetcher)

        current_candidate_data[sym] = {
            "avg_volume":      avg_volume,
            "avg_volume_str":  utils.format_volume_usd(avg_volume),
            "volatility":      vol_annual,
            "structure_score": structure_sc,
            "price_change":    price_change,
            "trend":           "long" if price_change > 0 else ("short" if price_change < 0 else "neutral"),
            "volume_increasing": bool(vol_inc),
            "volume_anomaly":  vol_anom,
            "open_interest":   oi,
            "current_price":   current_price,
        }

    # ── 3. Формуємо список кандидатів ---------------------------------------
    candidates = [
        {
            "ticker": sym,
            **current_candidate_data[sym]
        }
        for sym in symbols
        if sym in current_candidate_data  # відкидаємо символи без даних
    ]

    # ── 4. Кешуємо результат -------------------------------------------------
    await cache_handler.store_in_cache(
        symbol="previous_candidate_data",
        interval="global",
        data_json=dumps_safe({c["ticker"]: c for c in candidates}),
        ttl=86_400,
        prefix="candidates"
    )

    logger.info("[fetch_candidates] Зібрано %d кандидатів.", len(candidates))
    return candidates


async def fetch_market_data(data_fetcher: OptimizedDataFetcher) -> Dict[str, Any]:
    """
    Отримує ринкові дані для референтного активу (BTCUSDT).
    Обчислює ринковий тренд та волатильність за 1-годинними даними.

    Returns:
        Словник із ключами "index_trend" та "index_volatility".
    """
    try:
        df = await data_fetcher.get_data(symbol="BTCUSDT", interval="1h", limit=100, min_candles=24)
    except Exception as e:
        logger.error(f"[fetch_market_data] get_data викликало помилку: {e}", exc_info=True)
        return {}

    if df is None or df.empty:
        logger.error("[fetch_market_data] Не вдалося отримати ринкові дані (BTCUSDT).")
        return {}

    initial_close = df.iloc[0]["close"]
    latest_close = df.iloc[-1]["close"]
    index_trend = (latest_close - initial_close) / initial_close if initial_close else 0.0
    index_volatility = analyzers.calculate_volatility(df)
    logger.debug(f"[fetch_market_data] index_trend={index_trend:.4f}, index_volatility={index_volatility:.4f}")
    return {"index_trend": index_trend, "index_volatility": index_volatility}


async def generate_analysis_report(
    candidates: List[Dict[str, Any]],
    cache_handler: SimpleCacheHandler,
    data_fetcher: OptimizedDataFetcher,
) -> str:
    """
    Формує текстовий звіт для логів.
    """
    if not candidates:
        return "Кандидатів немає."

    # ── агрегована статистика ───────────────────────────────────────────────
    total = len(candidates)
    long_count   = sum(1 for a in candidates if a.get("trend") == "long")
    short_count  = sum(1 for a in candidates if a.get("trend") == "short")
    neutral_count = total - long_count - short_count

    lines: List[str] = [
        "================= Звіт аналізу активів =================",
        f"Загальна кількість активів: {total}",
        f"Лонг: {long_count}",
        f"Шорт: {short_count}",
        f"Нейтральний: {neutral_count}",
        "--------------------------------------------------------",
    ]

    def classify_z(z: float) -> str:
        if np.isnan(z):
            return "no_data"
        if z >= 3:
            return "extreme_spike"
        if z >= 2:
            return "spike"
        if z >= 1:
            return "moderate"
        if z <= -2:
            return "very_low"
        return "normal"

    for asset in candidates:
        df = await data_fetcher.get_data(asset["ticker"], interval="1d", limit=252, min_candles=24)
        if df is None or df.empty:
            logger.warning("Відсутні дані для %s. Пропускаємо.", asset["ticker"])
            continue

        df = utils.standardize_format(df)
        vol_an = await analyze_volatility(asset["ticker"], df, cache_handler)

        z_val = asset.get("volume_anomaly", np.nan)
        z_txt = classify_z(z_val)
        rel_str = asset.get("rel_strength", 0.0)
        rel_str_txt = "n/a" if abs(rel_str) < 1e-4 else f"{rel_str:.4f}"

        lines.append(
            f"{asset['ticker']}:"
            f"\n   Обсяг: {asset['avg_volume_str']}"
            f"\n   Зміна ціни: {asset['price_change']:.4f}"
            f"\n   Волатильність (річна): {asset['volatility']:.4f}"
            f"\n   (Денна: {vol_an['daily_volatility']:.4f}, "
            f"Тижнева: {vol_an['weekly_volatility']:.4f}, "
            f"Місячна: {vol_an['monthly_volatility']:.4f})"
            f"\n   Стан волатильності: {vol_an['volatility_state']}"
            f"\n   Структура: {asset['structure_score']:.4f}"
            f"\n   Тренд: {asset['trend']}"
            f"\n   Відносна сила: {rel_str_txt}"
            f"\n   Зростання обсягу: {asset['volume_increasing']}"
            f"\n   Аномалія обсягу (Z): {z_val:.2f} ({z_txt})"
            f"\n   Кореляція: {asset.get('correlation_with_index', 0.0):.4f}"
            f"\n   OpenInterest: {utils.format_open_interest(asset['open_interest'])}"
            "\n--------------------------------------------------------"
        )

    report = "\n".join(lines)
    logger.info("[generate_analysis_report] Згенеровано звіт аналізу активів.")
    return report


async def prefilter_symbols(
    data_fetcher: Any,
    cache_handler: Any,
    prefiltered_symbols: List[str] | None = None,
) -> List[str]:
    if prefiltered_symbols is None:
        prefiltered_symbols = await get_prefiltered_symbols(
            session=data_fetcher.session,
            cache_handler=cache_handler,
            thresholds={
                "MIN_QUOTE_VOLUME": 1_500_000,
                "MIN_PRICE_CHANGE": 2.5,
                "MIN_OPEN_INTEREST": 600_000,
                "MAX_SYMBOLS": 180,
            },
            dynamic=False
        )
    logger.info("[hierarchical_asset_selection] Прийнято %d символів на вхід.", len(prefiltered_symbols))
    return prefiltered_symbols


async def get_candidates(
    data_fetcher: Any,
    cache_handler: Any,
    thresholds: Dict[str, float],
    prefiltered_symbols: List[str]
) -> List[Dict[str, Any]]:
    candidates = await fetch_candidates(data_fetcher, cache_handler, thresholds, prefiltered_symbols=prefiltered_symbols)
    if not candidates:
        logger.warning("[hierarchical_asset_selection] Кандидатів немає.")
    return candidates


async def apply_basic_filtering(
    candidates: List[Dict[str, Any]],
    thresholds: Dict[str, float]
) -> List[Dict[str, Any]]:
    base_filtered = []
    for a in candidates:
        MIN_PRICE = 0.01
        if a["current_price"] < MIN_PRICE:
            logger.debug("[Відхилено] %s: низька ціна (%.4f USD).", a["ticker"], a["current_price"])
            continue
        if a["open_interest"] < 250_000 and a["avg_volume"] > 1_000_000:
            logger.warning(
                "[Відхилено] %s: низький OpenInterest (%.1fK) при високому обсязі.",
                a["ticker"], a["open_interest"] / 1_000
            )
            continue

        reasons = []
        if a["avg_volume"] < thresholds["MIN_AVG_VOLUME"]:
            reasons.append("volume")
        if a["volatility"] < max(thresholds["MIN_VOLATILITY"], 0.005):
            reasons.append("volatility")
        if (a["structure_score"] < thresholds["MIN_STRUCTURE_SCORE"] and a["volume_anomaly"] < 2.0):
            reasons.append("structure")
        if a["open_interest"] < thresholds["MIN_OPEN_INTEREST"]:
            reasons.append("OI")
        if not good_volume_anomaly(a, thresholds["MIN_VOLATILITY"], thresholds["MIN_OPEN_INTEREST"]):
            reasons.append("Z")

        if reasons:
            logger.debug("%s відхилено: %s", a["ticker"], ",".join(reasons))
        else:
            base_filtered.append(a)
    logger.debug("Після базових фільтрів: %d активів.", len(base_filtered))
    return base_filtered


async def apply_rs_and_volume_filtering(
    base_filtered: List[Dict[str, Any]],
    market_data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    idx_trend = market_data.get("index_trend", 1e-9)
    for a in base_filtered:
        a["rel_strength"] = (1 + a["price_change"]) / (1 + idx_trend) - 1

    rs_vals = np.array([a["rel_strength"] for a in base_filtered])
    dyn_min_rs = max(np.nanpercentile(rs_vals, 60), 0.025)
    logger.debug("dynamic MIN_REL_STRENGTH = %.4f", dyn_min_rs)

    stage2 = []
    for a in base_filtered:
        if a["trend"] == "long":
            asset_min_rs = dyn_min_rs * 0.9
        elif a["trend"] == "short":
            asset_min_rs = dyn_min_rs * 1.1
        else:
            asset_min_rs = dyn_min_rs

        cond_rs = a["rel_strength"] >= asset_min_rs
        cond_vol = a["volume_increasing"] or a["volume_anomaly"] >= 1.0

        logger.debug(
            "[Фільтрація активу %s]:\n"
            " • RS: %.2f%% (мін.: %.2f%%)\n"
            " • Обсяг (Z): %.2f\n"
            " • Волатильність: %.2f%%\n"
            " • OpenInterest: %s",
            a["ticker"],
            a["rel_strength"] * 100, asset_min_rs * 100,
            a["volume_anomaly"],
            a["volatility"] * 100,
            utils.format_open_interest(a["open_interest"])
        )

        if cond_rs and cond_vol:
            a["stage2_flag"] = "ok"
        else:
            reasons = []
            if not cond_rs:
                reasons.append("RS")
            if not cond_vol:
                reasons.append("vol_inc")
            a["stage2_flag"] = f"warning({','.join(reasons)})"
            logger.info(
                "[stage2_warning] %s: RS=%.2f%%, vol_inc=%s, Z=%.2f — передано з попередженням.",
                a["ticker"], 100 * a["rel_strength"], a["volume_increasing"], a["volume_anomaly"]
            )
        stage2.append(a)
    logger.debug("Після RS/volume_increasing (зі статусами): %d активів.", len(stage2))
    return stage2


async def annotate_correlation(
    stage2: List[Dict[str, Any]],
    data_fetcher: Any,
    reference_symbol: str = "BTCUSDT",
    period: int = 24
) -> List[Dict[str, Any]]:
    """
    Обчислює кореляцію кожного активу з reference_symbol та додає
    поле "correlation_with_index" до кожного словника активу.
    Активи не відсіюються на цьому етапі.
    """
    # Отримуємо дані для референтного символу
    reference_df = await data_fetcher.get_data(symbol=reference_symbol, interval="1h", limit=period)
    if reference_df.empty:
        logger.error("[annotate_correlation] Не вдалося отримати дані для референтного символу.")
        for a in stage2:
            a["correlation_with_index"] = 0.0
        return stage2

    reference_returns = reference_df["close"].pct_change().dropna()

    for a in stage2:
        ticker = a.get("ticker", "Unknown")
        df = await data_fetcher.get_data(symbol=ticker, interval="1h", limit=period)
        if df.empty:
            a["correlation_with_index"] = 0.0
            logger.debug(f"[annotate_correlation] Не вдалося отримати дані для {ticker}.")
            continue
        asset_returns = df["close"].pct_change().dropna()
        correlation = reference_returns.corr(asset_returns)
        a["correlation_with_index"] = correlation
        logger.debug(f"[annotate_correlation] Кореляція {ticker} з {reference_symbol}: {correlation:.2f}")
    return stage2



async def hierarchical_asset_selection(
    data_fetcher: Any,
    cache_handler: Any,
    thresholds: Dict[str, float] | None = None,
    prefiltered_symbols: List[str] | None = None,
) -> Dict[str, Any]:
    # Перевірка US-сесії
    if not utils.is_us_session(datetime.now()):
        logger.info("[hierarchical_asset_selection] Поза US‑сесією. Вихід.")
        return {"final": [], "base": [], "visual": {}}

    if thresholds is None:
        thresholds = {
            "MIN_AVG_VOLUME": config.MIN_AVG_VOLUME,
            "MIN_VOLATILITY": config.MIN_VOLATILITY,
            "MIN_STRUCTURE_SCORE": config.MIN_STRUCTURE_SCORE,
            "MIN_REL_STRENGTH": config.MIN_REL_STRENGTH,
            "MIN_RELATIVE_VOLATILITY": config.MIN_RELATIVE_VOLATILITY,
            "CORRELATION_THRESHOLD": 0.6,
            "MIN_OPEN_INTEREST": config.MIN_OPEN_INTEREST,
        }

    # Отримання попередньо відфільтрованих символів та кандидатів
    prefiltered_symbols = await prefilter_symbols(data_fetcher, cache_handler, prefiltered_symbols)
    candidates = await get_candidates(data_fetcher, cache_handler, thresholds, prefiltered_symbols)
    if not candidates:
        logger.warning("[hierarchical_asset_selection] Кандидатів немає.")
        return {"final": [], "base": [], "visual": {}}

    market_data = await fetch_market_data(data_fetcher)
    report = await generate_analysis_report(candidates, cache_handler, data_fetcher)
    logger.debug(report)

    adaptive = analyzers.get_adaptive_thresholds(pd.DataFrame(candidates))
    logger.debug("adaptive_thresholds = %s", adaptive)
    new_thr = {
        "MIN_AVG_VOLUME": thresholds["MIN_AVG_VOLUME"],
        "MIN_VOLATILITY": max(adaptive.get("min_volatility", 0.0), 0.003),
        "MIN_STRUCTURE_SCORE": max(adaptive.get("min_structure_score", 0.0), 0.50),
        "MIN_REL_STRENGTH": max(adaptive.get("min_rel_strength", 0.0), 0.05),
        "MIN_RELATIVE_VOLATILITY": adaptive.get("min_relative_volatility", thresholds["MIN_RELATIVE_VOLATILITY"]),
        "CORRELATION_THRESHOLD": thresholds["CORRELATION_THRESHOLD"],
        "MIN_OPEN_INTEREST": thresholds["MIN_OPEN_INTEREST"],
    }
    
    # Можлива корекція порогів в реальному часі
    if datetime.now().minute == 0:
        # adjusted = analyzers.auto_adjust_thresholds(candidates, market_data)
        # new_thr.update(adjusted)
        logger.debug("Updated thresholds via auto_adjust_thresholds: %s", new_thr)

    # Базова фільтрація
    base_filtered = await apply_basic_filtering(candidates, thresholds)
    # Подальший аналіз RS та обсягу
    stage2 = await apply_rs_and_volume_filtering(base_filtered, market_data)
    # Додаємо дані кореляції до активів (без відсіювання)
    stage2 = await annotate_correlation(stage2, data_fetcher, reference_symbol="BTCUSDT", period=24)

    # Розрахунок рейтингової оцінки з використанням RS, обсягу, волатильності, структури, OpenInterest та кореляції
    for a in stage2:
        a["score"] = (
            0.35 * a.get("rel_strength", 0.0) +
            0.2 * a.get("volume_anomaly", 0.0) +
            0.15 * a.get("volatility", 0.0) +
            0.1 * a.get("structure_score", 0.0) +
            0.1 * (a.get("open_interest", 0.0) / (a.get("avg_volume", 1))) +
            0.1 * a.get("correlation_with_index", 0.0)
        )

    # Формування топ-активів за рейтингом
    final_assets = sorted(stage2, key=lambda x: x["score"], reverse=True)[:10]
    logger.info("Після рейтингової пріоритезації повертаємо %d активів.", len(final_assets))

    # Додаткове сортування для візуального відображення за різними метриками
    visual = {
        "by_volume_anomaly": sorted(stage2, key=lambda x: x.get("volume_anomaly", 0.0), reverse=True),
        "by_open_interest": sorted(stage2, key=lambda x: x.get("open_interest", 0.0), reverse=True),
        "by_correlation": sorted(stage2, key=lambda x: x.get("correlation_with_index", 0.0), reverse=True),
        "by_rel_strength": sorted(stage2, key=lambda x: x.get("rel_strength", 0.0), reverse=True),
    }
    display_top_assets(final_assets)

    return {"final": final_assets, "base": base_filtered, "visual": visual}


def good_volume_anomaly(a: dict, min_vola: float, min_oi: float) -> bool:
    """
    Перевірка аномалії обсягу (Z-score) з урахуванням:
      • Якщо Z ≥ 1   → актив приймається
      • Якщо Z знаходиться в межах [-1.5, 1) та волатильність > 0.03 і Open Interest більше порогу,
        актив також приймається (попередження)
      • Якщо |Z| ≥ 2 та актив не “neutral” → актив приймається
    """
    z = a["volume_anomaly"]
    if z >= 1.0:
        return True
    if z >= -1.5 and a["volatility"] > 0.03 and a["open_interest"] > min_oi:
        return True
    return abs(z) >= 2.0 and a["trend"] != "neutral"

def classify_z(z: float) -> str:
    """
    Класифікація аномалії обсягу (Z-score).
    """
    if np.isnan(z):
        return "no_data"
    if z >= 3:
        return "extreme_spike"
    if z >= 2:
        return "spike"
    if z >= 1:
        return "moderate"
    if z <= -2:
        return "very_low"
    return "normal"

def display_top_assets(final_assets: list) -> None:
    """
    Формує звіт для топ-10 активів за рейтингом із виведенням ключових метрик.
    Приклад виводу:
    --------------------------------------------------------
    JASMYUSDT:
       Обсяг: 4.73M USD
       Зміна ціни: -0.0221 {normal}
       Волатильність: 0.7790 {normal}
       Структура: 1.0000 {normal}
    
       Тренд: short
       Відносна сила: n/a
       Зростання обсягу: False
       Аномалія обсягу (Z): -2.75 (very_low)
       OpenInterest: 561.74M {normal}
    --------------------------------------------------------
    """
    print("--------------------------------------------------------")
    for asset in final_assets:
        ticker = asset.get("ticker", "N/A")
        avg_volume_str = asset.get("avg_volume_str", "N/A")
        price_change = asset.get("price_change", 0.0)
        volatility = asset.get("volatility", 0.0)
        structure_score = asset.get("structure_score", 0.0)
        trend = asset.get("trend", "neutral")
        rel_strength = asset.get("rel_strength", 0.0)
        rel_strength_txt = "n/a" if abs(rel_strength) < 1e-4 else f"{rel_strength:.4f}"
        volume_increasing = asset.get("volume_increasing", False)
        volume_anomaly = asset.get("volume_anomaly", float("nan"))
        z_class = classify_z(volume_anomaly)
        # Приклад форматування Open Interest (переводимо у мільйони)
        open_interest = asset.get("open_interest", 0.0)
        open_interest_str = f"{open_interest/1e6:.2f}M"
        
        print(f"{ticker}:")
        print(f"   Обсяг: {avg_volume_str}")
        print(f"   Зміна ціни: {price_change:.4f} ")
        print(f"   Волатильність: {volatility:.4f} ")
        print(f"   Структура: {structure_score:.4f} ")
        print("")
        print(f"   Тренд: {trend}")
        print(f"   Відносна сила: {rel_strength_txt}")
        print(f"   Зростання обсягу: {volume_increasing}")
        print(f"   Аномалія обсягу (Z): {volume_anomaly:.2f} ({z_class})")
        print(f"   Кореляція: {asset.get('correlation_with_index', 0.0):.4f}")
        print(f"   OpenInterest: {open_interest_str} ")
        print("--------------------------------------------------------")


"""
Поточна логіка роботи:
┌──────────────────────────────────────────────────────────────────────────────┐
│                     Hierarchical Asset Selection                             │
├──────────────────────────────────────────────────────────────────────────────┤
│ 1. Prefilter Symbols & Get Candidates                                        │
│    └─ Використання get_prefiltered_symbols для первинного відбору та         │
│       fetch_candidates для отримання базових метрик (обсяг, волатильність,   │
│       структура, зміна ціни, Open Interest тощо).                            │
├──────────────────────────────────────────────────────────────────────────────┤
│ 2. Basic Filtering                                                           │
│    └─ Відсіювання активів з низькою ціною, недостатнім Open Interest,        │
│       невідповідним обсягом, низькою волатильністю або порушенням структурних│
│       критеріїв.                                                             │
├──────────────────────────────────────────────────────────────────────────────┤
│ 3. RS та Обсяг (Volume) Аналіз                                               │
│    └─ Обчислення відносної сили (RS) з урахуванням ринкового тренду та       │
│       перевірка зростання обсягу. Активи отримують статус "ok" або           │
│       "warning" залежно від виконання умов.                                  │
├──────────────────────────────────────────────────────────────────────────────┤
│ 4. Анотація Кореляції                                                        │
│    └─ Обчислення кореляції кожного активу з BTCUSDT (за 1h даними) без       │
│       відсіювання активів. Результат записується у поле                      │
│       "correlation_with_index".                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│ 5. Рейтинговий Розрахунок та Сортування                                      │
│    └─ Розрахунок комбінованої оцінки на основі:                              │
│         • Відносної сили (RS)                                                │
│         • Аномалії обсягу                                                    │
│         • Волатильності                                                      │
│         • Структурного показника                                             │
│         • Open Interest відносно середнього обсягу                           │
│         • Кореляції                                                          │
│       Формування топ-активів та додаткових візуальних списків (сортування    │
│       за обсягом, Open Interest, кореляцією та RS).                          │
└──────────────────────────────────────────────────────────────────────────────┘
│                     ▼                                                        │
│  Результат: повернення словника з "final" (топ активи), "base"               │
│  (активи після базової фільтрації) та "visual" (сортування для візуального)  │
│  моніторингу у реальному часі.                                               │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

final_assets — це список топ-активів, кожен з яких є словником із ключами:

"ticker"

"avg_volume", "avg_volume_str"

"volatility"

"structure_score"

"price_change"

"trend"

"rel_strength" (і, відповідно, форматований рядок)

"volume_increasing"

"volume_anomaly"

"open_interest"

"correlation_with_index"

"""

              