"""
monitor/asset_monitor/monitoring_engine.py

Модуль, що містить функції для циклічного оновлення даних, моніторингу та аналізу активів.
"""
import asyncio
import logging
import time
from io import StringIO
from datetime import datetime
import pandas as pd

from .config import HOURLY_UPDATE_PERIOD, DAILY_UPDATE_PERIOD_INITIAL, DAILY_UPDATE_PERIOD_MAX
from .analyzers import dynamic_threshold
from .event_detector import send_telegram_alert, generate_alert, generate_combined_signal

logger = logging.getLogger("asset_monitor.monitoring_engine")


def log_execution(func):
    """
    Декоратор для логування виконання асинхронних функцій.
    """
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"[{func.__name__}] Початок виконання.")
        result = await func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.debug(f"[{func.__name__}] Завершено за {elapsed:.2f} сек.")
        return result
    return wrapper


@log_execution
async def hourly_update_cycle(symbol: str, data_fetcher: any, period: int = HOURLY_UPDATE_PERIOD) -> None:
    last_df = None
    while True:
        logger.debug(f"[HOURLY] Оновлення даних для {symbol}.")
        df = await data_fetcher.get_data(symbol=symbol, interval='1h', limit=24)

        if df is not None:
            logger.debug(f"[HOURLY] Отримано {len(df)} записів для {symbol}.")
            new_last_ts = df["timestamp"].max()
            if last_df is not None and last_df["timestamp"].max() == new_last_ts:
                logger.debug(f"[HOURLY] Дані не змінилися для {symbol}.")
            else:
                last_df = df.copy()
        else:
            logger.error(f"[HOURLY] Не вдалося отримати дані для {symbol}.")

        await asyncio.sleep(period)


@log_execution
async def daily_update_cycle(symbol: str, data_fetcher: any, period: int = DAILY_UPDATE_PERIOD_INITIAL, full_limit: int = 60, incremental_limit: int = 2) -> None:
    while True:
        logger.debug(f"[DAILY] Початок циклу оновлення для {symbol}.")
        df = await data_fetcher.get_data(symbol=symbol, interval='1d', limit=full_limit)
        
        if df is not None and not df.empty:
            logger.debug(f"[DAILY] Отримано {len(df)} записів для {symbol}.")
        else:
            logger.error(f"[DAILY] Не отримано даних для {symbol}.")

        period = max(100, period // 2) if df is not None and not df.empty else min(period * 2, DAILY_UPDATE_PERIOD_MAX)
        logger.debug(f"[DAILY] Новий період: {period} сек.")
        await asyncio.sleep(period)



async def fetch_with_retry(data_fetcher: any, symbol: str, interval: str, limit: int, retries: int = 3, delay: int = 5) -> 'pd.DataFrame':
    """
    Логіка повторних спроб для отримання даних з API.
    """
    for attempt in range(1, retries + 1):
        new_df = await data_fetcher.get_data(symbol=symbol, interval=interval, limit=limit)

        if new_df is not None and not new_df.empty:
            logger.debug(f"[fetch_with_retry][{symbol}] Дані отримано на спробі {attempt}.")
            return new_df
        logger.warning(f"[fetch_with_retry][{symbol}] Спроба {attempt} не вдалася, повтор через {delay} сек.")
        await asyncio.sleep(delay)
    logger.error(f"[fetch_with_retry][{symbol}] Всі {retries} спроби невдалі.")
    return None


@log_execution
async def monitor_and_analyze(symbol: str, cache_handler: any) -> None:
    df_daily = await cache_handler.fetch_from_cache(f"{symbol}:1d")
    
    if df_daily is None or len(df_daily) < 2:
        logger.error(f"[monitor_and_analyze][{symbol}] Недостатньо даних.")
        await asyncio.sleep(3600)
        return

    last_close = df_daily.iloc[-1]["close"]
    prev_close = df_daily.iloc[-2]["close"]
    price_change_pct = (last_close - prev_close) / prev_close * 100
    logger.debug(f"[monitor_and_analyze][{symbol}] Зміна ціни: {price_change_pct:.2f}%")

    window_classic = min(180, len(df_daily))
    support_classic = df_daily["low"].tail(window_classic).min()
    resistance_classic = df_daily["high"].tail(window_classic).max()

    from .analyzers import (
        dynamic_threshold, find_local_levels, determine_nearest_levels,
        normalize_asset_levels
    )

    forecast_period = dynamic_threshold(df_daily)

    local_levels = find_local_levels(df_daily["close"].values)
    nearest_levels = determine_nearest_levels(last_close, local_levels, df_daily["close"].values)
    adaptive_support, adaptive_resistance = normalize_asset_levels(
        df_daily,
        nearest_levels.get("support", support_classic),
        nearest_levels.get("resistance", resistance_classic)
    )

    combined_signal = generate_combined_signal(df_daily, symbol, last_close)
    await send_telegram_alert(combined_signal.get("alert", "N/A"))
    logger.info(f"[monitor_and_analyze][{symbol}] Сигнал: {combined_signal.get('alert', 'N/A')}")

    alert = None
    if abs(price_change_pct) > 2 or last_close < support_classic or last_close > resistance_classic:
        level_for_alert = adaptive_support if last_close < support_classic else adaptive_resistance
        level_type = "Підтримка" if last_close < support_classic else "Опір"
        alert = generate_alert(symbol, last_close, level_for_alert, level_type, approach=False, forecast_period=str(forecast_period))
        logger.warning(f"[monitor_and_analyze][{symbol}] Тригер: {alert}")
    else:
        distance_to_support = abs(last_close - adaptive_support) / adaptive_support * 100
        distance_to_resistance = abs(adaptive_resistance - last_close) / adaptive_resistance * 100
        threshold = dynamic_threshold(df_daily)
        if distance_to_support <= threshold:
            alert = generate_alert(symbol, last_close, adaptive_support, "Підтримка", True, forecast_period=str(forecast_period))
        elif distance_to_resistance <= threshold:
            alert = generate_alert(symbol, last_close, adaptive_resistance, "Опір", True, forecast_period=str(forecast_period))
            
    if alert:
        await send_telegram_alert(alert)

    await asyncio.sleep(3600)



@log_execution
async def monitor_single_asset(symbol: str, data_fetcher: any, cache_handler: any, file_manager: any) -> None:
    """
    Моніторинг одного активу для тестування нових алгоритмів.
    """
    await asyncio.gather(
        daily_update_cycle(symbol, data_fetcher, cache_handler, file_manager, period=100),
        monitor_and_analyze(symbol, data_fetcher, cache_handler, file_manager, limit=60)
    )


@log_execution
async def monitor_multiple_assets(symbols: list, data_fetcher: any, cache_handler: any, file_manager: any) -> None:
    """
    Паралельний моніторинг декількох активів.
    """
    tasks = []
    for symbol in symbols:
        tasks.append(monitor_single_asset(symbol, data_fetcher, cache_handler, file_manager))
    await asyncio.gather(*tasks)
