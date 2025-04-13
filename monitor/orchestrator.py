"""
Модуль orchestrator.py

Оркеструє виконання двох основних процесів:
  1. Щоденний скринінг активів із використанням hierarchical_asset_selection.
  2. Моніторинг активів із різними інтервалами для сильного пулу (final) та watchlist.

Цей модуль викликається з основного модуля системи.
"""

import asyncio
import sys
import aiohttp
from datetime import datetime

# Імпортуємо необхідні функції та класи із інших модулів
from asset_selector import hierarchical_asset_selection
from fast_prefilter import get_prefiltered_symbols  # припустимо, цей модуль відповідає за швидкий pre‑filter
from monitoring import monitoring_loop, AssetMonitor
from system_init import init_system  # функція ініціалізації кешу, логування тощо
from config import MIN_AVG_VOLUME, MIN_VOLATILITY, MIN_STRUCTURE_SCORE, MIN_REL_STRENGTH, MIN_RELATIVE_VOLATILITY, CORRELATION_THRESHOLD, MIN_OPEN_INTEREST
import logging

logger = logging.getLogger("orchestrator")
logger.setLevel(logging.DEBUG)


# Глобальні змінні для збереження пулу активів
CURRENT_FINAL = []
CURRENT_WATCHLIST = []


async def asset_selection_loop(data_fetcher, cache_handler):
    """
    Виконується один раз на добу.
    Запускає процес hierarchical_asset_selection із заданими порогами та pre‑filter.
    Результатом є два списки: final (сильні активи) та watchlist (активи із попередженням).
    """
    global CURRENT_FINAL, CURRENT_WATCHLIST

    # ---- Швидкий pre‑filter ------------------------------------------------
    fast_symbols = await get_prefiltered_symbols(
        session=data_fetcher.session,
        cache_handler=cache_handler,
        thresholds={
            "MIN_QUOTE_VOLUME": 200_000.0,
            "MIN_PRICE_CHANGE": 3.0,
            "MIN_OPEN_INTEREST": 500_000.0,
            "MAX_SYMBOLS": 120,
        },
        dynamic=False
    )
    if not fast_symbols:
        logger.warning("Не знайдено жодного активу після швидкого фільтра.")
        return

    logger.info("Після швидкого фільтра: %d символів.", len(fast_symbols))

    # ---- Hierarchical selection --------------------------------------------
    thresholds = {
        "MIN_AVG_VOLUME": MIN_AVG_VOLUME,
        "MIN_VOLATILITY": MIN_VOLATILITY,
        "MIN_STRUCTURE_SCORE": MIN_STRUCTURE_SCORE,
        "MIN_REL_STRENGTH": MIN_REL_STRENGTH,
        "MIN_RELATIVE_VOLATILITY": MIN_RELATIVE_VOLATILITY,
        "CORRELATION_THRESHOLD": CORRELATION_THRESHOLD,
        "MIN_OPEN_INTEREST": MIN_OPEN_INTEREST,
    }

    # Припускаємо, що hierarchical_asset_selection повертає кортеж: (final_assets, watchlist_assets)
    result = await hierarchical_asset_selection(
        data_fetcher,
        cache_handler,
        thresholds,
        prefiltered_symbols=fast_symbols
    )
    if not result:
        logger.warning("Жоден актив не пройшов hierarchical selection.")
        return

    # Розпаковуємо результати
    CURRENT_FINAL, CURRENT_WATCHLIST = result
    logger.info("Сканування завершено. Final: %d активів, Watchlist: %d активів.",
                len(CURRENT_FINAL), len(CURRENT_WATCHLIST))


async def monitoring_loop_wrapper(data_fetcher, monitor, interval_final_sec, interval_watchlist_sec):
    """
    Запускає два окремих цикли моніторингу з різними інтервалами:
      - Для фінального пулу (final_assets).
      - Для watchlist.
    """
    while True:
        if CURRENT_FINAL:
            logger.info("%s - Моніторинг фінального пулу активів: %s",
                        datetime.now(), [a["ticker"] for a in CURRENT_FINAL])
            # Моніторинг для final assets (наприклад, з коротшим інтервалом)
            await monitoring_loop(CURRENT_FINAL, data_fetcher, monitor, interval_sec=interval_final_sec)
        else:
            logger.info("%s - Фінальний пул активів порожній.", datetime.now())

        if CURRENT_WATCHLIST:
            logger.info("%s - Моніторинг watchlist активів: %s",
                        datetime.now(), [a["ticker"] for a in CURRENT_WATCHLIST])
            # Моніторинг для watchlist (наприклад, з довшим інтервалом)
            await monitoring_loop(CURRENT_WATCHLIST, data_fetcher, monitor, interval_sec=interval_watchlist_sec)
        else:
            logger.info("%s - Watchlist активів порожній.", datetime.now())

        # Очікуємо, перед наступним циклом моніторингу
        await asyncio.sleep(300)  # або встановіть загальний інтервал оновлення


async def run_pipeline() -> None:
    """
    Головна функція оркестрації, що виконується з основного модуля системи.
    Вона ініціалізує систему, виконує відбір активів і запускає моніторинг.
    """
    cache_handler, _ = await init_system()

    async with aiohttp.ClientSession() as session:
        # ---- 1. DataFetcher -------------------------------------------------
        from data.raw_data import OptimizedDataFetcher  # імпорт у потрібному місці
        data_fetcher = OptimizedDataFetcher(
            cache_handler=cache_handler,
            session=session,
            binance_api_url="https://api.binance.com"
        )

        # ---- 2. Виконання щоденного скринінгу активів ----------------------
        # Наприклад, запускаємо скринінг один раз на добу
        await asset_selection_loop(data_fetcher, cache_handler)

        # ---- 3. Моніторинг активів -------------------------------------------
        monitor = AssetMonitor(volume_z_threshold=2.5, rsi_overbought=70, rsi_oversold=30)
        logger.info("Запускаємо моніторинг активів…")
        # Запускаємо моніторинг із різними інтервалами:
        # Наприклад, final_assets – кожні 5 хвилин, watchlist – кожні 15 хвилин.
        await monitoring_loop_wrapper(data_fetcher, monitor, interval_final_sec=300, interval_watchlist_sec=900)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)

