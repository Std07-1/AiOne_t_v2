"""
asset_monitor.py

Основний модуль моніторингу активів.
Отримує список активів для моніторингу та запускає паралельне спостереження.
Якщо актив демонструє потенційний рух, його аналіз переходить на наступний етап 
(підтвердження, моніторинг та генерація сигналів).
Автор: [Std07.1]
Дата: [16.03.25]
"""

import asyncio
import logging
from typing import List, Dict, Any

from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher
from datafetcher.file_manager import FileManager
from .monitoring_engine import monitor_multiple_assets

logger = logging.getLogger("asset_monitor.asset_monitor")


async def monitor_selected_assets(selected_assets: List[Dict[str, Any]],
                                  cache_handler: SimpleCacheHandler,
                                  file_manager: FileManager) -> None:
    """
    Отримує список відібраних активів та запускає їх моніторинг.
    
    Кожен актив перевіряється циклічно: дані оновлюються, проводиться аналіз (порівняння
    поточної ціни з ключовими рівнями, аналіз тренду, обсягу) і, у разі виявлення потенційного руху,
    генерується сигнал (наприклад, BUY або SELL) для подальшого підтвердження.
    
    Args:
        selected_assets (List[Dict[str, Any]]): Список активів з ключем "ticker" та розрахованими показниками.
        cache_handler (CacheHandler): Об'єкт для роботи з Redis.
        file_manager (FileManager): Об'єкт для збереження даних у файловій системі.
    """
    if not selected_assets:
        logger.info("Список активів для моніторингу порожній.")
        return

    # Формуємо список тикерів з отриманих активів
    tickers = [asset.get("ticker", "N/A") for asset in selected_assets]
    logger.info(f"Запуск моніторингу для активів: {tickers}")

    # Ініціалізуємо DataFetcher для отримання даних моніторингу (можна використовувати окрему інстанцію)
    data_fetcher = OptimizedDataFetcher(cache_handler=cache_handler )
    # Запускаємо паралельний моніторинг для всіх активів
    await monitor_multiple_assets(tickers, data_fetcher, cache_handler, file_manager)
