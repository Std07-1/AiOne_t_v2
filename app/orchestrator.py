"""
app/orchestrator.py

Модуль оркестрації, який дозволяє спостерігати роботу системи для одного або декількох активів,
замість відбору всіх активів одразу. Цей модуль ініціалізує системні компоненти та запускає
циклічні завдання для кожного активу (годинне оновлення, денне оновлення, моніторинг та аналіз).

Автор: [Std07.1]
Дата: [16.03.25]
"""

import asyncio
from app.settings import Settings
from app.main import init_system  # init_system повертає (cache_handler, db_pool)
from monitor.asset_monitor.monitoring_engine import (
    hourly_update_cycle,
    daily_update_cycle,
    monitor_and_analyze,
    monitor_multiple_assets  # Альтернативний варіант моніторингу для кількох активів
)
from datafetcher.file_manager import FileManager
from datafetcher.raw_data import DataFetcher

settings = Settings()

async def orchestrate(symbols: list):
    """
    Функція оркестрації:
      - Ініціалізує системні компоненти (CacheHandler, базу даних, FileManager, DataFetcher).
      - Для кожного символу планує циклічні завдання:
          • годинне оновлення даних,
          • денне оновлення даних,
          • моніторинг та аналіз.
    
    Аргументи:
        symbols (list): Список активів (тикерів), для яких потрібно запускати моніторинг.
    """
    # Ініціалізація системних компонентів
    cache_handler, db_pool = await init_system()
    if cache_handler is None:
        raise Exception("CacheHandler не ініціалізовано!")
    
    # Ініціалізуємо FileManager та DataFetcher
    file_manager = FileManager(redis_client=None)
    data_fetcher = DataFetcher(cache_handler=cache_handler, file_manager=file_manager)
    
    tasks = []
    for symbol in symbols:
        tasks.extend([
            hourly_update_cycle(symbol, data_fetcher, cache_handler, file_manager, period=600),
            daily_update_cycle(symbol, data_fetcher, cache_handler, file_manager),
            monitor_and_analyze(symbol, data_fetcher, cache_handler, file_manager, limit=60)
        ])
    await asyncio.gather(*tasks)

# Альтернативно, якщо хочете запустити моніторинг декількох активів одним завданням:
async def orchestrate_multiple(symbols: list):
    cache_handler, db_pool = await init_system()
    if cache_handler is None:
        raise Exception("CacheHandler не ініціалізовано!")
    file_manager = FileManager(redis_client=None)
    # Для моніторингу декількох активів використовуємо функцію monitor_multiple_assets
    await monitor_multiple_assets(symbols, DataFetcher(cache_handler=cache_handler, file_manager=file_manager), cache_handler, file_manager)

if __name__ == "__main__":
    # Вкажіть тут активи вручну, для яких потрібно запустити моніторинг
    symbols = ['KAVAUSDT', 'NOTUSDT', 'ZROUSDT', 'DOGEUSDT', 'OMNIUSDT', 'DFUSDT', 'PHAUSDT', 'WUSDT', 'BLURUSDT']
    # Викликаємо orchestrate(), який запускає моніторинг за ініціалізованими компонентами
    asyncio.run(orchestrate(symbols))
