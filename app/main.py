"""
Основний модуль запуску системи
app/main.py
"""

import asyncio
import aiohttp
import logging
import sys
import os
from asyncpg import create_pool
from dotenv import load_dotenv

# Імпорти системних компонентів
from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher

# Імпорти модулів відбору та моніторингу
from monitor.asset_monitoring import AssetMonitor, monitoring_loop
from monitor.asset_selector.asset_selector import hierarchical_asset_selection
from monitor.asset_monitor.asset_monitor import monitor_selected_assets
from monitor.asset_selector.optimized_asset_filter import get_prefiltered_symbols

from monitor.asset_selector.config import (
    MIN_AVG_VOLUME, MIN_VOLATILITY, MIN_STRUCTURE_SCORE,
    MIN_REL_STRENGTH, MIN_RELATIVE_VOLATILITY, CORRELATION_THRESHOLD, MIN_OPEN_INTEREST
)

# Імпорт налаштувань (з файлу settings.py, що знаходиться в корені проекту)
from app.settings import settings

# Завантаження змінних середовища (для локального запуску)
load_dotenv()

# Імпорт конфігурації логування з logging_config.py
import logging_config  # цей модуль встановлює logging.config.dictConfig(LOG_CONFIG)

# Отримуємо логгер для модуля "main"
logger = logging.getLogger("main")

def validate_settings() -> None:
    """
    Перевіряємо або REDIS_URL, або пару host/port.
    """
    missing = []
    if not os.getenv("REDIS_URL"):
        if not settings.redis_host:
            missing.append("REDIS_HOST")
        if not settings.redis_port:
            missing.append("REDIS_PORT")

    if not settings.binance_api_key:
        missing.append("BINANCE_API_KEY")
    if not settings.binance_secret_key:
        missing.append("BINANCE_SECRET_KEY")

    if missing:
        raise ValueError(f"Відсутні налаштування: {', '.join(missing)}")
    logger.info("Налаштування перевірено — OK.")



async def init_db():
    """
    Ініціалізує підключення до бази даних через asyncpg.
    Повертає пул підключень або None (у тестовому режимі).
    """
    try:
        pool = await create_pool(
            dsn=os.getenv("DATABASE_URL"),  # або settings.database_url, якщо додати в settings.py
            min_size=1,
            max_size=20,
            timeout=60,
            command_timeout=60,
        )
        logger.info("Пул підключень до бази даних створено успішно.")
        return pool
    except Exception as e:
        # Додаємо exc_info=True для детального traceback
        logger.error(f"Помилка підключення до бази даних: {e}", exc_info=True)
        # У тестовому режимі повертаємо None і продовжуємо роботу.
        return None


# ────────────────────────── init_system ──────────────────────────
async def init_system():
    validate_settings()

    # 1️⃣  Пробуємо Heroku‑стиль REDIS_URL
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        cache_handler = SimpleCacheHandler.from_url(redis_url)
        logger.info("Підключились до Redis через URI.")
    else:
        # 2️⃣  Фолбек на локальні змінні
        cache_handler = SimpleCacheHandler(
            host=settings.redis_host,
            port=settings.redis_port,
        )
        logger.info("Підключились до Redis через host/port.")

    db_pool = await init_db()
    if db_pool is None:
        logger.warning("База даних не підключена — тестовий режим.")

    return cache_handler, db_pool

async def run_pipeline() -> None:
    cache_handler, _ = await init_system()

    async with aiohttp.ClientSession() as session:
        # ---- 1. DataFetcher -------------------------------------------------
        data_fetcher = OptimizedDataFetcher(
            cache_handler=cache_handler,
            session=session,
            binance_api_url="https://api.binance.com"
        )

        # ---- 2. Швидкий pre‑filter  (30‑40 символів) ------------------------
        fast_symbols = await get_prefiltered_symbols(
            session=session,
            cache_handler=cache_handler,
            thresholds={
                "MIN_QUOTE_VOLUME": 1_000_000.0,
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

        # ---- 3. Hierarchical selection  -------------------------------------
        thresholds = {
            "MIN_AVG_VOLUME": MIN_AVG_VOLUME,
            "MIN_VOLATILITY": MIN_VOLATILITY,
            "MIN_STRUCTURE_SCORE": MIN_STRUCTURE_SCORE,
            "MIN_REL_STRENGTH": MIN_REL_STRENGTH,
            "MIN_RELATIVE_VOLATILITY": MIN_RELATIVE_VOLATILITY,
            "CORRELATION_THRESHOLD": CORRELATION_THRESHOLD,
            "MIN_OPEN_INTEREST": MIN_OPEN_INTEREST
        }

        selected_assets = await hierarchical_asset_selection(
            data_fetcher,
            cache_handler,
            thresholds,
            prefiltered_symbols=fast_symbols          # ← уникаємо дублювання
        )

        if not selected_assets:
            logger.warning("Жоден актив не пройшов hierarchical selection.")
            return
        logger.info("Фінально відібрано %d активів.", len(selected_assets))

        # ---- 4. Моніторинг  --------------------------------------------------
        monitor = AssetMonitor(volume_z_threshold=2.5, rsi_overbought=70, rsi_oversold=30)
        logger.info("Запускаємо моніторинг…")
        await monitoring_loop(selected_assets, data_fetcher, monitor, interval_sec=300)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)