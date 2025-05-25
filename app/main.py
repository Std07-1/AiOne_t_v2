"""
Основний модуль запуску системи AiOne_t
app/main.py
"""
import time
import json
import time
import asyncio
import logging
import os
import sys
from pathlib import Path
from dataclasses import asdict

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ─────────────────────────── Імпорти бізнес-логіки ───────────────────────────
from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher
from data.file_manager import FileManager
from stage1.asset_monitoring import AssetMonitorStage1, screening_producer
from data.ws_worker import WSWorker
from data.ram_buffer import RAMBuffer

from UI.ui_consumer import UI_Consumer
from stage1.optimized_asset_filter import get_prefiltered_symbols

from app.settings import settings
from rich.console import Console
from rich.logging import RichHandler

# Завантажуємо налаштування з .env
load_dotenv()

# --- Логування ---
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.INFO)
main_logger.handlers.clear()
main_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
main_logger.propagate = False   # ← Критично важливо!

def debug_ram_buffer(buffer, symbols, tf="1m"):
    import time
    now = int(time.time() * 1000)
    for sym in symbols:
        bars = buffer.get(sym, tf, 3)
        if bars:
            last_ts = bars[-1]['timestamp']
            print(f"[{sym}] Last bar: {last_ts} | Age: {(now - last_ts) // 1000}s")
        else:
            print(f"[{sym}] No bars in RAMBuffer")

# Створюємо FastAPI-додаток
app = FastAPI()

# Шлях до кореня проекту
BASE_DIR = Path(__file__).resolve().parent.parent
# Каталог зі статичними файлами (фронтенд WebApp)
STATIC_DIR = BASE_DIR / "static"

# 1) Роздача статичних ресурсів під /static
app.mount(
    "/static",
    StaticFiles(directory=str(STATIC_DIR)),
    name="static",
)

# 2) GET / → повертає index.html
@app.get("/", include_in_schema=False)
async def serve_index():
    """
    Віддає головну сторінку WebApp (index.html).
    """
    index_path = STATIC_DIR / "index.html"
    return FileResponse(str(index_path))


# Глобальний Redis-кеш, ініціалізується при старті
cache_handler: SimpleCacheHandler

@app.on_event("startup")
async def on_startup():
    """
    Подія запуску FastAPI:
      - Підключається до Redis (через URL або host/port)
      - Запускає фоновий таск для pipeline
    """
    global cache_handler

    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        cache_handler = SimpleCacheHandler.from_url(redis_url)
        main_logger.info("🔗 Підключено до Redis via URL: %s", redis_url)
    else:
        cache_handler = SimpleCacheHandler(
            host=settings.redis_host,
            port=settings.redis_port,
        )
        main_logger.info(
            "🔗 Підключено до Redis via host/port: %s:%s",
            settings.redis_host,
            settings.redis_port,
        )

    # Запускаємо головний конвеєр обробки в фоновому таску
    asyncio.create_task(run_pipeline())


@app.get("/api/data")
async def api_data():
    """
    Ендпоінт для фронтенду: повертає останні дані asset_stats.
    Читає Redis-ключ "asset_stats:global".
    """
    raw = await cache_handler.fetch_from_cache("asset_stats", "global")
    if not raw:
        # якщо даних ще немає — повертаємо пустий список
        return JSONResponse({"timestamp": int(time.time()), "assets": []})

    top10 = json.loads(raw.decode("utf-8"))
    return JSONResponse({"timestamp": int(time.time()), "assets": top10})


def validate_settings() -> None:
    """
    Перевіряємо необхідні змінні оточення:
      - REDIS_URL або (REDIS_HOST + REDIS_PORT)
      - BINANCE_API_KEY, BINANCE_SECRET_KEY
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
        
    main_logger.info("Налаштування перевірено — OK.")


async def init_system() -> SimpleCacheHandler:
    """
    Ініціалізація зовнішніх систем:
      - Валідація налаштувань
      - Підключення до Redis
    Повертає: інстанс SimpleCacheHandler
    """
    validate_settings()

    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        handler = SimpleCacheHandler.from_url(redis_url)
        #logger.debug("Redis через URL: %s", redis_url)
    else:
        handler = SimpleCacheHandler(
            host=settings.redis_host,
            port=settings.redis_port,
        )
        #logger.debug(
        #    "Redis через host/port: %s:%s",
        #    settings.redis_host,
        #    settings.redis_port,
        #)
    return handler


async def run_pipeline() -> None:
    """
    Основний pipeline:
    1. Ініціалізація системи та кешу
    2. Pre-filter активів
    3. Запуск ws_worker + RAMBuffer для 1m даних + healthcheck
    4. Запуск скринінгу (screening_producer) для stage1
    5. (Опційно) запуск UI/live-stats/fastapi
    """
    # 1. Ініціалізація
    cache = await init_system()
    file_manager = FileManager()
    buffer = RAMBuffer(max_bars=120)

    # 2. Pre-filter: отримуємо список найліквідніших активів для WS
    async with aiohttp.ClientSession() as session:
        fast_symbols = await get_prefiltered_symbols(
            session=session,
            cache_handler=cache,
            thresholds={
                "MIN_QUOTE_VOLUME": 1_000_000.0,
                "MIN_PRICE_CHANGE": 3.0,
                "MIN_OPEN_INTEREST": 500_000.0,
                "MAX_SYMBOLS": 350,   
            },
            dynamic=False,
        )
        if not fast_symbols:
            main_logger.warning("❌ Не знайдено жодного активу після pre-filter.")
            return
        main_logger.info("Після pre-filter: %d символів", len(fast_symbols))
    
        # 2.1. Записуємо тільки через set_fast_symbols/get_fast_symbols
        fast_symbols = [s.lower() for s in fast_symbols]

        await cache.set_fast_symbols(fast_symbols, ttl=600)
        actual_symbols = await cache.get_fast_symbols()
        main_logger.info("WS selector записано у Redis(%d symbols): %s", len(actual_symbols), list(actual_symbols)[:5])    #: %s", actual_symbols)
        if set(s.lower() for s in fast_symbols) != set(s.lower() for s in actual_symbols):
            main_logger.warning("❗️Розбіжність між fast_symbols і тим, що записано у Redis!")

        # ─── Preload історичних 1m‐барів для швидкого старту Stage1 ───
        lookback = 50
        main_logger.debug(
            "Починаємо preload історії: завантажуємо %d 1m-барів для %d символів…",
            lookback, len(fast_symbols)
        )
        # Створюємо fetcher для пакетного завантаження
        fetcher = OptimizedDataFetcher(cache_handler=cache, session=session)

        # … попередній preload 1m …
        hist_data = await fetcher.get_data_batch(
            fast_symbols,
            interval="1m",
            limit=lookback,
            min_candles=lookback,
            show_progress=True,
            read_cache=False,    # вимикаємо кеш при preload
            write_cache=True     # опціонально: одразу оновимо кеш свіжими даними
        )

        # ─── Preload денного таймфрейму для глобальних рівнів ───
        days = 30
        main_logger.debug("Preload Daily: завантажуємо %d денних свічок…", days)
        daily_data = await fetcher.get_data_batch(
            fast_symbols,
            interval="1d",
            limit=days,
            min_candles=days,
            show_progress=False,
            read_cache=False,
            write_cache=False
        )
        # Передамо в монітор глобальні рівні

        # Наповнюємо RAMBuffer отриманими свічками
        for sym, df in hist_data.items():
            for bar in df.to_dict("records"):
                # Конвертуємо pandas.Timestamp → ms
                ts = bar["timestamp"]
                if isinstance(ts, pd.Timestamp):
                    bar["timestamp"] = int(ts.value // 1_000_000)
                else:
                    bar["timestamp"] = int(ts)
                buffer.add(sym.lower(), "1m", bar)
        main_logger.debug(
            "Preload завершено: історія додана в RAMBuffer для %d символів",
            len(hist_data)
        )

    # 3. Запуск WSWorker та RAMBuffer
    ws_worker = WSWorker(
        symbols=fast_symbols,
        ram_buffer=buffer,
        redis_cache=cache,
    )

    # 3.1. HealthCheck — окремий таск (опціонально, але strongly recommended!)
    async def ram_buffer_healthcheck(
        buffer,
        symbols,
        max_age=90,
        interval=30,
        ws_worker=None,
        tf="1m"
    ):
        """
        Моніторинг живучості даних у RAMBuffer.
        Якщо дані по символу не оновлювались >max_age сек — вважається "мертвим".
        При знаходженні таких символів логуються WARN, і опційно перезапускається WSWorker.
        """
        while True:
            now = int(time.time() * 1000)
            dead = []
            for sym in symbols:
                bars = buffer.get(sym, tf, 1)
                if not bars or (now - bars[-1]["timestamp"]) > max_age * 1000:
                    dead.append(sym)
            if dead:
                main_logger .debug("[HealthCheck] Symbols stalled: %s", dead)
                if ws_worker is not None:
                    main_logger .debug("[HealthCheck] Restarting WSWorker due to stalled symbols")
                    await ws_worker.stop()
                    asyncio.create_task(ws_worker.consume())
            else:
                main_logger .debug("[HealthCheck] All symbols are alive (checked %d symbols).", len(symbols))
            await asyncio.sleep(interval)

    ws_task = asyncio.create_task(ws_worker.consume())
    health_task = asyncio.create_task(ram_buffer_healthcheck(buffer, fast_symbols, ws_worker=ws_worker))

    # Прогрів RAMBuffer (очікуємо перші бари)
    await asyncio.sleep(5)
    #debug_ram_buffer(buffer, fast_symbols)

    # 4. AssetMonitor + screening_producer
    monitor = AssetMonitorStage1(cache_handler=cache)

    monitor.set_global_levels(daily_data)
    main_logger .debug("Глобальні рівні для %d символів встановлено", len(daily_data))

    queue = asyncio.Queue(maxsize=1)
    ui = UI_Consumer(vol_z_threshold=2.5)

    prod = asyncio.create_task(
        screening_producer(
            monitor,
            buffer,
            cache,            # додаємо Redis‑кеш для динаміки
            fast_symbols,     # початковий список
            queue,
            timeframe="1m",
            lookback=50,
            interval_sec=30
        )
    )
    cons = asyncio.create_task(
        ui.ui_consumer(queue, refresh_rate=1.0, loading_delay=5.0, smooth_delay=0.1)
    )

    # Очікуємо завершення тасків (Ctrl+C — завершить все)
    await asyncio.gather(ws_task, health_task, prod, cons)

if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        main_logger .error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)
