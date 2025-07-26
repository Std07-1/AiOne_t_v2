"""
Основний модуль запуску системи AiOne_t
app/main.py
"""

import time
import time
import asyncio
import logging
import os
import sys
from pathlib import Path
from dataclasses import asdict

from redis.asyncio import Redis
import subprocess
import aiohttp
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter

# ─────────────────────────── Імпорти бізнес-логіки ───────────────────────────
from data.cache_handler import SimpleCacheHandler
from data.raw_data import OptimizedDataFetcher
from data.file_manager import FileManager
from data.ws_worker import WSWorker
from data.ram_buffer import RAMBuffer

from stage1.asset_monitoring import AssetMonitorStage1
from app.screening_producer import screening_producer, publish_full_state
from UI.ui_consumer import UI_Consumer
from stage1.optimized_asset_filter import get_filtered_assets
from stage3.trade_manager import TradeLifecycleManager
from stage2.calibration_engine import CalibrationEngine
from app.settings import settings
from rich.console import Console
from rich.logging import RichHandler
from .utils.metrics import MetricsCollector
from stage2.calibration_queue import CalibrationQueue
from app.screening_producer import AssetStateManager
from stage2.config import STAGE2_CONFIG
from stage2.calibration.calibration_config import CalibrationConfig

# Завантажуємо налаштування з .env
load_dotenv()

# --- Логування ---
main_logger = logging.getLogger("main")
main_logger.setLevel(logging.INFO)
main_logger.handlers.clear()
main_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
main_logger.propagate = False  # ← Критично важливо!


# Створюємо FastAPI-додаток
app = FastAPI()
router = APIRouter()

# Глобальний Redis-кеш, ініціалізується при старті
cache_handler: SimpleCacheHandler

# Додаємо оголошення глобальної змінної calib_queue
global calib_queue

# Шлях до кореня проекту
BASE_DIR = Path(__file__).resolve().parent.parent
# Каталог зі статичними файлами (фронтенд WebApp)
STATIC_DIR = BASE_DIR / "static"


def launch_ui_consumer():
    """
    Запуск UI/ui_consumer_entry.py у новому терміналі (Windows).
    """
    proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if sys.platform.startswith("win"):
        subprocess.Popen(
            ["start", "cmd", "/k", "python", "-m", "UI.ui_consumer_entry"],
            shell=True,
            cwd=proj_root,  # запуск з кореня проекту, щоб UI бачився як модуль
        )
    else:
        subprocess.Popen(
            ["gnome-terminal", "--", "python3", "-m", "UI.ui_consumer_entry"],
            cwd=proj_root,
        )


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
        # logger.debug("Redis через URL: %s", redis_url)
    else:
        handler = SimpleCacheHandler(
            host=settings.redis_host,
            port=settings.redis_port,
        )
        # logger.debug(
        #    "Redis через host/port: %s:%s",
        #    settings.redis_host,
        #    settings.redis_port,
        # )
    return handler


# --- Дебаг-функція для RAMBuffer ---
def debug_ram_buffer(buffer, symbols, tf="1m"):
    """
    Дебажить свіжість барів у RAMBuffer для заданих символів.
    """
    now = int(time.time() * 1000)
    for sym in symbols:
        bars = buffer.get(sym, tf, 3)
        if bars:
            last_ts = bars[-1]["timestamp"]
            print(f"[{sym}] Last bar: {last_ts} | Age: {(now - last_ts) // 1000}s")
        else:
            print(f"[{sym}] No bars in RAMBuffer")


# --- Фоновий таск: періодичне оновлення fast_symbols через prefilter ---
async def periodic_prefilter_and_update(
    cache, session, thresholds, interval=600, buffer=None, fetcher=None, lookback=500
):
    """
    Періодично виконує prefilter та оновлює fast_symbols у Redis.
    Додає preload історії для нових активів.
    """
    # Початковий набір символів
    initial_symbols = set(await cache.get_fast_symbols())
    prev_symbols = initial_symbols.copy()

    # Затримка перед першим оновленням (щоб уникнути конфлікту з первинним префільтром)
    await asyncio.sleep(interval)  # Чекаємо звичайний інтервал (600 сек)
    while True:
        try:
            main_logger.info("🔄 Оновлення списку fast_symbols через prefilter...")
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=cache,
                thresholds=thresholds,
                dynamic=True,
            )

            if fast_symbols:
                fast_symbols = [s.lower() for s in fast_symbols]
                current_symbols = set(fast_symbols)
                await cache.set_fast_symbols(
                    fast_symbols, ttl=interval * 2
                )  # TTL 1200 сек

                main_logger.info(
                    "Prefilter: %d символів записано у Redis: %s",
                    len(fast_symbols),
                    fast_symbols[:5],
                )

                # --- preload для нових активів ---
                if buffer is not None and fetcher is not None:
                    # Знаходимо ТІЛЬКИ нові символи
                    new_symbols = current_symbols - prev_symbols

                    # Додаємо debug-лог для відстеження станів символів
                    main_logger.debug(
                        f"Стан символів: "
                        f"Поточні={len(current_symbols)}, "
                        f"Попередні={len(prev_symbols)}, "
                        f"Нові={len(new_symbols)}"
                    )
                    if new_symbols:
                        new_symbols_list = list(new_symbols)
                        main_logger.info(
                            f"Preload історії для {len(new_symbols_list)} нових активів"
                        )
                        await preload_1m_history(
                            fetcher, new_symbols_list, buffer, lookback=lookback
                        )
                        await preload_daily_levels(fetcher, new_symbols_list, days=30)

                # Оновлюємо попередні символи
                prev_symbols = current_symbols
            else:
                main_logger.warning(
                    "Prefilter повернув порожній список, fast_symbols не оновлено."
                )
        except Exception as e:
            main_logger.warning("Помилка оновлення prefilter: %s", e)

        await asyncio.sleep(interval)  # 600 сек


# --- Preload історії для Stage1 ---
async def preload_1m_history(fetcher, fast_symbols, buffer, lookback=50):
    """
    Preload історичних 1m-барів для швидкого старту Stage1.
    Перевіряє, що lookback >= 12 (мінімум для індикаторів типу RSI/ATR).
    """
    if lookback < 12:
        main_logger.warning(
            "lookback (%d) для 1m-барів занадто малий. Встановлено мінімум 12.",
            lookback,
        )
        lookback = 12

    main_logger.info(
        "Preload 1m: завантажуємо %d 1m-барів для %d символів…",
        lookback,
        len(fast_symbols),
    )
    hist_data = await fetcher.get_data_batch(
        fast_symbols,
        interval="1m",
        limit=lookback,
        min_candles=lookback,
        show_progress=True,
        read_cache=False,
        write_cache=True,
    )

    # Додаємо бари в RAMBuffer
    for sym, df in hist_data.items():
        for bar in df.to_dict("records"):
            ts = bar["timestamp"]
            if isinstance(ts, pd.Timestamp):
                bar["timestamp"] = int(ts.value // 1_000_000)
            else:
                bar["timestamp"] = int(ts)
            buffer.add(sym.lower(), "1m", bar)
    main_logger.info(
        "Preload 1m завершено: історія додана в RAMBuffer для %d символів.",
        len(hist_data),
    )
    return hist_data


# --- Preload денних барів для глобальних рівнів ---
async def preload_daily_levels(fetcher, fast_symbols, days=30):
    """
    Preload денного таймфрейму для розрахунку глобальних рівнів підтримки/опору.
    Перевіряє, що days >= 30.
    """
    if days < 30:
        main_logger.warning(
            "Кількість днів (%d) для денних барів занадто мала. Встановлено мінімум 30.",
            days,
        )
        days = 30

    main_logger.info(
        "Preload Daily: завантажуємо %d денних свічок для %d символів…",
        days,
        len(fast_symbols),
    )
    daily_data = await fetcher.get_data_batch(
        fast_symbols,
        interval="1d",
        limit=days,
        min_candles=days,
        show_progress=False,
        read_cache=False,
        write_cache=False,
    )
    main_logger.info("Preload Daily завершено для %d символів.", len(daily_data))
    return daily_data


# --- HealthCheck для RAMBuffer ---
async def ram_buffer_healthcheck(
    buffer, symbols, max_age=90, interval=30, ws_worker=None, tf="1m"
):
    """
    Моніторинг живучості даних у RAMBuffer.
    Якщо дані по символу не оновлювались >max_age сек — лог WARN і опційно перезапуск WSWorker.
    """
    while True:
        now = int(time.time() * 1000)
        dead = []
        for sym in symbols:
            bars = buffer.get(sym, tf, 1)
            if not bars or (now - bars[-1]["timestamp"]) > max_age * 1000:
                dead.append(sym)
        if dead:
            main_logger.warning("[HealthCheck] Symbols stalled: %s", dead)
            if ws_worker is not None:
                main_logger.warning(
                    "[HealthCheck] Restarting WSWorker через застій символів."
                )
                await ws_worker.stop()
                asyncio.create_task(ws_worker.consume())
        else:
            main_logger.debug(
                "[HealthCheck] Всі символи активні (перевірено %d).", len(symbols)
            )
        await asyncio.sleep(interval)


async def trade_manager_updater(
    trade_manager: TradeLifecycleManager,
    buffer: RAMBuffer,
    monitor: AssetMonitorStage1,
    timeframe: str = "1m",
    lookback: int = 20,
    interval_sec: int = 30,
):
    """
    Фоновий таск: оновлює активні угоди,
    бере ATR/RSI/VOLUME із Stage1.stats, а не з сирих барів,
    і виводить лічильники active/closed.
    """
    while True:
        # 1) Оновлюємо всі активні угоди
        active = await trade_manager.get_active_trades()
        for tr in active:
            sym = tr["symbol"]
            bars = buffer.get(sym, timeframe, lookback)
            if not bars or len(bars) < lookback:
                continue

            df = pd.DataFrame(bars)
            # Використовуємо AssetMonitorStage1 для отримання stats
            stats = (
                await monitor.get_current_stats(sym, df)
                if hasattr(monitor, "get_current_stats")
                else {}
            )
            market_data = {
                "price": stats.get("current_price", 0),
                "atr": stats.get("atr", 0),
                "rsi": stats.get("rsi", 0),
                "volume": stats.get("volume_mean", 0),
                "context_break": stats.get("context_break", False),
            }
            await trade_manager.update_trade(tr["id"], market_data)

        # 2) Після оновлення виводимо статистику
        active = await trade_manager.get_active_trades()
        closed = await trade_manager.get_closed_trades()
        main_logger.info(
            f"🟢 Active trades: {len(active)}    🔴 Closed trades: {len(closed)}"
        )

        await asyncio.sleep(interval_sec)


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
    cache = await init_system()  # Ініціалізуємо кеш
    file_manager = FileManager()  # Файловий менеджер для зберігання даних
    buffer = RAMBuffer(max_bars=120)  # RAMBuffer для зберігання історії
    calibration_config = CalibrationConfig()  # Конфігурація калібрування
    stage2_config = STAGE2_CONFIG  # Конфігурація Stage2

    # Підключення до Redis
    redis_conn = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        decode_responses=True,
        encoding="utf-8",
    )

    launch_ui_consumer()  # Запускаємо UI-споживача у новому терміналі
    trade_manager = TradeLifecycleManager(log_file="trade_log.jsonl")  # Менеджер угод
    file_manager = FileManager()  # Файловий менеджер
    buffer = RAMBuffer(max_bars=500)  # RAMBuffer для зберігання історії
    thresholds = {
        "MIN_QUOTE_VOLUME": 1_000_000.0,  # Мінімальний об'єм торгівлі
        "MIN_PRICE_CHANGE": 3.0,  # Мінімальна зміна ціни
        "MIN_OPEN_INTEREST": 500_000.0,  # Мінімальний відкритий інтерес
        "MAX_SYMBOLS": 350,  # Максимальна кількість символів
    }

    # 2. Створюємо довгоживу ClientSession
    session = aiohttp.ClientSession()
    try:
        fetcher = OptimizedDataFetcher(cache_handler=cache, session=session)

        # ===== НОВА ЛОГІКА ВИБОРУ РЕЖИМУ =====
        use_manual_list = (
            True  # Змінити на False для автоматичного режиму, True - для ручного
        )

        if use_manual_list:
            # Ручний режим: використовуємо фіксований список
            fast_symbols = [
                "btcusdt",
                "ethusdt",
                "tonusdt",
                "adausdt",
            ]
            await cache.set_fast_symbols(fast_symbols, ttl=3600)  # TTL 1 година
            main_logger.info(
                f"[Main] Використовуємо ручний список символів: {fast_symbols}"
            )
        else:
            # Автоматичний режим: виконуємо первинний префільтр
            main_logger.info("[Main] Запускаємо первинний префільтр...")

            # Використовуємо новий механізм відбору активів
            fast_symbols = await get_filtered_assets(
                session=session,
                cache_handler=cache,
                min_quote_vol=1_000_000.0,
                min_price_change=3.0,
                min_oi=500_000.0,
                min_depth=50_000.0,
                min_atr=0.5,
                max_symbols=350,
                dynamic=False,  # Фіксований список для первинного префільтру
            )

            fast_symbols = [s.lower() for s in fast_symbols]
            await cache.set_fast_symbols(fast_symbols, ttl=600)  # TTL 10 хвилин
            # Логуємо кількість символів
            main_logger.info(
                f"[Main] Первинний префільтр: {len(fast_symbols)} символів"
            )

        # Отримуємо актуальний список символів
        fast_symbols = await cache.get_fast_symbols()
        if not fast_symbols:
            main_logger.error("[Main] Не вдалося отримати список символів. Завершення.")
            return

        main_logger.info(
            f"[Main] Початковий список символів: {fast_symbols} (кількість: {len(fast_symbols)})"
        )

        # Preload історії
        await preload_1m_history(
            fetcher, fast_symbols, buffer, lookback=500
        )  # 500 барів (~8.3 години)
        daily_data = await preload_daily_levels(fetcher, fast_symbols, days=30)

        # --- CalibrationEngine ---
        main_logger.info("[Main] Ініціалізуємо CalibrationEngine...")
        calib_engine = CalibrationEngine(
            config=calibration_config,
            stage2_config=stage2_config,
            ram_buffer=buffer,  # Використовуємо RAMBuffer для історії
            fetcher=fetcher,  # Оптимізований фетчер
            redis_client=redis_conn,  # Підключення до Redis
            interval="1m",  # Таймфрейм для аналізу
            min_bars=350,  # Мінімальна кількість барів для аналізу
            metric="profit_factor",  # Метрика для калібрування
        )

        # --- CalibrationQueue ---
        main_logger.info("[Main] Ініціалізуємо CalibrationQueue...")

        # Ініціалізуємо AssetStateManager для коректної інтеграції зі станом активів
        assets_current = [s.lower() for s in fast_symbols]
        state_manager = AssetStateManager(assets_current)
        calib_queue = CalibrationQueue(
            config=CalibrationConfig(
                n_trials=25,  # Ще менше спроб для швидкості
                lookback_days=14,  # 7 днів історії
                max_concurrent=15,  # Більше паралельних задач
            ),
            cache=cache,
            calib_engine=calib_engine,
            state_manager=state_manager,
        )
        await calib_queue.start_workers(n_workers=20)  # Збільшуємо кількість воркерів

        # Ініціалізація AssetMonitorStage1
        main_logger.info("[Main] Ініціалізуємо AssetMonitorStage1...")
        monitor = AssetMonitorStage1(
            cache_handler=cache,
            vol_z_threshold=2.5,
            rsi_overbought=70,
            rsi_oversold=30,
            min_reasons_for_alert=2,
        )
        # Встановлюємо глобальні рівні підтримки/опору
        monitor.set_global_levels(daily_data)

        # --- Виконуємо фон-воркери ---
        ws_task = asyncio.create_task(WSWorker(fast_symbols, buffer, cache).consume())
        health_task = asyncio.create_task(
            ram_buffer_healthcheck(buffer, fast_symbols, ws_worker=None)
        )

        # Ініціалізуємо UI-споживача
        main_logger.info("[Main] Ініціалізуємо UI-споживача...")
        ui = UI_Consumer()

        # Запускаємо Screening Producer
        main_logger.info("[Main] Запускаємо Screening Producer...")
        prod = asyncio.create_task(
            screening_producer(
                monitor,
                buffer,
                cache,
                fast_symbols,
                redis_conn,
                fetcher,
                trade_manager=trade_manager,
                timeframe="1m",
                lookback=50,
                interval_sec=30,
                calib_engine=calib_engine,
                calib_queue=calib_queue,
                # Додаємо state_manager для повної інтеграції (опціонально)
                state_manager=state_manager,
            )
        )

        # Публікуємо початковий стан в Redis
        main_logger.info("[Main] Публікуємо початковий стан в Redis...")
        await publish_full_state(state_manager, cache, redis_conn)

        # Запускаємо TradeLifecycleManager для управління угодами
        main_logger.info("[Main] Запускаємо TradeLifecycleManager...")
        trade_update_task = asyncio.create_task(
            trade_manager_updater(
                trade_manager,
                buffer,
                monitor,
            )
        )

        # Запускаємо періодичне оновлення тільки в автоматичному режимі
        prefilter_task = None
        if not use_manual_list:
            prefilter_task = asyncio.create_task(
                periodic_prefilter_and_update(
                    cache,
                    session,
                    thresholds,
                    interval=600,
                    buffer=buffer,
                    fetcher=fetcher,
                )
            )

        # Завдання для збору
        tasks_to_run = [
            ws_task,
            health_task,
            prod,
            trade_update_task,
        ]

        if prefilter_task:
            tasks_to_run.append(prefilter_task)

        await asyncio.gather(*tasks_to_run)
    finally:
        await session.close()


@router.get("/metrics")
async def metrics_endpoint():
    # calib_queue має бути глобально доступний або імпортований
    metrics = calib_queue.get_metrics()
    output = []
    # Лічильники
    for key, value in metrics.get("counters", {}).items():
        output.append(f"{key} {value}")
    # Гістограми
    for key, data in metrics.get("histograms", {}).items():
        output.append(f"{key}_count {data.get('count', 0)}")
        output.append(f"{key}_sum {data.get('sum', 0)}")
        output.append(f"{key}_avg {data.get('avg', 0)}")
    # Датчики
    for key, value in metrics.get("gauges", {}).items():
        output.append(f"{key} {value}")
    output.append(f"system_uptime {metrics.get('uptime', 0)}")
    return "\n".join(output)


# Додаємо роутер до FastAPI
app.include_router(router)


if __name__ == "__main__":
    try:
        asyncio.run(run_pipeline())
    except Exception as e:
        main_logger.error("Помилка виконання: %s", e, exc_info=True)
        sys.exit(1)
