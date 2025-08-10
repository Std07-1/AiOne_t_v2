# app/screening_producer.py
# -*- coding: utf-8 -*-

import pandas as pd
import logging
import asyncio
import json
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

from rich.console import Console
from rich.logging import RichHandler

from stage1.asset_monitoring import AssetMonitorStage1
from stage3.trade_manager import TradeLifecycleManager
from utils.utils_1_2 import _safe_float
from stage2.calibration_queue import CalibrationQueue
from stage2.processor import Stage2Processor
from utils.utils_1_2 import ensure_timestamp_column
from app.thresholds import save_thresholds, Thresholds
from stage2.level_manager import LevelManager
from app.utils.helper import (
    buffer_to_dataframe,
    resample_5m,
    estimate_atr_pct,
    get_tick_size,
)

# --- Налаштування логування ---
logger = logging.getLogger("app.screening_producer")
logger.setLevel(logging.INFO)  # Змінено на INFO для зменшення шуму
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False

# --- Глобальні константи ---
DEFAULT_LOOKBACK = 20  # Кількість барів для аналізу
DEFAULT_TIMEFRAME = "1m"  # Основний таймфрейм для аналізу
MIN_READY_PCT = 0.1  # Мінімальний % активів з даними для старту аналізу
MAX_PARALLEL_STAGE2 = 10  # Максимальна кількість паралельних задач Stage2
MIN_CONFIDENCE_TRADE = 0.5  # Мінімальна впевненість для відкриття угоди
TRADE_REFRESH_INTERVAL = 60  # Інтервал оновлення в секундах

# Глобальний семафор для обмеження паралельних задач Stage2
STAGE2_SEMAPHORE = asyncio.Semaphore(MAX_PARALLEL_STAGE2)


class AssetStateManager:
    """Централізований менеджер стану активів з підтримкою калібрування"""

    def __init__(self, initial_assets: List[str]):
        self.state = {}
        self.calibration_events = {}  # Для асинхронного очікування калібрування
        for asset in initial_assets:
            self.init_asset(asset)

    def init_asset(self, symbol: str):
        """Ініціалізація базового стану для активу"""
        self.state[symbol] = {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": [],
            "confidence": 0.0,
            "hints": ["Очікування даних..."],
            "tp": None,
            "sl": None,
            "cluster_factors": [],
            "stats": {},
            "state": "init",
            "stage2": False,
            "stage2_status": "pending",
            "last_updated": datetime.utcnow().isoformat(),
            "visible": True,
            "calib_status": "pending",
            "last_calib": None,
            "calib_priority": "normal",  # +++ НОВЕ ПОЛЕ +++
            "calib_queued_at": None,
        }
        # Створюємо подію для очікування калібрування
        self.calibration_events[symbol] = asyncio.Event()

    async def wait_for_calibration(self, symbol: str, timeout: float = 120):
        """Асинхронно чекає завершення калібрування активу або таймауту"""
        event = self.calibration_events.get(symbol)
        if event is None:
            # Якщо подія не створена, ініціалізуємо актив
            self.init_asset(symbol)
            event = self.calibration_events[symbol]
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass

    def update_asset(self, symbol: str, updates: Dict[str, Any]):
        """Оновлення стану активу з мерджем існуючих даних"""
        if symbol not in self.state:
            self.init_asset(symbol)

        current = self.state[symbol]
        # Додаємо статуси для відстеження термінових завдань
        if "calib_status" in updates:
            logger.debug(f"Updating {symbol} calib_status: {updates['calib_status']}")
        if "calib_status" in updates and updates["calib_status"] == "queued_urgent":
            updates["calib_priority"] = "urgent"
            updates["calib_queued_at"] = datetime.utcnow().isoformat()
        self.state[symbol] = {
            **current,
            **updates,
            "last_updated": datetime.utcnow().isoformat(),
        }

    def get_all_assets(self) -> List[Dict[str, Any]]:
        """Отримати всі активи для відображення в UI"""
        if not self.state:
            logger.warning("Стан активів порожній, немає даних для відображення")
            return []

        return list(self.state.values())

    def get_alert_signals(self) -> List[Dict[str, Any]]:
        """Отримати сигнали ALERT для Stage2 обробки"""
        return [
            asset for asset in self.state.values() if asset.get("signal") == "ALERT"
        ]

    async def update_calibration(self, symbol: str, params: Dict[str, Any]):
        # Генерація та збереження порогів
        thr = Thresholds.from_mapping(params)
        await save_thresholds(symbol, thr, self.cache)

        # Оновлення стану
        if symbol in self.state:
            self.state[symbol].update(
                {"calibrated_params": params, "calib_status": "completed"}
            )

        # Оновлення локального кешу
        if symbol in self._symbol_cfg:
            self._symbol_cfg[symbol] = thr

        # Сигнал про завершення
        if event := self.calibration_events.get(symbol):
            event.set()


def normalize_result_types(result: dict) -> dict:
    """Нормалізує типи даних та додає стан для UI"""
    numeric_fields = [
        "confidence",
        "tp",
        "sl",
        "current_price",
        "atr",
        "rsi",
        "volume",
        "volume_mean",
        "volume_usd",
        "volume_z",
        "open_interest",
        "btc_dependency_score",
    ]

    if "calibrated_params" in result:
        result["calibrated_params"] = {
            k: float(v) for k, v in result["calibrated_params"].items()
        }

    for field in numeric_fields:
        if field in result:
            result[field] = _safe_float(result[field])
        elif "stats" in result and field in result["stats"]:
            result["stats"][field] = _safe_float(result["stats"][field])

    # Визначення стану сигналу
    signal_type = result.get("signal", "NONE").upper()
    if signal_type == "ALERT":
        result["state"] = "alert"
    elif signal_type == "NORMAL":
        result["state"] = "normal"
    else:
        result["state"] = "no_trade"

    # Додаємо поле для відображення в UI
    result["visible"] = True

    return result


def make_serializable_safe(data) -> Any:
    """
    Рекурсивно перетворює об'єкти у JSON-сумісні формати:
    - DataFrame → список словників
    - Series → словник
    - Обробляє вкладені структури
    """
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")

    if hasattr(data, "to_dict") and not isinstance(data, dict):
        return data.to_dict()

    if isinstance(data, dict):
        return {key: make_serializable_safe(value) for key, value in data.items()}

    if isinstance(data, list):
        return [make_serializable_safe(item) for item in data]

    return data


async def process_asset_batch(
    symbols: list,
    monitor: AssetMonitorStage1,
    buffer: Any,
    timeframe: str,
    lookback: int,
    state_manager: AssetStateManager,
):
    """Обробляє батч символів та оновлює стан"""
    for symbol in symbols:
        bars = buffer.get(symbol, timeframe, lookback)
        if not bars or len(bars) < 5:
            state_manager.update_asset(symbol, create_no_data_signal(symbol))
            continue

        try:
            df = pd.DataFrame(bars)
            df = ensure_timestamp_column(df)  # Стандартизація timestamp
            signal = await monitor.check_anomalies(symbol, df)
            normalized = normalize_result_types(signal)
            state_manager.update_asset(symbol, normalized)
        except Exception as e:
            logger.error(f"Помилка AssetMonitor для {symbol}: {str(e)}")
            state_manager.update_asset(symbol, create_error_signal(symbol, str(e)))


def create_no_data_signal(symbol: str) -> Dict[str, Any]:
    return normalize_result_types(
        {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": ["Недостатньо даних"],
            "confidence": 0.0,
            "hints": ["Недостатньо даних для аналізу"],
            "state": "no_data",
            "stage2_status": "skipped",
        }
    )


def create_error_signal(symbol: str, error: str) -> Dict[str, Any]:
    return normalize_result_types(
        {
            "symbol": symbol,
            "signal": "NONE",
            "trigger_reasons": ["Помилка обробки"],
            "confidence": 0.0,
            "hints": [f"Помилка: {error}"],
            "state": "error",
            "stage2_status": "error",
        }
    )


async def open_trades(
    signals: List[Dict[str, Any]],
    trade_manager: TradeLifecycleManager,
    max_parallel: int,
) -> None:
    """
    Відкриває угоди для найперспективніших сигналів:
    1. Сортує сигнали за впевненістю
    2. Обмежує кількість одночасних угод
    3. Відкриває угоди через TradeLifecycleManager
    """
    if not trade_manager or not signals:
        return

    # Вибір найкращих сигналів
    sorted_signals = sorted(
        [s for s in signals if s.get("validation_passed")],
        key=lambda x: x.get("confidence", 0),
        reverse=True,
    )[:max_parallel]

    for signal in sorted_signals:
        symbol = signal["symbol"]
        confidence = _safe_float(signal.get("confidence", 0))

        # Детальне логування причин, чому угода не відкривається
        if confidence < MIN_CONFIDENCE_TRADE:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: впевненість {confidence:.3f} < поріг {MIN_CONFIDENCE_TRADE}"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            continue

        # Додаткові перевірки (можна розширити)
        if signal.get("signal", "NONE").upper() not in [
            "ALERT",
            "ALERT_BUY",
            "ALERT_SELL",
        ]:
            logger.info(
                f"⛔️ Не відкриваємо угоду для {symbol}: тип сигналу {signal.get('signal')} не є ALERT"
            )
            logger.debug(
                f"Деталі сигналу: {json.dumps(signal, ensure_ascii=False, default=str)}"
            )
            continue

        try:
            # Захист від нульових значень ATR
            atr = _safe_float(signal.get("atr"))
            if atr is None or atr < 0.0001:
                atr = 0.01
                logger.warning(
                    f"Коригування ATR для {symbol}: {signal.get('atr')} -> 0.01"
                )

            # Підготовка даних для відкриття угоди
            trade_data = {
                "symbol": symbol,
                "current_price": _safe_float(signal.get("current_price")),
                "atr": _safe_float(signal.get("atr")),
                "rsi": _safe_float(signal.get("rsi")),
                "volume": _safe_float(signal.get("volume_mean")),
                "tp": _safe_float(signal.get("tp")),
                "sl": _safe_float(signal.get("sl")),
                "confidence": confidence,
                "hints": signal.get("hints", []),
                "cluster_factors": signal.get("cluster_factors", []),
                "context_metadata": signal.get("context_metadata", {}),
                "strategy": "stage2_cluster",
            }

            # Відкриття угоди
            await trade_manager.open_trade(trade_data)
            logger.info(
                f"✅ Відкрито угоду для {symbol} (впевненість: {confidence:.2f})"
            )
        except Exception as e:
            logger.error(f"Помилка відкриття угоди для {symbol}: {str(e)}")


async def publish_full_state(
    state_manager: AssetStateManager, cache_handler: Any, redis_conn: Any
) -> None:
    try:
        all_assets = state_manager.get_all_assets()
        serialized_assets = []

        for asset in all_assets:
            # Конвертуємо всі числові поля у float
            for key in ["tp", "sl", "rsi", "volume", "atr", "confidence"]:
                if key in asset:
                    try:
                        asset[key] = (
                            float(asset[key])
                            if asset[key] not in [None, "", "NaN"]
                            else 0.0
                        )
                    except (TypeError, ValueError):
                        asset[key] = 0.0

            # Форматуємо ціни в UI
            if "stats" in asset and "current_price" in asset["stats"]:
                asset["price_str"] = str(asset["stats"]["current_price"])

            # Конвертуємо stats
            if "stats" in asset:
                for stat_key in [
                    "current_price",
                    "atr",
                    "volume_mean",
                    "open_interest",
                    "rsi",
                    "rel_strength",
                    "btc_dependency_score",
                ]:
                    if stat_key in asset["stats"]:
                        try:
                            asset["stats"][stat_key] = (
                                float(asset["stats"][stat_key])
                                if asset["stats"][stat_key] not in [None, "", "NaN"]
                                else 0.0
                            )
                        except (TypeError, ValueError):
                            asset["stats"][stat_key] = 0.0

            serialized_assets.append(asset)

        # Публікуємо в Redis
        await redis_conn.publish(
            "asset_state_update", json.dumps(serialized_assets, default=str)
        )

        logger.info(f"✅ Опубліковано стан {len(serialized_assets)} активів")

    except Exception as e:
        logger.error(f"Помилка публікації стану: {str(e)}")


async def process_single_stage2(
    signal: Dict[str, Any],
    processor: Stage2Processor,
    state_manager: AssetStateManager,
) -> None:
    """Обробка одного сигналу Stage2 з оновленням стану"""
    symbol = signal["symbol"]
    try:
        # Оновлюємо статус перед обробкою
        state_manager.update_asset(symbol, {"stage2_status": "processing"})

        # Безпосередня обробка через Stage2Processor
        result = await processor.process(signal)

        # Готуємо оновлення для стану активу
        update = {
            "stage2": True,
            "stage2_status": "completed",
            "last_updated": datetime.utcnow().isoformat(),
        }

        # Обробка результатів
        if "error" in result:
            update.update(
                {
                    "signal": "NONE",
                    "hints": [f"Stage2 error: {result.get('error', 'unknown')}"],
                }
            )
        else:
            # Визначаємо сигнал на основі рекомендації
            recommendation = result.get("recommendation", "")
            if recommendation in ["STRONG_BUY", "BUY_IN_DIPS"]:
                signal_type = "ALERT_BUY"
            elif recommendation in ["STRONG_SELL", "SELL_ON_RALLIES"]:
                signal_type = "ALERT_SELL"
            else:
                signal_type = "NORMAL"

            # Оновлюємо метрики
            confidence = result.get("confidence_metrics", {}).get(
                "composite_confidence", 0.0
            )
            risk_params = result.get("risk_parameters", {})

            update.update(
                {
                    "signal": signal_type,
                    "confidence": confidence,
                    "hints": [result.get("narrative", "")],
                    "tp": risk_params.get("tp_targets", [None])[0],
                    "sl": risk_params.get("sl_level"),
                    "market_context": result.get("market_context"),
                    "risk_parameters": risk_params,
                    "confidence_metrics": result.get("confidence_metrics"),
                    "anomaly_detection": result.get("anomaly_detection"),
                    "narrative": result.get("narrative"),
                    "recommendation": recommendation,
                }
            )

        # Оновлюємо стан активу
        state_manager.update_asset(symbol, update)

    except Exception as e:
        logger.error(f"Stage2 помилка для {symbol}: {str(e)}")
        state_manager.update_asset(symbol, {"stage2_status": "error", "error": str(e)})


async def process_single_stage2_with_semaphore(
    signal: Dict[str, Any],
    processor: Stage2Processor,
    semaphore: asyncio.Semaphore,
    state_manager: AssetStateManager,
) -> None:
    """Обробка сигналу Stage2 з обмеженням через семафор"""
    async with semaphore:
        await process_single_stage2(signal, processor, state_manager)


async def screening_producer(
    monitor: AssetMonitorStage1,
    buffer: Any,
    cache_handler: Any,
    assets: List[str],
    redis_conn: Any,
    fetcher: Any,
    trade_manager: Optional[TradeLifecycleManager] = None,
    reference_symbol: str = "BTCUSDT",
    timeframe: str = DEFAULT_TIMEFRAME,
    lookback: int = DEFAULT_LOOKBACK,
    interval_sec: int = TRADE_REFRESH_INTERVAL,
    min_ready_pct: float = MIN_READY_PCT,
    calib_engine: Optional[Any] = None,
    calib_queue: Optional[CalibrationQueue] = None,
    state_manager: AssetStateManager = None,
    level_manager: LevelManager = None,
    # Додаємо параметри налаштувань користувача
    user_lang: str = "UA",
    user_style: str = "explain",
) -> None:
    """
    Основний цикл генерації сигналів з централізованим станом:
    1. Ініціалізація системи
    2. Оновлення списку активів
    3. Перевірка готовності даних
    4. Генерація Stage1 сигналів
    5. Валідація Stage2 для сигналів ALERT
    6. Відкриття угод
    7. Періодичне оновлення

    Параметри:
    - screener: Система тригерів для Stage1
    - buffer: Буфер даних (RAMBuffer)
    - cache_handler: Обробник кешу
    - assets: Початковий список активів
    - redis_conn: З'єднання з Redis
    - fetcher: Засоби отримання даних
    - trade_manager: Менеджер торгів (опціонально)
    - reference_symbol: Базовий актив (BTCUSDT)
    - timeframe: Таймфрейм аналізу
    - lookback: Глибина історичних даних
    - interval_sec: Інтервал оновлення (секунди)
    - min_ready_pct: Мінімальний % активів з даними
    - calib_engine: Система калібрування (опціонально)
    """
    logger.info(
        f"🚀 Старт screening_producer: {len(assets)} активів, таймфрейм {timeframe}, "
        f"глибина {lookback}, оновлення кожні {interval_sec} сек"
    )

    # throttle для оновлення рівнів (раз на 20–30 сек)
    _last_levels_update_ts = {}
    LEVELS_UPDATE_EVERY = 25  # сек

    # Ініціалізація менеджера стану
    if state_manager is None:
        assets_current = [s.lower() for s in assets]
        state_manager = AssetStateManager(assets_current)
    else:
        assets_current = list(state_manager.state.keys())

    logger.info(f"Ініціалізовано стан для {len(assets_current)} активів")

    # Передаємо state_manager у calib_queue, якщо потрібно
    if calib_queue and hasattr(calib_queue, "set_state_manager"):
        calib_queue.set_state_manager(state_manager)

    # Створюємо Stage2Processor (без черг)
    processor = Stage2Processor(
        calib_queue,
        timeframe,
        state_manager,
        level_manager=level_manager,  # Передаємо LevelManager
        user_lang=user_lang,  # Передаємо налаштування
        user_style=user_style,
    )

    # Семафор для обмеження паралельних задач Stage2
    stage2_semaphore = asyncio.Semaphore(MAX_PARALLEL_STAGE2)

    # Публікація початкового стану
    await publish_full_state(state_manager, cache_handler, redis_conn)

    # Основний цикл обробки

    while True:
        start_time = time.time()

        # Оновлення списку активів
        try:
            new_assets_raw = await cache_handler.get_fast_symbols()
            if new_assets_raw:
                new_assets = [s.lower() for s in new_assets_raw]
                current_set = set(assets_current)
                new_set = set(new_assets)

                added = new_set - current_set
                removed = current_set - new_set

                # Оновлення стану для нових активів
                for symbol in added:
                    state_manager.init_asset(symbol)

                assets_current = list(new_set)
                # Синхронізуємо state_manager зі списком активів
                for symbol in removed:
                    if symbol in state_manager.state:
                        del state_manager.state[symbol]
                logger.info(
                    f"🔄 Оновлено список активів: +{len(added)}/-{len(removed)} "
                    f"(загалом: {len(assets_current)})"
                )
        except Exception as e:
            logger.error(f"Помилка оновлення активів: {str(e)}")

        # Оновлення ALERT-символів у CalibrationQueue
        if calib_queue:
            alert_symbols = [s["symbol"] for s in state_manager.get_alert_signals()]
            calib_queue.set_alert_symbols(alert_symbols)

        # Перевірка готовності даних
        ready_assets = []
        for symbol in assets_current + [reference_symbol.lower()]:
            bars = buffer.get(symbol, timeframe, lookback)
            if bars and len(bars) >= lookback:
                ready_assets.append(symbol)

        ready_count = len(ready_assets)
        min_ready = max(1, int(len(assets_current) * min_ready_pct))

        if ready_count < min_ready:
            logger.warning(
                f"⏳ Недостатньо даних: {ready_count}/{min_ready} активів готові. "
                f"Очікування {interval_sec} сек..."
            )
            await asyncio.sleep(interval_sec)
            continue

        logger.info(f"📊 Дані готові для {ready_count}/{len(assets_current)} активів")

        # --- Оновлення LevelSystem v2 для активів, які зараз обробляємо ---
        now_ts = int(time.time())
        for symbol in ready_assets:
            last_ts = _last_levels_update_ts.get(symbol, 0)
            if (now_ts - last_ts) < LEVELS_UPDATE_EVERY:
                continue

            df_1m = buffer_to_dataframe(buffer, symbol, limit=500)
            if df_1m is None or df_1m.empty:
                continue
            df_5m = resample_5m(df_1m)
            atr_pct = estimate_atr_pct(df_1m)
            price_hint = float(df_1m["close"].iloc[-1])
            tick_size = get_tick_size(symbol, price_hint=price_hint)

            level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)
            level_manager.update_from_bars(
                symbol, df_1m=df_1m, df_5m=df_5m
            )  # df_1d не обов'язковий щоразу

            _last_levels_update_ts[symbol] = now_ts

        # Додавання завдань калібрування з детальним логуванням
        if calib_queue:
            urgent_calib_tasks = []
            high_priority_tasks = []
            normal_tasks = []

            for symbol in ready_assets:
                asset_state = state_manager.state.get(symbol, {})
                # Визначення потреби калібрування
                last_calib_str = asset_state.get("last_calib")
                if last_calib_str:
                    last_calib = datetime.fromisoformat(last_calib_str)
                else:
                    last_calib = datetime.min

                needs_calib = (
                    asset_state.get("calib_status") in ["pending", "expired"]
                    or (datetime.utcnow() - last_calib).total_seconds() > 3600
                )

                if not needs_calib:
                    continue

                # Класифікація завдань
                if asset_state.get("signal") == "ALERT":
                    urgent_calib_tasks.append(symbol)
                elif asset_state.get("volume_usd", 0) > 5_000_000:
                    high_priority_tasks.append(symbol)
                else:
                    normal_tasks.append(symbol)

            # Обробка термінових завдань (асинхронно, з логуванням)
            for symbol in urgent_calib_tasks:
                logger.info(
                    f"[CALIB_QUEUE] Додаємо термінове калібрування: {symbol} (ALERT)"
                )
            urgent_tasks = [
                asyncio.create_task(
                    calib_queue.put(
                        symbol=symbol,
                        tf="1m",
                        is_urgent=True,  # Позначка терміновості
                        priority=1.0,
                    )
                )
                for symbol in urgent_calib_tasks
            ]
            await asyncio.gather(*urgent_tasks)
            for symbol in urgent_calib_tasks:
                logger.debug(f"[CALIB_QUEUE] Додано у чергу: {symbol} (ALERT)")
                state_manager.update_asset(
                    symbol,
                    {
                        "calib_status": "queued_urgent",
                        "calib_queued_at": datetime.utcnow().isoformat(),
                    },
                )

            # Обробка високопріоритетних завдань (асинхронно, з логуванням)
            for symbol in high_priority_tasks:
                logger.info(
                    f"[CALIB_QUEUE] Додаємо high-priority калібрування: {symbol}"
                )
            high_tasks = [
                asyncio.create_task(
                    calib_queue.put(
                        symbol=symbol, tf=timeframe, priority=1.0, is_high_priority=True
                    )
                )
                for symbol in high_priority_tasks
            ]
            await asyncio.gather(*high_tasks)
            for symbol in high_priority_tasks:
                logger.info(f"[CALIB_QUEUE] Додано у чергу: {symbol} (HIGH)")
                state_manager.update_asset(symbol, {"calib_status": "queued_high"})

            # Обробка звичайних завдань (асинхронно, з логуванням)
            for symbol in normal_tasks:
                logger.debug(f"[CALIB_QUEUE] Додаємо звичайне калібрування: {symbol}")
            normal_tasks_async = [
                asyncio.create_task(
                    calib_queue.put(symbol=symbol, tf=timeframe, priority=0.5)
                )
                for symbol in normal_tasks
            ]
            await asyncio.gather(*normal_tasks_async)
            for symbol in normal_tasks:
                logger.debug(f"[CALIB_QUEUE] Додано у чергу: {symbol} (NORMAL)")
                state_manager.update_asset(symbol, {"calib_status": "queued"})

        # Генерація Stage1 сигналів (паралельна обробка)
        try:
            batch_size = 20
            tasks = []
            for i in range(0, len(ready_assets), batch_size):
                batch = ready_assets[i : i + batch_size]
                tasks.append(
                    process_asset_batch(
                        batch, monitor, buffer, timeframe, lookback, state_manager
                    )
                )

                # +++ НОВА ЛОГІКА: Пріоритетне додавання калібрування для ALERT +++
                if calib_queue:
                    for symbol in batch:
                        asset_state = state_manager.state.get(symbol, {})
                        if asset_state.get("signal") == "ALERT":
                            # Перевірка чи потрібне калібрування
                            if asset_state.get("calib_status") not in [
                                "completed",
                                "in_progress",
                            ]:
                                logger.warning(
                                    f"🚨 Додаємо ТЕРМІНОВЕ калібрування для ALERT: {symbol}"
                                )
                                try:
                                    await calib_queue.put(
                                        symbol=symbol,
                                        tf=timeframe,
                                        priority=1.0,  # Максимальний пріоритет
                                        is_urgent=True,
                                        is_high_priority=True,
                                    )
                                    state_manager.update_asset(
                                        symbol,
                                        {
                                            "calib_status": "queued_urgent",
                                            "calib_queued_at": datetime.utcnow().isoformat(),
                                        },
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Помилка додавання термінового калібрування: {e}"
                                    )

            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Критична помилка Stage1: {str(e)}")

        # --- Інтеграція Stage2 через Stage2Processor (новий модуль) ---

        # Оновлення каліброваних параметрів
        if calib_queue:
            for symbol in ready_assets:
                if cached_params := await calib_queue.get_cached(symbol, timeframe):
                    await state_manager.update_calibration(symbol, cached_params)
                    # Додатково активуємо подію (на випадок, якщо update_calibration не викликається ззовні)
                    event = getattr(state_manager, "calibration_events", {}).get(symbol)
                    if event:
                        event.set()

        # Перевірка чи всі ALERT активи відкалібровані
        alert_signals = state_manager.get_alert_signals()
        if alert_signals and calib_queue:
            logger.info(f"[Stage2] Обробка {len(alert_signals)} сигналів...")

            # Перевірка калібрування
            not_calibrated = [
                s
                for s in alert_signals
                if state_manager.state[s["symbol"]].get("calib_status") != "completed"
            ]

            if not_calibrated:
                logger.warning(
                    f"⏳ Очікування калібрування для {len(not_calibrated)} активів..."
                )
                # Запит термінового калібрування
                for signal in not_calibrated:
                    symbol = signal["symbol"]
                    try:
                        await calib_queue.put(
                            symbol=symbol, tf=timeframe, priority=0.1, is_urgent=True
                        )
                        state_manager.update_asset(
                            symbol, {"calib_status": "requeued_urgent"}
                        )
                    except Exception as e:
                        logger.error(f"Помилка запиту калібрування для {symbol}: {e}")

                # Чекаємо 5 секунд на початок обробки (не блокуюче очікування)
                await asyncio.sleep(5)
                continue  # Переходимо до наступного циклу

            # Обробка сигналів через Stage2Processor
            tasks = []
            for signal in alert_signals:
                task = asyncio.create_task(
                    process_single_stage2_with_semaphore(
                        signal, processor, stage2_semaphore, state_manager
                    )
                )
                tasks.append(task)

            await asyncio.gather(*tasks)
            logger.info(f"[Stage2] Завершено обробку {len(alert_signals)} сигналів")
        else:
            logger.info("[Stage2] Немає сигналів ALERT для обробки")

        # Публікація стану активів
        logger.info("📢 Публікація стану активів...")
        await publish_full_state(state_manager, cache_handler, redis_conn)

        # Відкриття угод
        if trade_manager and alert_signals:
            logger.info("💼 Відкриття угод для Stage2 сигналів...")
            max_trades = getattr(trade_manager, "max_parallel_trades", 3)
            if max_trades is None or max_trades <= 0:
                max_trades = 3
            logger.info(f"Максимальна кількість угод: {max_trades}")
            await open_trades(alert_signals, trade_manager, max_trades)
        else:
            logger.info(
                "💼 Торгівля Stage2 вимкнена або немає сигналів для відкриття угод"
            )

        # Очікування наступного циклу
        processing_time = time.time() - start_time
        logger.info(f"⏳ Час обробки циклу: {processing_time:.2f} сек")
        if processing_time < 1:
            logger.warning(
                "Час обробки циклу менше 1 секунди, можливо, система працює занадто швидко"
            )
        # Визначення часу очікування до наступного циклу
        if processing_time >= interval_sec:
            logger.warning(
                f"Час обробки циклу ({processing_time:.2f} сек) перевищує інтервал оновлення ({interval_sec} сек)"
            )
            # Якщо час обробки перевищує інтервал, чекаємо 1 секунду
            # Це дозволяє уникнути надто частих циклів, які можуть призвести
            # до перевантаження системи
            # і зменшує ризик втрати даних через часті запити до Redis
            logger.info("⏱ Час очікування до наступного циклу: 1 сек")
            # Чекаємо 1 секунду, щоб уникнути надто частих циклів
            sleep_time = 1
        else:
            # Якщо час обробки менший за інтервал, чекаємо залишок часу
            # до наступного циклу, щоб дотримуватися заданого інтервалу
            logger.info(
                f"⏱ Час очікування до наступного циклу: {interval_sec - int(processing_time)} сек"
            )
            # Гарантуємо, що час очікування не буде менше 1 секунди
        sleep_time = max(1, interval_sec - int(processing_time))
        logger.info(f"⏱ Час обробки: {processing_time:.2f} сек")
        await asyncio.sleep(sleep_time)


"""
Послідовність роботи:

```mermaid

graph TD
    A[Оновлення активів] --> B[Перевірка даних]
    B --> C[Stage1 Моніторинг]
    C --> D{ALERT?}
    D -->|Так| E[Stage2 Валідація]
    D -->|Ні| F[Оновлення стану]
    E --> G[Калібрування]
    G --> H[Відкриття угод]
    H --> I[Публікація стану]
    I --> J[Очікування]
    J --> A

``` 

"""
