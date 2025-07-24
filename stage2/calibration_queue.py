# app/calibration_queue.py
# -*- coding: utf-8 -*-
# app/calibration_queue.py
# -*- coding: utf-8 -*-
"""
Покращена версія CalibrationQueue з:
- Захистом від втрати завдань
- Адаптивними параметрами калібрування
- Старінням пріоритетів
- Метриками продуктивності
- Circuit Breaker
- TTL-інвалідацією кешу
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, NamedTuple
import json

# Внутрішні пакети
from data.cache_handler import SimpleCacheHandler
from stage2.calibration_engine import CalibrationEngine
from stage2.calibration.calibration_config import CalibrationConfig
from app.utils.metrics import MetricsCollector  # Новий модуль для метрик

from rich.console import Console
from rich.logging import RichHandler

# ─────────────────── Налаштування логування ────────────────────
log = logging.getLogger("calib_queue")
log.setLevel(logging.DEBUG)
log.handlers.clear()
log.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
log.propagate = False  # ← Критично важливо!

# Константи для Circuit Breaker
MAX_ATTEMPTS = 3  # Максимальна кількість спроб калібрування
CIRCUIT_BREAKER_TIMEOUT = 600  # 10 хвилин

DEFAULT_ASSET_CLASS = "futures"  # Дефолтний клас активів


class AssetClassConfig:
    def __init__(self, mapping: Dict[str, list]):
        self.mapping = mapping
        self.compiled_patterns = self._compile_patterns(mapping)

    def _compile_patterns(
        self, mapping: Dict[str, list]
    ) -> Dict[str, list[re.Pattern]]:
        compiled = {}
        for asset_class, regex_list in mapping.items():
            compiled[asset_class] = [
                re.compile(regex, re.IGNORECASE) for regex in regex_list
            ]
        return compiled

    def match_symbol(self, symbol: str) -> Optional[str]:
        symbol = symbol.upper()
        for asset_class, patterns in self.compiled_patterns.items():
            for pattern in patterns:
                if pattern.search(symbol):
                    return asset_class
        return None

    async def update_calibration_status(self, symbol: str, status: str):
        if symbol in self.state:
            self.state[symbol]["calib_status"] = status
            self.state[symbol]["last_updated"] = datetime.utcnow().isoformat()


class CalibrationTask(NamedTuple):
    symbol: str
    tf: str
    priority: float
    created_at: float = time.time()
    attempts: int = 0
    is_high_priority: bool = False
    is_urgent: bool = False


class CalibrationQueue:
    def set_state_manager(self, state_manager: Any) -> None:
        """Встановлює зовнішній state_manager для оновлення статусу калібрування."""
        self._state_manager = state_manager
        log.info(f"StateManager set for CalibrationQueue: {id(state_manager)}")

    def __init__(
        self,
        config: CalibrationConfig,
        cache: SimpleCacheHandler,
        calib_engine: CalibrationEngine,
        metrics: Optional[MetricsCollector] = None,
        state_manager: Optional[Any] = None,
    ) -> None:
        self._cache = cache
        self._engine = calib_engine
        self.config = config
        self._sem = asyncio.Semaphore(config.max_concurrent)
        self._queue = asyncio.PriorityQueue()
        self._workers = []
        self._metrics = metrics or MetricsCollector()
        self._failure_count = defaultdict(int)
        self._circuit_breaker = {}

        # Використовуємо asset_class_mapping з конфігу
        self.asset_class_config = AssetClassConfig(config.asset_class_mapping)

        self._state_manager = state_manager
        self.alert_symbols = set()

        self.default_calib_count = 0

        # Ініціалізація базових метрик
        self._metrics.gauge("queue_size", 0)
        self._metrics.gauge("active_workers", 0)
        self._metrics.gauge("circuit_breaker_active", 0)
        log.info(
            f"CalibrationQueue created. Max concurrent: {config.max_concurrent}, "
            f"Default trials: {config.n_trials}, Lookback days: {config.lookback_days}"
        )

    def _get_asset_class(self, symbol: str) -> str:
        """Визначає клас активу на основі конфігурації"""
        if matched_class := self.asset_class_config.match_symbol(symbol):
            return matched_class
        return DEFAULT_ASSET_CLASS  # Використовуємо за замовчуванням

    # ──────────────────── Публічний API ────────────────────────
    async def put(
        self,
        symbol: str,
        tf: str,
        priority: float = 0.5,
        is_high_priority: bool = False,
        is_urgent: bool = False,
    ) -> None:
        """
        Додає завдання до черги зі старінням пріоритетів та детальним логуванням.
        Якщо завдання вже існує — оновлює його пріоритет для термінових/ALERT.
        Виправлено логіку видалення старого завдання: видаляється саме той кортеж, який знайдено.
        """
        log.debug(
            f"[put] Вхід: symbol={symbol}, tf={tf}, priority={priority}, is_high_priority={is_high_priority}, is_urgent={is_urgent}"
        )

        # Перевірка наявності символу та таймфрейму
        if not symbol or not tf:
            log.error("❌ Невірний символ або таймфрейм")
            return

        # Перевірка на circuit breaker
        if self._is_circuit_broken(symbol):
            log.warning(f"[put] Circuit breaker active for {symbol}, skipping")
            return

        # Збільшуємо пріоритет для термінових завдань
        if is_urgent:
            priority = 1.0  # Максимальний пріоритет
        elif is_high_priority:
            priority = min(0.8, priority + 0.3)  # Високий пріоритет
        else:
            priority = max(0.5, priority)  # Звичайний пріоритет

        # Перевірка на валідність символу та таймфрейму
        if not isinstance(symbol, str) or not isinstance(tf, str):
            log.error("Невірний тип символу або таймфрейму")
            return

        # Перевірка на наявність активного двигуна
        if self._engine is None:
            log.error("❌ CalibrationEngine не ініціалізовано!")
            return

        # --- Перевірка чи завдання вже в черзі ---
        current_time = time.time()
        found_task = None
        found_prio_tuple = None
        for prio_task in list(self._queue._queue):
            # prio_task: (priority, CalibrationTask)
            _, task = prio_task
            if task.symbol == symbol.lower() and task.tf == tf:
                found_task = task
                found_prio_tuple = prio_task
                break

        if found_task:
            log.debug(f"[put] Завдання вже існує: {found_task}")
            # Якщо вже є термінове — нічого не робимо
            if is_urgent and not found_task.is_urgent:
                log.info(f"🆙 Оновлюємо пріоритет для {symbol}/{tf} (термінове)")
                new_task = found_task._replace(
                    priority=1.0,  # Максимальний пріоритет для термінових
                    is_urgent=True,
                    is_high_priority=True,
                    created_at=current_time,
                )
                # Видаляємо старе завдання (кортеж)
                try:
                    self._queue._queue.remove(found_prio_tuple)
                    log.debug(
                        f"[put] Видалено старий кортеж з черги: {found_prio_tuple}"
                    )
                except ValueError:
                    log.error(
                        f"Помилка видалення {symbol}/{tf} з черги: кортеж не знайдено"
                    )
                # Додаємо нове завдання з оновленим пріоритетом
                await self._queue.put(self._calculate_priority(new_task))
                log.info(f"[put] Додано оновлене термінове завдання для {symbol}/{tf}")
                return
            log.debug(f"[put] Завдання вже в черзі, дубль не додається")
            return  # Завдання вже в черзі, не додаємо дубль

        log.info(
            f"[QUEUE_PUT] Додаємо завдання: symbol={symbol}, tf={tf}, priority={priority}, is_urgent={is_urgent}, is_high_priority={is_high_priority}"
        )

        task = CalibrationTask(
            symbol=symbol.lower(),
            tf=tf,
            priority=priority,
            is_high_priority=is_high_priority,
            is_urgent=is_urgent,  # Додано параметр is_urgent
            created_at=time.time(),
        )

        # Додатковий буст для термінових завдань
        if is_urgent:
            task = task._replace(priority=min(1.0, priority + 0.3))
        # Отримання класу активів та дефолтів
        await self._queue.put(self._calculate_priority(task))
        self._metrics.inc("queue_added_urgent" if is_urgent else "queue_added")
        self._metrics.gauge("queue_size", self._queue.qsize())
        self._metrics.gauge(
            "active_workers", self._sem._value
        )  # pylint: disable=protected-access
        self._metrics.gauge("circuit_breaker_active", len(self._circuit_breaker))
        log.info(
            f"[QUEUE_PUT] Завдання додано у чергу: symbol={symbol}, tf={tf}, priority={priority}, is_urgent={is_urgent}, is_high_priority={is_high_priority}, queue_size={self._queue.qsize()}"
        )
        if is_high_priority:
            self._metrics.inc("high_priority_tasks")
        else:
            self._metrics.inc("normal_priority_tasks")
        # Логування пріоритету
        if priority > 1.0:
            log.warning("Високий пріоритет для %s/%s: %.2f", symbol, tf, priority)
        else:
            log.debug("Звичайний пріоритет для %s/%s: %.2f", symbol, tf, priority)
        # Логування розміру черги
        log.debug("Поточний розмір черги: %d", self._queue.qsize())
        # Логування активних воркерів
        log.debug("Поточна кількість активних воркерів: %d", self._sem._value)
        # Логування активних circuit breakers
        log.debug("Активні circuit breakers: %d", len(self._circuit_breaker))

    async def start_workers(self, n_workers: int = 2) -> None:
        """Запускає воркери з обробкою помилок."""
        log.info(f"[start_workers] self._engine: {self._engine}")
        log.info(
            f"[start_workers] self._sem._value: {self._sem._value}, len(self._workers): {len(self._workers)}"
        )
        if not self._engine:
            log.error("❌ CalibrationEngine не ініціалізовано!")
            return
        available_slots = self._sem._value - len(self._workers)
        workers_to_start = min(n_workers, available_slots)
        log.info(
            f"[start_workers] available_slots: {available_slots}, workers_to_start: {workers_to_start}"
        )
        if workers_to_start <= 0:
            log.warning(
                "Досягнуто максимальну кількість воркерів: %d", self._sem._value
            )
            return
        for _ in range(workers_to_start):
            log.debug("Створення воркера для обробки завдань...")
            worker = asyncio.create_task(self._safe_worker(), name="CalibrationWorker")
            self._workers.append(worker)
            self._metrics.inc("active_workers")
            log.debug("Воркер створено, очікування на запуск...")
        log.info("Старт воркерів: %d", workers_to_start)

    async def shutdown(self, timeout: float = 10.0) -> None:
        """Коректне завершення з очікуванням завершення завдань."""
        log.info("Завершення роботи черги...")
        for worker in self._workers:
            worker.cancel()

        done, pending = await asyncio.wait(
            self._workers, timeout=timeout, return_when=asyncio.ALL_COMPLETED
        )

        if pending:
            log.warning("Час очікування завершення воркерів вичерпано")

        log.info("Черга зупинена")

    # ────────────────── Внутрішня логіка ──────────────────────
    def _calculate_priority(
        self, task: CalibrationTask
    ) -> Tuple[float, CalibrationTask]:
        """Адаптивне старіння з підвищенням пріоритету для термінових та ALERT-активів"""
        base_priority = -task.priority
        if task.is_urgent:
            # Максимальний пріоритет для термінових завдань
            return (-100.0, task)
        wait_time = time.time() - task.created_at
        age_factor = min(wait_time * 0.05, 2.0)  # Більш агресивне старіння
        # Підвищення пріоритету для активів з ALERT
        if hasattr(self, "alert_symbols") and task.symbol in self.alert_symbols:
            age_factor *= 1.5
        return (base_priority - age_factor, task)

    def set_alert_symbols(self, symbols):
        """Оновлює список активів з активними ALERT-сигналами"""
        self.alert_symbols = set(s.lower() for s in symbols)
        log.info(f"Оновлено ALERT-символи: {len(self.alert_symbols)} активів")

    def get_performance_metrics(self) -> dict:
        """Розширені метрики продуктивності"""
        return {
            "queue_size": self._queue.qsize(),
            "active_tasks": self._sem._value,  # pylint: disable=protected-access
            "avg_calibration_time": (
                self._metrics.get_avg("calibration_time")
                if hasattr(self._metrics, "get_avg")
                else None
            ),
            "urgent_tasks_processed": (
                self._metrics.get_count("urgent_tasks_processed")
                if hasattr(self._metrics, "get_count")
                else None
            ),
            "default_calibrations": getattr(self, "default_calib_count", 0),
            "cache_hit_rate": (
                self._metrics.get_ratio("cache_hits", "cache_attempts")
                if hasattr(self._metrics, "get_ratio")
                else None
            ),
            "failure_rate": (
                self._metrics.get_ratio("calibration_failures", "calibration_attempts")
                if hasattr(self._metrics, "get_ratio")
                else None
            ),
        }

    async def adaptive_worker_scaling(self):
        """Автоматичне регулювання кількості воркерів"""
        while True:
            queue_size = self._queue.qsize()
            active_workers = len(self._workers)
            if queue_size > 20 and active_workers < self.max_concurrent:
                new_workers = min(5, self.max_concurrent - active_workers)
                await self.start_workers(new_workers)
                log.info(
                    f"Додано {new_workers} воркерів. Загалом: {active_workers + new_workers}"
                )
            elif queue_size < 5 and active_workers > 3:
                remove_count = min(2, active_workers - 3)
                # TODO: Логіка зупинки надлишкових воркерів (можна реалізувати через cancel)
                log.info(
                    f"Можна зупинити {remove_count} воркерів (реалізуйте за потреби)"
                )
            await asyncio.sleep(30)

    def _calculate_ttl(self, symbol: str) -> int:
        """Визначає TTL на основі типу активу та волатильності"""
        asset_class = self._get_asset_class(symbol)
        volatility = self._get_symbol_volatility(symbol)

        # Базові значення TTL з конфігу
        base_ttl = {
            "meme": 3600,
            "ai": 5400,
            "nft": 7200,
            "defi": 10800,
            "spot": 14400,
            "futures": 18000,
        }.get(asset_class, 7200)

        # Коригування TTL на основі волатильності
        if volatility > 40:
            return max(
                1800, int(base_ttl * 0.5)
            )  # Скорочуємо TTL для волатильних активів
        elif volatility < 15:
            return min(28800, int(base_ttl * 1.5))  # Збільшуємо для стабільних

        return base_ttl

    async def _safe_worker(self) -> None:
        """Воркер з обробкою помилок та повторними спробами."""
        log.info("[safe_worker] Воркер стартує...")
        while True:
            # Додаємо короткий sleep для звільнення потоку
            await asyncio.sleep(0.01)
            log.debug("[safe_worker] Перед запуском worker_loop")
            await self._worker_loop()
            log.debug("🚀 Воркер завершив worker_loop (має бути нескінченний цикл)")

    async def _worker_loop(self) -> None:
        """Основний цикл обробки завдань з діагностикою очікування."""
        worker_id = id(asyncio.current_task())
        log.info(f"[worker_loop] Стартує цикл обробки завдань (worker_id={worker_id})")
        last_get_time = time.time()
        while True:
            await asyncio.sleep(0)
            queue_size = self._queue.qsize()
            log.info(
                f"[worker_loop] Поточний розмір черги: {queue_size} (worker_id={worker_id})"
            )
            if queue_size > 0:
                log.info(
                    f"[worker_loop] Черга містить {queue_size} завдань (worker_id={worker_id})"
                )

            get_start = time.time()
            prio_task = await self._queue.get()
            get_end = time.time()
            wait_time = get_end - get_start
            if wait_time > 1.0:
                log.warning(
                    f"[worker_loop] Воркер {worker_id} чекав {wait_time:.2f} сек на завдання з черги!"
                )
            else:
                log.debug(
                    f"[worker_loop] Воркер {worker_id} отримав завдання через {wait_time:.2f} сек"
                )
            log.info(
                f"[worker_loop] Отримано завдання з черги: {prio_task} (worker_id={worker_id})"
            )
            _, task = prio_task
            log.debug(
                f"[worker_loop] Воркер {worker_id} отримав семафор для {task.symbol}/{task.tf}"
            )
            if self._engine is None:
                log.error(
                    "❌ CalibrationEngine не ініціалізовано! (worker_id={worker_id})"
                )
                continue
            if task is None:
                log.info(
                    f"[worker_loop] Отримано сигнал для завершення воркера (worker_id={worker_id})"
                )
                break
            symbol, tf = task.symbol, task.tf
            log.info(
                f"[worker_loop] Обробка завдання: {symbol}/{tf} (prio={task.priority}) is_urgent={task.is_urgent} is_high_priority={task.is_high_priority} (worker_id={worker_id})"
            )

            if task.is_urgent:
                log.warning(
                    f"🚨 [worker_loop] ТЕРМІНОВЕ калібрування {task.symbol}/{task.tf} (worker_id={worker_id})"
                )
                self._metrics.inc("urgent_tasks_processed")

            queue_time = time.time() - task.created_at
            self._metrics.observe("queue_time", queue_time)
            log.info(
                f"[worker_loop] Завдання в черзі {queue_time:.2f} сек (worker_id={worker_id})"
            )

            if self._is_circuit_broken(symbol):
                log.warning(
                    f"[worker_loop] Circuit breaker активний для {symbol} (worker_id={worker_id})"
                )
                self._queue.task_done()
                continue

            # Безпечна перевірка наявності _state_manager
            if hasattr(self, "_state_manager") and self._state_manager:
                self._state_manager.update_asset(
                    task.symbol,
                    {
                        "calib_status": "in_progress",
                        "calib_started": datetime.utcnow().isoformat(),
                    },
                )
            log.debug(
                f"[worker_loop] Воркер {worker_id} очікує на семафор для {symbol}/{tf}"
            )

            async with self._sem:
                log.info(
                    f"[worker_loop] Воркер {worker_id} починає калібрування {symbol}/{tf}"
                )
                await self._process_task(task)
                self._queue.task_done()
                self._metrics.observe("queue_time", time.time() - task.created_at)

    async def _process_task(self, task: CalibrationTask) -> None:
        """Обробка одного завдання калібрування з діагностикою та детальним логуванням"""
        symbol, tf = task.symbol, task.tf
        log.info(
            f"[CALIB_WORKER] ▶️ Старт калібрування {symbol}/{tf} (prio={task.priority}, urgent={task.is_urgent}, high={task.is_high_priority})"
        )
        processing_start = time.time()  # Діагностичні метрики

        # Отримання динамічних параметрів з адаптивним TTL
        params = self._get_dynamic_params(symbol, task.is_high_priority)
        params["result_ttl"] = self._calculate_ttl(symbol)
        log.info(f"[CALIB_WORKER] Параметри калібрування для {symbol}/{tf}: {params}")

        if task.is_urgent:
            # Швидкий режим для ALERT
            params["n_trials"] = max(15, params["n_trials"] // 2)
            params["lookback_days"] = max(10, params["lookback_days"] // 2)
            log.warning(
                f"[CALIB_WORKER] 🚨 ТЕРМІНОВИЙ РЕЖИМ: {task.symbol} (n_trials={params['n_trials']})"
            )

        # Виконання калібрування
        log.info(f"[CALIB_WORKER] Виклик calibrate_symbol_timeframe для {symbol}/{tf}")
        try:
            result = await self._engine.calibrate_symbol_timeframe(
                symbol=symbol,
                timeframe=tf,
                date_from=datetime.utcnow() - timedelta(days=params["lookback_days"]),
                date_to=datetime.utcnow(),
                n_trials=params["n_trials"],
                config_template=None,
                override_old=False,
            )
            log.info(
                f"[CALIB_WORKER] ✅ Завершено calibrate_symbol_timeframe для {symbol}/{tf}"
            )
        except Exception as e:
            log.error(
                f"[CALIB_WORKER] ❌ Помилка calibrate_symbol_timeframe для {symbol}/{tf}: {e}"
            )
            self._metrics.inc("calibration_failures")
            if hasattr(self, "_state_manager") and self._state_manager:
                self._state_manager.update_asset(
                    symbol,
                    {"calib_status": "failed", "calib_error": str(e)},
                )
            raise

        log.info(f"[CALIB_WORKER] Результат калібрування для {symbol}/{tf}: {result}")
        # Перевірка результату калібрування
        if not isinstance(result, dict):
            log.error(
                f"[CALIB_WORKER] {symbol} Калібрування повернуло невалідний результат: {result}"
            )
            self._metrics.inc("calibration_invalid_results")
            result = None

        if result is None:
            log.error(f"[CALIB_WORKER] Калібрування не вдалося для {symbol}/{tf}")
            self._metrics.inc("calibration_failures")
            if hasattr(self, "_state_manager") and self._state_manager:
                self._state_manager.update_asset(
                    symbol,
                    {"calib_status": "failed", "calib_error": "calibration_failed"},
                )
            raise RuntimeError(f"Калібрування не вдалося для {symbol}/{tf}")

        # Оновлення кешу з адаптивним TTL
        log.info(f"[CALIB_WORKER] Оновлення кешу для {symbol}/{tf}")
        await self._cache.set_json(
            symbol,
            tf,
            result,
            ttl=params["result_ttl"],
            prefix="calib",
        )

        # Оновлення статусу та результатів
        if hasattr(self, "_state_manager") and self._state_manager:
            log.info(f"Updating state for {symbol} to 'completed'")
            self._state_manager.update_asset(
                symbol,
                {
                    "calib_status": "completed",
                    "last_calib": datetime.utcnow().isoformat(),
                    "calib_params": result,
                },
            )

        # Запис метрик
        calibration_time = time.time() - processing_start
        self._metrics.observe("calibration_time", calibration_time)
        self._metrics.gauge(f"calib_{symbol}_status", 1)
        log.info(
            f"[CALIB_WORKER] ✅ Успішно: {symbol}/{tf} (час: {calibration_time:.2f} сек)"
        )

    async def _handle_task_failure(
        self, task: CalibrationTask, error: Exception
    ) -> None:
        """Обробка невдалого виконання завдання."""
        symbol, tf = task.symbol, task.tf
        log.error("Помилка %s/%s: %s", symbol, tf, error)
        self._metrics.inc("calibration_errors")

        # Логіка повторної спроби
        if task.attempts < MAX_ATTEMPTS:
            new_task = task._replace(
                attempts=task.attempts + 1,
                created_at=time.time() + (2**task.attempts),  # Exponential backoff
            )
            await self._queue.put(self._calculate_priority(new_task))
            self._metrics.inc("queue_retries")
            log.debug("Повторна спроба #%d: %s/%s", new_task.attempts, symbol, tf)
        else:
            self._trigger_circuit_breaker(symbol)
            log.error("Досягнуто макс. спроб для %s. Circuit breaker активний", symbol)

    def _get_dynamic_params(
        self, symbol: str, is_high_priority: bool = False, is_urgent: bool = False
    ) -> Dict[str, Any]:
        """Генерує динамічні параметри калібрування на основі:
        - Конфігурації системи
        - Типу активу
        - Волатильності
        - Пріоритету завдання
        """
        # Базові параметри з конфігу
        base_config = {
            "lookback_days": self.config.lookback_days,
            "n_trials": self.config.n_trials,
            "result_ttl": self._calculate_ttl(symbol),
        }

        # Отримуємо клас активу для додаткової адаптації
        asset_class = self._get_asset_class(symbol)

        # Адаптація на основі пріоритету завдання
        if is_urgent:
            # Швидкий режим для термінових завдань
            base_config.update(
                {
                    "n_trials": max(15, int(self.config.n_trials * 0.6)),
                    "lookback_days": max(10, int(self.config.lookback_days * 0.7)),
                }
            )
        elif is_high_priority:
            # Оптимізований режим для високопріоритетних завдань
            base_config.update(
                {
                    "n_trials": int(self.config.n_trials * 0.8),
                    "lookback_days": int(self.config.lookback_days * 0.9),
                }
            )

        # Адаптація на основі волатильності
        volatility = self._get_symbol_volatility(symbol)
        base_config = self._adjust_params_by_volatility(base_config, volatility)

        # Адаптація на основі класу активу
        if asset_class == "meme":
            base_config["n_trials"] = min(30, base_config["n_trials"])
            base_config["lookback_days"] = min(15, base_config["lookback_days"])
        elif asset_class == "stable":
            base_config["n_trials"] = max(10, int(base_config["n_trials"] * 0.5))

        return base_config

    def _adjust_params_by_volatility(
        self, params: Dict[str, Any], volatility: float
    ) -> Dict[str, Any]:
        """Адаптує параметри калібрування на основі волатильності"""
        adjusted = params.copy()

        if volatility > 50:  # Висока волатильність
            adjusted.update(
                {
                    "n_trials": min(60, int(params["n_trials"] * 1.5)),
                    "lookback_days": max(7, int(params["lookback_days"] * 0.7)),
                }
            )
        elif volatility > 30:  # Середня волатильність
            adjusted.update({"n_trials": min(45, int(params["n_trials"] * 1.2))})
        elif volatility < 10:  # Низька волатильність
            adjusted.update(
                {
                    "n_trials": max(10, int(params["n_trials"] * 0.8)),
                    "lookback_days": min(60, int(params["lookback_days"] * 1.3)),
                }
            )

        return adjusted

    def _get_symbol_volatility(self, symbol: str) -> float:
        """Заглушка для отримання волатильності символу (реалізація залежить від джерела даних)"""
        # TODO: Інтегрувати з реальним джерелом даних
        # Тимчасові значення на основі класу активу
        asset_class = self._get_asset_class(symbol)
        volatility_map = {
            "meme": 45.0,
            "ai": 35.0,
            "nft": 30.0,
            "defi": 25.0,
            "spot": 20.0,
            "futures": 40.0,
            "stable": 5.0,
        }
        return volatility_map.get(asset_class, 25.0)

    def _is_circuit_broken(self, symbol: str) -> bool:
        """Перевіряє чи активний circuit breaker для символу."""
        expiry = self._circuit_breaker.get(symbol, 0)
        return time.time() < expiry

    def _trigger_circuit_breaker(self, symbol: str) -> None:
        """Активує circuit breaker для символу."""
        self._circuit_breaker[symbol] = time.time() + CIRCUIT_BREAKER_TIMEOUT
        log.warning("Circuit breaker активовано для %s", symbol)

    # ──────────────── Утиліти та моніторинг ────────────────────
    @staticmethod
    def _redis_key(symbol: str, tf: str) -> str:
        return f"calib:{symbol}:{tf}"

    async def get_cached(self, symbol: str, tf: str) -> Optional[Dict[str, Any]]:
        """Перевіряє кеш з автоматичною інвалідацією."""
        redis_key = self._redis_key(symbol, tf)
        if cached := await self._cache.fetch_from_cache(
            symbol, "", prefix="calib", raw=False  # порожній interval
        ):
            self._metrics.inc("cache_hits")
            return cached

        self._metrics.inc("cache_misses")
        return None

    async def await_result(
        self,
        symbol: str,
        tf: str,
        timeout: float = 15.0,
    ) -> Optional[Dict[str, Any]]:
        """Очікує результат з Pub/Sub та автоматичним оновленням."""
        redis_key = self._redis_key(symbol, tf)
        start_time = time.time()

        while (time.time() - start_time) < timeout:
            if result := await self.get_cached(symbol, tf):
                return result

            # Fallback polling
            await asyncio.sleep(0.5)

        log.warning("Таймаут очікування для %s/%s", symbol, tf)
        return None

    async def wait_until_empty(self, timeout: float = 30.0) -> bool:
        """Очікує порожньої черги з таймаутом."""
        try:
            await asyncio.wait_for(self._queue.join(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def get_queue_size(self) -> int:
        """Повертає поточний розмір черги."""
        return self._queue.qsize()

    async def get_active_workers(self) -> int:
        """Повертає кількість активних воркерів."""
        return self._sem._value  # pylint: disable=protected-access

    def get_metrics(self) -> Dict[str, float]:
        """Повертає поточні метрики продуктивності."""
        return self._metrics.collect()
