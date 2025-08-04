# stage2/processor.py
# -*- coding: utf-8 -*-

"""
Модуль для обробки сигналів Stage2.
Включає в себе логіку для аналізу ринкових сигналів, виявлення аномалій, розрахунку ризиків та генерації рекомендацій.

Модуль Stage2: Розширений аналіз ринкового контексту з інтеграцією калібрування

Версія 2.0
----------------
Мета:
    Забезпечити надійний, гнучкий та прозорий аналіз ринкових сигналів з урахуванням калібрування, аномалій, ризиків та формування рекомендацій для трейдингу.

Основні компоненти:
    - Stage2Processor: Клас для обробки сигналів Stage1, застосування калібрування, аналізу ринкового контексту, виявлення аномалій, розрахунку ризиків та формування рекомендацій.
    - stage2_consumer: Асинхронний пайплайн-споживач для інтеграції з чергою сигналів.

Особливості:
    - Розширене логування (DEBUG/INFO/WARNING/ERROR) для діагностики та аудиту.
    - Документовані функції та класи для полегшення підтримки та розвитку.
    - Захист від помилок, перевірки вхідних даних, fallback-логіка.

Використання:
    Див. Stage2Processor.process() та stage2_consumer для інтеграції у пайплайн.
"""

import logging
import asyncio
from rich.console import Console
from rich.logging import RichHandler
from typing import Any, Dict, Optional
from datetime import datetime
from stage2.config import STAGE2_CONFIG

from .calibration_handler import (
    get_calibrated_params,
    default_calibration,
    apply_calibration,
)

from .anomaly_detector import _detect_anomalies
from .context_analyzer import _analyze_market_context
from .risk_manager import _calculate_risk_parameters
from .confidence_calculator import _calculate_confidence_metrics
from .narrative_generator import _generate_trader_narrative
from .recommendation_engine import _generate_recommendation
from .validation import _validate_input

# Налаштування логування
logger = logging.getLogger("stage2.processor")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


class Stage2Processor:
    """
    Клас для розширеного аналізу ринкових сигналів з інтеграцією калібрування.

    Основні етапи:
        - Валідація вхідних даних
        - Отримання та застосування каліброваних параметрів
        - Аналіз ринкового контексту (волатильність, ключові рівні, сценарії)
        - Виявлення аномалій (обсяги, ліквідність, патерни)
        - Розрахунок метрик впевненості
        - Генерація трейдерського нарративу та торгової рекомендації
        - Розрахунок ризикових параметрів (TP, SL, RR)

    Вхідні параметри:
        calib_queue (CalibrationQueue): Черга калібрування
        timeframe (str): Таймфрейм даних ('1m', '5m' тощо)

    Методи:
        process(stage1_signal): Основний метод обробки сигналу Stage1
    """

    def __init__(
        self, calib_queue: Any, timeframe: str = "1m", state_manager: Any = None
    ):
        """
        Ініціалізація Stage2Processor.

        Args:
            calib_queue (CalibrationQueue): Черга калібрування.
            timeframe (str): Таймфрейм даних (наприклад, '1m', '5m').
        """
        self.config = STAGE2_CONFIG
        self.calib_queue = calib_queue
        self.timeframe = timeframe
        self._state_manager = (
            state_manager  # Менеджер стану для зберігання каліброваних параметрів
        )
        self.calib_cache: Dict[str, Dict] = {}
        self.default_calib_count = 0  # Лічильник використання дефолтних параметрів
        logger.debug(f"Stage2Processor ініціалізовано для таймфрейму {timeframe}")

    def _get_current_price(self, symbol: str) -> Optional[float]:
        """
        Метод-заглушка для отримання поточної ціни (має бути імплементовано в іншому місці).
        Args:
            symbol (str): Тікер/символ активу.
        Returns:
            float | None: Поточна ціна або None.
        """
        # У реальній системі тут буде з'єднання з ринковими даними
        return None

    async def process(self, stage1_signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Основний метод обробки сигналу Stage1 з урахуванням калібрування, аналізу, аномалій, ризиків та формування рекомендації.

        Args:
            stage1_signal (dict): Вхідний сигнал Stage1 (має містити 'stats', 'symbol', 'trigger_reasons').
        Returns:
            dict: Результат аналізу з ринковим контекстом, ризиками, впевненістю, нарративом, рекомендацією та метаданими.
        """
        logger.debug("Початок обробки сигналу Stage1")
        try:
            # Валідація вхідних даних
            _validate_input(stage1_signal["stats"])
            logger.debug("Вхідні дані пройшли валідацію")

            # Отримання каліброваних параметрів
            calibrated_params = await get_calibrated_params(
                symbol=stage1_signal["symbol"],
                timeframe=self.timeframe,
                calib_cache=self.calib_cache,
                calib_queue=self.calib_queue,
                get_current_price_func=self._get_current_price,
                config=self.config,
            )
            logger.debug(f"Отримано калібровані параметри: {calibrated_params}")

            # Обробка випадку відсутності калібрування
            if not calibrated_params:
                logger.warning("Використано дефолтні параметри калібрування")
                calibrated_params = default_calibration()

            # Застосування калібрування до сигналу
            calibrated_signal = apply_calibration(stage1_signal, calibrated_params)
            logger.debug(f"Сигнал після калібрування: {calibrated_signal}")
            stats = calibrated_signal["stats"]

            # Аналіз ринкового контексту
            market_context = _analyze_market_context(stats, calibrated_params)
            logger.debug(f"Ринковий контекст: {market_context}")
            market_context["symbol"] = stage1_signal["symbol"]  # Додаємо символ для NLP

            # Виявлення аномалій
            anomalies = _detect_anomalies(
                stats, calibrated_signal["trigger_reasons"], calibrated_params
            )
            logger.debug(f"Виявлені аномалії: {anomalies}")

            # Розрахунок метрик впевненості
            confidence = _calculate_confidence_metrics(
                market_context, calibrated_signal["trigger_reasons"]
            )
            logger.debug(f"Метрики впевненості: {confidence}")

            # Генерація нарративу
            narrative = _generate_trader_narrative(
                market_context, anomalies, calibrated_signal["trigger_reasons"]
            )

            # Логування згенерованого нарративу
            logger.info(f"Згенерований нарратив: " f"{narrative}")

            # Формування рекомендації з детальним логуванням причин
            logger.info(
                f"[Діагностика відкриття угоди] Символ: {stage1_signal.get('symbol')}, "
                f"Composite confidence: {confidence.get('composite_confidence')}, "
                f"Сценарій: {market_context.get('scenario')}, Тригери: {calibrated_signal.get('trigger_reasons')}, "
                f"Аномалії: {anomalies}, Ринковий контекст: {market_context}"
            )
            recommendation = _generate_recommendation(market_context, confidence)
            logger.info(f"Рекомендація: {recommendation}")

            # Додаткове debug-логування причин, чому не відкривається угода
            if recommendation in ("AVOID", "AVOID_HIGH_RISK", "WAIT_FOR_CONFIRMATION"):
                logger.debug(
                    f"[Причина не відкриття угоди] Символ: {stage1_signal.get('symbol')}, "
                    f"Рекомендація: {recommendation}, Composite confidence: {confidence.get('composite_confidence')}, "
                    f"Сценарій: {market_context.get('scenario')}, Тригери: {calibrated_signal.get('trigger_reasons')}, "
                    f"Аномалії: {anomalies}, Ринковий контекст: {market_context}"
                )

            # Розрахунок ризикових параметрів
            risk_params = _calculate_risk_parameters(
                stats, market_context, calibrated_params
            )
            logger.debug(f"Ризикові параметри: {risk_params}")

            # Збір метрик якості
            quality_metrics = {
                "used_default_calib": calibrated_params.get("is_default", False),
                "default_calib_count": self.default_calib_count,
                "processing_time": datetime.utcnow().isoformat(),
            }
            logger.info(
                f"Stage2Processor завершив обробку сигналу для {stage1_signal.get('symbol')}"
            )

            return {
                "symbol": calibrated_signal["symbol"],
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "market_context": market_context,
                "risk_parameters": risk_params,
                "confidence_metrics": confidence,
                "anomaly_detection": anomalies,
                "narrative": narrative,
                "recommendation": recommendation,
                "calibration_params": calibrated_params,
                "quality_metrics": quality_metrics,
                "calibration_params": calibrated_params,
            }

        except ValueError as e:
            logger.error(f"Помилка валідації: {e}")
            return {
                "error": str(e),
                "symbol": stage1_signal.get("symbol", "UNKNOWN"),
                "recommendation": "INVALID_DATA",
            }
        except Exception as e:
            logger.exception(f"Критична помилка обробки: {e}")
            return {
                "error": "SYSTEM_FAILURE",
                "symbol": stage1_signal.get("symbol", "UNKNOWN"),
                "recommendation": "AVOID",
            }


# Інтеграція з пайплайном (приклад)
async def stage2_consumer(
    input_queue: "asyncio.Queue",
    output_queue: "asyncio.Queue",
    calib_queue: Any,
    timeframe: str,
):

    # Асинхронний споживач Stage2 для обробки сигналів з Stage1.

    # Args:
    #    input_queue (asyncio.Queue): Вхідна черга сигналів Stage1.
    #    output_queue (asyncio.Queue): Вихідна черга для результатів Stage2.
    #    calib_queue (CalibrationQueue): Черга калібрування.
    #    timeframe (str): Таймфрейм даних.

    logger.info("Запуск Stage2 споживача...")
    processor = Stage2Processor(calib_queue, timeframe)

    while True:
        try:
            # Очікування сигналів з Stage1
            stage1_signals = await input_queue.get()
            logger.debug(f"Отримано {len(stage1_signals)} сигналів для обробки")

            # Обробка кожного сигналу
            stage2_results = []
            for signal in stage1_signals:
                if signal.get("signal") == "ALERT":
                    logger.debug(f"Обробка сигналу: {signal}")
                    result = await processor.process(signal)
                    stage2_results.append(result)

            # Передача результатів на подальшу обробку
            if stage2_results:
                logger.info(
                    f"Передано {len(stage2_results)} результатів у вихідну чергу"
                )
                await output_queue.put(stage2_results)

        except asyncio.CancelledError:
            logger.warning("Stage2 споживач зупинений")
            break
        except Exception as e:
            logger.error(f"Помилка в Stage2 споживачі: {e}")
            # Передача інформації про помилку
            await output_queue.put(
                {
                    "error": str(e),
                    "stage": "stage2_consumer",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
