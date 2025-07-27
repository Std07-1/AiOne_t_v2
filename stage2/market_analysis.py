# stage2/market_analysis.py
# -*- coding: utf-8 -*-
"""
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
import math
import asyncio
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from rich.console import Console
from rich.logging import RichHandler
from stage2.config import STAGE2_CONFIG

# Конфігурація NLP
SCENARIO_MAP = {
    "BULLISH_BREAKOUT": "{symbol} демонструє потенціал до бичого пробою",
    "BEARISH_REVERSAL": "{symbol} показує ризики ведмежого розвороту",
    "HIGH_VOLATILITY": "{symbol} у фазі підвищеної волатильності",
    "BULLISH_CONTROL": "{symbol} під контролем покупців",
    "BEARISH_CONTROL": "{symbol} під контролем продавців",
    "RANGE_BOUND": "{symbol} торгується в боковому діапазоні",
    "DEFAULT": "Невизначений сценарій для {symbol}",
}

TRIGGER_NAMES = {
    "volume_spike": "сплеск обсягів",
    "rsi_oversold": "перепроданість RSI",
    "breakout_up": "пробій вгору",
    "vwap_deviation": "відхилення від VWAP",
    "ma_crossover": "перетин ковзних середніх",
    "volume_divergence": "розбіжність обсягів",
    "key_level_break": "пробиття ключового рівня",
}


# Налаштування логування
logger = logging.getLogger("stage2_analyzer")

logger.setLevel(logging.INFO)  # Змінено на INFO для зменшення обсягу логів
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

    def __init__(self, calib_queue: Any, timeframe: str = "1m"):
        """
        Ініціалізація Stage2Processor.

        Args:
            calib_queue (CalibrationQueue): Черга калібрування.
            timeframe (str): Таймфрейм даних (наприклад, '1m', '5m').
        """
        self.config = STAGE2_CONFIG
        self.calib_queue = calib_queue
        self.timeframe = timeframe
        self.calib_cache: Dict[str, Dict] = {}
        self.default_calib_count = 0  # Лічильник використання дефолтних параметрів
        logger.debug(f"Stage2Processor ініціалізовано для таймфрейму {timeframe}")

    async def _get_calibrated_params(self, symbol: str) -> Dict[str, Any]:
        """
        Отримання каліброваних параметрів з кешу або черги калібрування.

        Args:
            symbol (str): Тікер/символ активу.
        Returns:
            dict: Калібровані параметри для аналізу.
        """
        cache_key = f"{symbol}:{self.timeframe}"

        # Перевірка кешу
        if cache_key in self.calib_cache:
            cache_entry = self.calib_cache[cache_key]
            if cache_entry["expiry"] > datetime.utcnow():
                logger.debug(f"Використано кешовані параметри для {symbol}")
                return cache_entry["params"]

        # Отримання з черги калібрування
        logger.info(f"Запит каліброваних параметрів для {symbol}")
        params = await self.calib_queue.get_cached(symbol, self.timeframe)

        if not params:
            logger.warning(
                f"Калібровані параметри для {symbol} не знайдені. Використовуються дефолтні"
            )
            params = self._default_calibration()
            params["is_default"] = True
            self.default_calib_count += 1  # Збільшення лічильника

            # Логування частоти використання дефолтних параметрів
            if self.default_calib_count % 10 == 0:
                logger.warning(
                    f"Дефолтні параметри використані {self.default_calib_count} разів"
                )

        # Оновлення кешу з можливістю примусового оновлення
        current_price = self._get_current_price(symbol)
        cache_duration = 30  # хвилин

        if current_price:
            price_change = abs(
                current_price - self.calib_cache.get(cache_key, {}).get("last_price", 0)
            )
            if price_change > current_price * 0.03:  # Примусове оновлення при зміні >3%
                logger.info(
                    f"Значна зміна ціни ({price_change:.2f}). Оновлення кешу калібрування"
                )
                cache_duration = 5  # Коротший термін дії кешу

        # Оновлення кешу
        self.calib_cache[cache_key] = {
            "params": params,
            "expiry": datetime.utcnow() + timedelta(minutes=cache_duration),
            "last_price": current_price,
        }

        return params

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

    def _default_calibration(self) -> Dict[str, Any]:
        """
        Дефолтні калібрувальні параметри для fallback-логіки.
        Returns:
            dict: Стандартний набір калібрувальних параметрів.
        """
        return {
            **self.config,  # Використовуємо базову конфігурацію
            "weights": {"volume": 0.4, "rsi": 0.3, "price_position": 0.3},
            "volatility_thresholds": {"high": 0.015, "low": 0.005},
            "sensitivity_adjustments": {"volume_spike": 0.2, "rsi_oversold": 0.15},
            "fallback_tp_multipliers": [1.0, 1.5, 2.0],  # Множники ATR для TP
        }

    def _validate_input(self, stats: Dict[str, Any]):
        """
        Валідація вхідних даних з додатковими перевірками.
        Args:
            stats (dict): Статистики ринку для перевірки.
        Raises:
            ValueError: Якщо дані некоректні або відсутні ключі.
        """
        required_keys = [
            "current_price",
            "vwap",
            "atr",
            "daily_high",
            "daily_low",
            "key_levels",
        ]

        for key in required_keys:
            if key not in stats:
                raise ValueError(f"Відсутній обов'язковий ключ: {key}")

            value = stats[key]
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                raise ValueError(f"Невірне значення для {key}: {value}")

        # Перевірка реалістичності ціни
        price = stats["current_price"]
        if price <= 0 or price > 1000000:
            raise ValueError(f"Нереалістична ціна: {price}")

        # Перевірка діапазону цін
        if stats["daily_high"] <= stats["daily_low"]:
            raise ValueError(
                f"Невірний діапазон цін: high={stats['daily_high']}, low={stats['daily_low']}"
            )

    def _apply_calibration(
        self, signal: Dict[str, Any], params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Застосування каліброваних параметрів до сигналу.
        Args:
            signal (dict): Вхідний сигнал Stage1.
            params (dict): Калібровані параметри.
        Returns:
            dict: Сигнал з урахуванням калібрування.
        """
        calibrated = signal.copy()

        # Калібрування тригерів
        calibrated_triggers = signal["trigger_reasons"].copy()
        for trigger in signal["trigger_reasons"]:
            if trigger in params.get("sensitivity_adjustments", {}):
                calibrated_triggers.append(f"{trigger}_calibrated")

        calibrated["trigger_reasons"] = calibrated_triggers

        # Калібрування технічних параметрів
        stats = calibrated["stats"]
        if "volatility_thresholds" in params:
            thresholds = params["volatility_thresholds"]
            atr = stats.get("atr", 0)
            daily_range = stats.get("daily_high", 0) - stats.get("daily_low", 1)

            # Уникнення ділення на нуль
            if daily_range <= 0:
                volatility_ratio = 0
            else:
                volatility_ratio = atr / daily_range

            if volatility_ratio > thresholds.get("high", 0.015):
                stats["volatility_profile"] = "HIGH"
            elif volatility_ratio < thresholds.get("low", 0.005):
                stats["volatility_profile"] = "LOW"
            else:
                stats["volatility_profile"] = "MEDIUM"

        return calibrated

    def _analyze_market_context(
        self, stats: Dict[str, Any], calib_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Аналіз ринкового контексту з каліброваними параметрами.
        Args:
            stats (dict): Статистики ринку.
            calib_params (dict): Калібровані параметри.
        Returns:
            dict: Опис ринкового контексту, ймовірності, ключові рівні, індекси.
        """
        logger.debug("Початок аналізу ринкового контексту")
        # Визначення ключових рівнів з перевіркою на пустий список
        levels = sorted(stats.get("key_levels", []))
        if not levels:
            logger.warning(
                "Ключові рівні відсутні або порожні, використання денних high/low"
            )
            levels = [stats["daily_low"], stats["daily_high"]]
        if len(levels) < 2:
            logger.warning(
                "Недостатньо ключових рівнів для аналізу, використання денних high/low"
            )
            levels = [stats["daily_low"], stats["daily_high"]]
        # Перевірка наявності поточної ціни
        current = stats["current_price"]

        # Пошук найближчих рівнів підтримки та опору
        logger.debug(f"Поточна ціна: {current}, Ключові рівні: {levels}")

        support_levels = [l for l in levels if l < current]
        resistance_levels = [l for l in levels if l > current]

        # Визначення найближчих рівнів підтримки та опору
        logger.debug(f"Підтримка: {support_levels}, Опір: {resistance_levels}")
        immediate_support = (
            max(support_levels) if support_levels else stats["daily_low"]
        )
        immediate_resistance = (
            min(resistance_levels) if resistance_levels else stats["daily_high"]
        )

        # Визначення наступного значного рівня
        # Якщо немає опору, використовуємо найближчу підтримку
        next_major_level = immediate_resistance
        if len(resistance_levels) > 1:
            next_major_level = resistance_levels[1]
        elif support_levels:
            next_major_level = (
                max(support_levels[-2], immediate_resistance)
                if len(support_levels) > 1
                else immediate_resistance
            )

        # Визначення ринкового сценарію
        volatility_ratio = (stats["daily_high"] - stats["daily_low"]) / max(current, 1)
        rsi = stats.get("rsi", 50)
        logger.debug(f"Волатильність: {volatility_ratio:.4f}, RSI: {rsi}")

        # Нормалізація RSI
        if rsi < 30:
            rsi_position = 0
        elif rsi > 70:
            rsi_position = 1
        else:
            rsi_position = (rsi - 30) / (70 - 30)

        # Визначення сценарію на основі волатильності та RSI
        if volatility_ratio > 0.02:
            if rsi > 60:
                scenario = "BULLISH_BREAKOUT"
            elif rsi < 40:
                scenario = "BEARISH_REVERSAL"
            else:
                scenario = "HIGH_VOLATILITY"
        else:
            if current > stats["vwap"] * 1.005:
                scenario = "BULLISH_CONTROL"
            elif current < stats["vwap"] * 0.995:
                scenario = "BEARISH_CONTROL"
            else:
                scenario = "RANGE_BOUND"

        # Якщо сценарій не визначено, використовуємо дефолтний
        if scenario not in SCENARIO_MAP:
            logger.warning(f"Невідомий сценарій: {scenario}, використання дефолтного")
            scenario = "DEFAULT"

        # Розрахунок ймовірностей з каліброваними вагами
        weights = calib_params.get("weights", {})
        breakout_prob = self._calculate_breakout_probability(stats, weights)

        logger.debug(
            f"Ринковий сценарій: {scenario}, Ймовірність пробою: {breakout_prob:.2f}"
        )

        return {
            "scenario": scenario,
            "breakout_probability": breakout_prob,
            "pullback_probability": 1 - breakout_prob,
            "key_levels": {
                "immediate_support": immediate_support,
                "immediate_resistance": immediate_resistance,
                "next_major_level": next_major_level,
            },
            "volume_analysis": (
                "CONFIRMING" if stats.get("volume_z", 0) > 1.5 else "DIVERGING"
            ),
            "sentiment_index": rsi_position * 2 - 1,  # Шкала -1..1
        }

    def _calculate_breakout_probability(
        self, stats: Dict[str, Any], weights: Dict[str, float]
    ) -> float:
        """
        Розрахунок ймовірності пробою з каліброваними вагами.
        Args:
            stats (dict): Статистики ринку.
            weights (dict): Ваги факторів (volume, rsi, price_position).
        Returns:
            float: Ймовірність пробою (0..1).
        """
        # Нормалізація факторів
        volume_z = stats.get("volume_z", 0)
        volume_factor = (
            max(0, min(volume_z, 3.0)) / 3.0
        )  # Виправлено: обмеження [0, 3] і нормалізація
        rsi_factor = 1 - abs(stats.get("rsi", 50) - 50) / 50
        price_position = (stats["current_price"] - stats["daily_low"]) / max(
            1, stats["daily_high"] - stats["daily_low"]
        )

        # Перевірка та нормалізація ваг
        valid_weights = {
            "volume": max(weights.get("volume", 0.4), 0),
            "rsi": max(weights.get("rsi", 0.3), 0),
            "price_position": max(weights.get("price_position", 0.3), 0),
        }

        total_weight = sum(valid_weights.values())
        if total_weight == 0:
            valid_weights = {"volume": 0.4, "rsi": 0.3, "price_position": 0.3}
            total_weight = 1.0

        normalized_weights = {
            "volume": valid_weights["volume"] / total_weight,
            "rsi": valid_weights["rsi"] / total_weight,
            "price_position": valid_weights["price_position"] / total_weight,
        }

        # Розрахунок ймовірності
        probability = (
            normalized_weights["volume"] * volume_factor
            + normalized_weights["rsi"] * rsi_factor
            + normalized_weights["price_position"] * price_position
        )

        return max(0, min(1, probability))  # Обмеження діапазону 0-1

    def _detect_anomalies(
        self,
        stats: Dict[str, Any],
        trigger_reasons: List[str],
        calib_params: Dict[str, Any],
    ) -> Dict[str, bool]:
        """
        Розширене виявлення аномалій з каліброваними порогами.
        Args:
            stats (dict): Статистики ринку.
            trigger_reasons (list): Список тригерів.
            calib_params (dict): Калібровані параметри.
        Returns:
            dict: Ознаки аномалій (маніпуляції, ліквідність, патерни).
        """
        logger.debug("Виявлення аномалій у ринкових даних")
        sensitivity = calib_params.get("sensitivity_adjustments", {})

        # Виявлення маніпуляцій обсягами
        volume_spike_sensitivity = max(sensitivity.get("volume_spike", 1.0), 0.1)
        volume_threshold = 3.0 * volume_spike_sensitivity
        suspected_manipulation = (
            "volume_spike" in trigger_reasons
            and stats.get("volume_z", 0) > volume_threshold
        )

        # Аналіз ліквідності
        price_range = stats["daily_high"] - stats["daily_low"]
        liquidity_issues = price_range < (0.005 * stats["current_price"])

        # Незвичайні вольюм-патерни
        unusual_volume = (
            "volume_spike_calibrated" in trigger_reasons
            and stats.get("volume_z", 0) > 2.5
            and stats.get("rsi", 50) < 40
        )

        return {
            "suspected_manipulation": suspected_manipulation,
            "liquidity_issues": liquidity_issues,
            "unusual_volume_pattern": unusual_volume,
        }

    def _calculate_confidence_metrics(
        self, context: Dict[str, Any], triggers: List[str]
    ) -> Dict[str, float]:
        """
        Розрахунок метрик впевненості для торгового рішення.
        Args:
            context (dict): Ринковий контекст.
            triggers (list): Список тригерів.
        Returns:
            dict: Метрики впевненості (technical, volume, trend, composite).
        """
        # Технічна впевненість (кількість тригерів)
        tech_confidence = min(len(triggers) / 5.0, 1.0)

        # Впевненість по обсягам
        volume_confidence = (
            0.3 if context.get("volume_analysis") == "CONFIRMING" else 0.1
        )

        # Впевненість по тренду
        if "breakout" in context["scenario"]:
            trend_confidence = context["breakout_probability"]
        else:
            trend_confidence = 1 - context["breakout_probability"]

        # Композитна впевненість
        composite = (
            tech_confidence * 0.4 + volume_confidence * 0.3 + trend_confidence * 0.3
        )

        logger.debug(
            f"Метрики впевненості: технічна={tech_confidence:.2f}, обсяги={volume_confidence:.2f}, тренд={trend_confidence:.2f}"
        )

        return {
            "technical_confidence": round(tech_confidence, 2),
            "volume_confidence": round(volume_confidence, 2),
            "trend_confidence": round(trend_confidence, 2),
            "composite_confidence": round(composite, 2),
        }

    def _generate_trader_narrative(
        self, context: Dict[str, Any], anomalies: Dict[str, bool], triggers: List[str]
    ) -> str:
        """
        Генерація трейдерського нарративу українською мовою.
        Args:
            context (dict): Ринковий контекст.
            anomalies (dict): Ознаки аномалій.
            triggers (list): Список тригерів.
        Returns:
            str: Згенерований текстовий нарратив.
        """
        narrative = []
        symbol = context.get("symbol", "активу")

        # Базовий сценарій
        scenario = context["scenario"]
        scenario_text = SCENARIO_MAP.get(scenario, SCENARIO_MAP["DEFAULT"]).format(
            symbol=symbol
        )
        narrative.append(scenario_text)

        # Ключові рівні
        levels = context["key_levels"]
        narrative.append(
            f"Найближчий рівень підтримки: {levels['immediate_support']:.2f}, "
            f"опору: {levels['immediate_resistance']:.2f}"
        )

        # Ймовірності
        breakout_prob = context["breakout_probability"]
        pullback_prob = context["pullback_probability"]

        if breakout_prob > 0.7:
            narrative.append(
                f"Висока ймовірність пробою рівня {levels['immediate_resistance']:.2f} "
                f"({breakout_prob*100:.1f}%)"
            )
        elif pullback_prob > 0.6:
            narrative.append(
                f"Імовірний відскок від рівня {levels['immediate_support']:.2f} "
                f"({pullback_prob*100:.1f}%)"
            )

        # Тригери
        if triggers:
            readable_triggers = [
                TRIGGER_NAMES.get(t, t)
                for t in triggers
                if t in TRIGGER_NAMES or "_calibrated" in t
            ]
            if readable_triggers:
                narrative.append(f"Ключові тригери: {', '.join(readable_triggers)}")

        # Попередження про аномалії
        if anomalies["suspected_manipulation"]:
            narrative.append("Увага: потенційні ознаки маніпуляції обсягами")
        if anomalies["liquidity_issues"]:
            narrative.append("Обережно: низька ліквідність може посилити коливання")

        return ". ".join(narrative) + "."

    def _generate_recommendation(
        self, context: Dict[str, Any], confidence: Dict[str, float]
    ) -> str:
        """
        Генерація торгової рекомендації на основі сценарію та впевненості.
        Args:
            context (dict): Ринковий контекст.
            confidence (dict): Метрики впевненості.
        Returns:
            str: Рекомендація (STRONG_BUY, WAIT, AVOID тощо).
        """
        composite = confidence["composite_confidence"]
        scenario = context["scenario"]

        if composite > 0.8:
            if "BULLISH" in scenario:
                return "STRONG_BUY"
            elif "BEARISH" in scenario:
                return "STRONG_SELL"

        elif composite > 0.65:
            if "BULLISH" in scenario:
                return "BUY_IN_DIPS"
            elif "BEARISH" in scenario:
                return "SELL_ON_RALLIES"
            else:
                return "RANGE_TRADE"

        elif composite > 0.5:
            return "WAIT_FOR_CONFIRMATION"

        else:
            if "VOLATILITY" in scenario:
                return "AVOID_HIGH_RISK"
            return "AVOID"

    def _calculate_risk_parameters(
        self,
        stats: Dict[str, Any],
        context: Dict[str, Any],
        calib_params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Розрахунок ризикових параметрів (TP, SL, RR) з fallback-логікою.
        Args:
            stats (dict): Статистики ринку.
            context (dict): Ринковий контекст.
            calib_params (dict): Калібровані параметри.
        Returns:
            dict: Оптимальні рівні входу, TP, SL, RR.
        """
        price = stats["current_price"]
        atr = max(stats["atr"], 0.001)  # Захист від нульового ATR
        levels = context["key_levels"]

        # Оптимальна зона входу
        entry_low = min(price, levels["immediate_support"])
        entry_high = max(price, levels["immediate_resistance"])

        # Цілі тейк-профіту (з fallback)
        tp_targets = []
        multipliers = calib_params.get("fallback_tp_multipliers", [1.0, 1.5, 2.0])

        for multiplier in multipliers:
            target = price + atr * multiplier
            # Перевірка на наступний значний рівень
            if target < levels.get("next_major_level", float("inf")):
                tp_targets.append(target)

        # Якщо не знайшли цілей, використовуємо множники без обмежень
        if not tp_targets:
            tp_targets = [price + atr * m for m in multipliers]
            logger.warning("Використано резервні цілі TP через відсутність рівнів")

        # Стоп-лос
        sl_level = max(levels["immediate_support"] - atr * 0.5, stats["daily_low"])

        # Risk/Reward Ratio
        if price > sl_level:
            rr_ratio = (tp_targets[0] - price) / (price - sl_level)
        else:
            rr_ratio = 1.0  # Захист від некоректних значень

        logger.debug(
            f"Ризикові параметри: TP={tp_targets[0]:.2f}, SL={sl_level:.2f}, RR={rr_ratio:.2f}"
        )

        return {
            "optimal_entry": [round(entry_low, 2), round(entry_high, 2)],
            "tp_targets": [round(tp, 2) for tp in tp_targets],
            "sl_level": round(sl_level, 2),
            "risk_reward_ratio": round(rr_ratio, 2),
        }

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
            self._validate_input(stage1_signal["stats"])
            logger.debug("Вхідні дані пройшли валідацію")

            # Отримання каліброваних параметрів
            calib_params = await self._get_calibrated_params(stage1_signal["symbol"])
            logger.debug(f"Отримано калібровані параметри: {calib_params}")

            # Застосування калібрування до сигналу
            calibrated_signal = self._apply_calibration(stage1_signal, calib_params)
            logger.debug(f"Сигнал після калібрування: {calibrated_signal}")
            stats = calibrated_signal["stats"]

            # Аналіз ринкового контексту
            market_context = self._analyze_market_context(stats, calib_params)
            logger.debug(f"Ринковий контекст: {market_context}")
            market_context["symbol"] = stage1_signal["symbol"]  # Додаємо символ для NLP

            # Виявлення аномалій
            anomalies = self._detect_anomalies(
                stats, calibrated_signal["trigger_reasons"], calib_params
            )
            logger.debug(f"Виявлені аномалії: {anomalies}")

            # Розрахунок метрик впевненості
            confidence = self._calculate_confidence_metrics(
                market_context, calibrated_signal["trigger_reasons"]
            )
            logger.debug(f"Метрики впевненості: {confidence}")

            # Генерація нарративу
            narrative = self._generate_trader_narrative(
                market_context, anomalies, calibrated_signal["trigger_reasons"]
            )

            # Логування згенерованого нарративу
            logger.info(f"Згенерований нарратив: " f"{narrative}")

            # Формування рекомендації
            recommendation = self._generate_recommendation(market_context, confidence)
            logger.info(f"Рекомендація: {recommendation}")

            # Розрахунок ризикових параметрів
            risk_params = self._calculate_risk_parameters(
                stats, market_context, calib_params
            )
            logger.debug(f"Ризикові параметри: {risk_params}")

            # Збір метрик якості
            quality_metrics = {
                "used_default_calib": calib_params.get("is_default", False),
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
                "calibration_params": calib_params,
                "quality_metrics": quality_metrics,
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
