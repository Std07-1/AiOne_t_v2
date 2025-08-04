# stage2/context_analyzer.py

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any
from .config import SCENARIO_MAP
from .cbp import _calculate_breakout_probability

# Налаштування логування
logger = logging.getLogger("stage2.context_analyzer")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _analyze_market_context(
    stats: Dict[str, Any], calibrated_params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Аналіз ринкового контексту з каліброваними параметрами.
    Args:
        stats (dict): Статистики ринку.
        calibrated_params (dict): Калібровані параметри.
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
    immediate_support = max(support_levels) if support_levels else stats["daily_low"]
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
    weights = calibrated_params.get("weights", {})
    breakout_prob = _calculate_breakout_probability(stats, weights)

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
