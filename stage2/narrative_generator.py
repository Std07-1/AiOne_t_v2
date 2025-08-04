# stage2\narrative_generator.py
"""
Модуль для генерації трейдерського нарративу Stage2.
Включає формування текстового опису ринкової ситуації на основі контексту, аномалій та тригерів.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List
from .config import SCENARIO_MAP, TRIGGER_NAMES

# Налаштування логування
logger = logging.getLogger("stage2.narrative_generator")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _generate_trader_narrative(
    context: Dict[str, Any], anomalies: Dict[str, bool], triggers: List[str]
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
