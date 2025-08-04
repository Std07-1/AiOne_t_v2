# stage2/recommendation_engine.py

"""
Модуль для генерації торгових рекомендацій на основі ринкового контексту та впевненості.
Включає логіку визначення рекомендацій (STRONG_BUY, BUY_IN_DIPS, WAIT_FOR_CONFIRMATION, AVOID тощо)
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any
from .config import SCENARIO_MAP

# Налаштування логування
logger = logging.getLogger("stage2.recommendation_engine")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _generate_recommendation(
    context: Dict[str, Any], confidence: Dict[str, float]
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
