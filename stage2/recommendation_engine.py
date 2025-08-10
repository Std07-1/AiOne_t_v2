# stage2/recommendation_engine.py

"""
Модуль для генерації торгових рекомендацій на основі ринкового контексту та впевненості.
Включає логіку визначення рекомендацій (STRONG_BUY, BUY_IN_DIPS, WAIT_FOR_CONFIRMATION, AVOID тощо)
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any

# Налаштування логування
logger = logging.getLogger("stage2.recommendation_engine")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def generate_recommendation(
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
    logger.debug("Початок генерації торгової рекомендації")
    composite = confidence["composite_confidence"]
    scenario = context["scenario"]
    stats = context.get("stats", {})

    # --- Додаємо розрахунок відстані ---
    support = context["key_levels"]["immediate_support"]
    resistance = context["key_levels"]["immediate_resistance"]
    current = context.get("current_price") or stats.get("current_price")

    if not current or current <= 0:
        return "AVOID"  # Захист від невалідних даних

    dist_to_support = (
        abs(current - support) / current * 100 if support is not None else 100
    )
    dist_to_resistance = (
        abs(current - resistance) / current * 100 if resistance is not None else 100
    )

    # Основна логіка рекомендацій
    if scenario == "MANIPULATED":
        return "AVOID"

    if scenario == "HIGH_VOLATILITY":
        return "AVOID_HIGH_RISK"

    if scenario == "BULLISH_CONTROL":
        return "BUY_IN_DIPS" if dist_to_support < 2.0 else "HOLD"

    if scenario == "BEARISH_CONTROL":
        return "SELL_ON_RALLIES" if dist_to_resistance < 2.0 else "HOLD"

    if scenario == "RANGE_BOUND":
        # Витягуємо коридорну мета-інфу (якщо є)
        km = (context or {}).get("key_levels_meta", {}) if "context" in locals() else {}
        dist_to_support = km.get("dist_to_support_pct", dist_to_support)
        dist_to_resistance = km.get("dist_to_resistance_pct", dist_to_resistance)
        band_pct = km.get("band_pct", None)
        if isinstance(band_pct, (int, float)) and band_pct < 0.08 and composite < 0.70:
            return "WAIT_FOR_CONFIRMATION"

        # Торгувати від меж:
        if dist_to_support is not None and dist_to_support < 1.0:
            return "BUY_IN_DIPS"
        if dist_to_resistance is not None and dist_to_resistance < 1.0:
            return "SELL_ON_RALLIES"

        # Посередині діапазону — будь обережний, особливо при невисокій впевненості
        mid_gap = (
            min(
                x
                for x in [dist_to_support, dist_to_resistance]
                if isinstance(x, (int, float))
            )
            if any(
                isinstance(x, (int, float))
                for x in [dist_to_support, dist_to_resistance]
            )
            else None
        )
        if (mid_gap is not None and mid_gap > 1.5) and composite < 0.70:
            return "WAIT_FOR_CONFIRMATION"

        # Якщо коридор дуже вузький (band_pct малий) — дозволяємо акуратний range trade
        if band_pct is not None and band_pct < 1.2:
            return "RANGE_TRADE"
        return "WAIT_FOR_CONFIRMATION"

    if composite > 0.8:
        if "BULLISH" in scenario:
            return "STRONG_BUY"
        if "BEARISH" in scenario:
            return "STRONG_SELL"

    if composite > 0.65:
        if "BULLISH" in scenario:
            return "BUY_IN_DIPS"
        if "BEARISH" in scenario:
            return "SELL_ON_RALLIES"
        return "RANGE_TRADE"

    if composite > 0.5:
        return "WAIT_FOR_CONFIRMATION"

    # Fallback для всіх інших випадків
    if "VOLATILITY" in scenario:
        return "AVOID_HIGH_RISK"

    return "AVOID"
