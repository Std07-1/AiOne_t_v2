# stage2\confidence_calculator.py
"""
Модуль для обробки впевненості в торгових рішеннях Stage2.
Включає розрахунок метрик впевненості на основі ринкового контексту та тригерів.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List

# Налаштування логування
logger = logging.getLogger("stage2.confidence_calculator")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _calculate_confidence_metrics(
    context: Dict[str, Any], triggers: List[str]
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
    volume_confidence = 0.3 if context.get("volume_analysis") == "CONFIRMING" else 0.1

    # Впевненість по тренду
    if "breakout" in context["scenario"]:
        trend_confidence = context["breakout_probability"]
    else:
        trend_confidence = 1 - context["breakout_probability"]

    # Композитна впевненість
    composite = tech_confidence * 0.4 + volume_confidence * 0.3 + trend_confidence * 0.3

    logger.debug(
        f"Метрики впевненості: технічна={tech_confidence:.2f}, обсяги={volume_confidence:.2f}, тренд={trend_confidence:.2f}"
    )

    return {
        "technical_confidence": round(tech_confidence, 2),
        "volume_confidence": round(volume_confidence, 2),
        "trend_confidence": round(trend_confidence, 2),
        "composite_confidence": round(composite, 2),
    }
