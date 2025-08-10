# stage2\narrative_generator.py
"""
Модуль для генерації трейдерського нарративу Stage2.
Включає формування текстового опису ринкової ситуації на основі контексту, аномалій та тригерів.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List, Optional
from .config_NLP import SCENARIO_MAP, NARRATIVE_BLOCKS
from .narrative_generator_utils import (
    generate_levels_block,
    generate_trigger_block,
    generate_anomaly_block,
    generate_cluster_block,
)

# Налаштування логування
logger = logging.getLogger("stage2.narrative_generator")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def generate_trader_narrative(
    context: Dict[str, Any],
    anomalies: Dict[str, bool],
    triggers: List[str],
    stats: Optional[Dict[str, Any]] = None,
    lang: str = "UA",
    style: str = "explain",
) -> str:
    """Основна функція генерації наративу"""
    narrative = []
    symbol = context.get("symbol", "активу")

    # Блок сценарію
    scenario = context.get("scenario", "DEFAULT")
    scenario_text = (
        SCENARIO_MAP.get(lang, {})
        .get(style, {})
        .get(scenario, SCENARIO_MAP["UA"]["explain"]["DEFAULT"])
        .format(symbol=symbol)
    )
    narrative.append(scenario_text)

    # Блок рівнів
    narrative.extend(generate_levels_block(context, lang, style))

    # Блок тригерів
    if trigger_block := generate_trigger_block(triggers, lang):
        narrative.append(trigger_block)

    # Блок аномалій
    narrative.extend(generate_anomaly_block(anomalies, lang))

    # Блок кластерів
    cluster_factors = stats.get("cluster_factors", []) if stats else []
    if cluster_block := generate_cluster_block(cluster_factors, lang, style):
        narrative.append(cluster_block)

    # Блок ймовірностей
    if prob := context.get("breakout_probability"):
        if prob > 0.7:
            narrative.append(NARRATIVE_BLOCKS[lang][style]["HIGH_BREAKOUT_PROB"])
        elif prob < 0.3:
            narrative.append(NARRATIVE_BLOCKS[lang][style]["LOW_BREAKOUT_PROB"])

    return "\n".join(narrative)
