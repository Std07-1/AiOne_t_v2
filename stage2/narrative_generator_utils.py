# stage2/narrative_generator_utils.py

"""
Модуль для допоміжних функцій генерації трейдерського нарративу Stage2.
Включає форматування рівнів, тригерів, аномалій та кластерного аналізу.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List, Optional
from .config_NLP import (
    NARRATIVE_BLOCKS,
    TRIGGER_NAMES,
    ANOMALY_MAP,
    CLUSTER_TEXT,
    LEVEL_TYPE_NAMES,
)


# Налаштування логування
logger = logging.getLogger("stage2.narrative_generator_utils")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


# --- Допоміжні функції ---
def _format_level_name(level_type: str, direction: str, lang: str) -> str:
    """Форматує назву рівня з урахуванням типу та напрямку"""
    lang_map = LEVEL_TYPE_NAMES.get(lang, LEVEL_TYPE_NAMES["UA"])
    type_map = lang_map.get(level_type, lang_map.get("local", {}))
    return type_map.get(direction, "")


def generate_levels_block(context: Dict[str, Any], lang: str, style: str) -> List[str]:
    """Генерує блок інформації про ключові рівні"""
    blocks = []
    levels = context.get("key_levels", {})
    current_price = context.get("current_price", 0)

    # Функція для отримання значення рівня
    def get_level_value(level):
        if isinstance(level, dict):
            return level.get("value")
        return level

    # Отримуємо значення рівнів
    support = get_level_value(levels.get("immediate_support"))
    resistance = get_level_value(levels.get("immediate_resistance"))

    # Допоміжна функція для форматування інформації про рівень
    def format_level_info(level, level_type, direction):
        if level is None or current_price <= 0:
            return None

        distance_pct = abs(current_price - level) / current_price * 100
        level_name = _format_level_name(level_type, direction, lang)

        return NARRATIVE_BLOCKS[lang][style]["LEVEL_PROXIMITY"].format(
            current_price=current_price,
            dist=distance_pct,
            level_type=level_name,
            level=level,
        )

    # Обробка підтримки
    if support is not None:
        support_type = "local"
        if isinstance(levels.get("immediate_support"), dict):
            support_type = levels["immediate_support"].get("type", "local")

        if support_info := format_level_info(support, support_type, "support"):
            blocks.append(support_info)
            if abs(current_price - support) / current_price * 100 < 1.0:
                blocks.append(NARRATIVE_BLOCKS[lang][style]["SUPPORT_BOUNCE"])

    # Обробка опору
    if resistance is not None:
        resistance_type = "local"
        if isinstance(levels.get("immediate_resistance"), dict):
            resistance_type = levels["immediate_resistance"].get("type", "local")

        if resistance_info := format_level_info(
            resistance, resistance_type, "resistance"
        ):
            blocks.append(resistance_info)
            if abs(current_price - resistance) / current_price * 100 < 1.0:
                blocks.append(NARRATIVE_BLOCKS[lang][style]["RESISTANCE_REJECT"])

    # Аналіз діапазону
    if support and resistance:
        range_pct = abs(resistance - support) / current_price * 100

        if range_pct < 2.0:
            blocks.append(NARRATIVE_BLOCKS[lang][style]["TIGHT_RANGE"])
        elif range_pct > 10.0:
            blocks.append(NARRATIVE_BLOCKS[lang][style]["WIDE_RANGE"])

    # Додаткові аналізи для флету
    if context.get("scenario") == "RANGE_BOUND" and (stats := context.get("stats")):
        consolidation_days = stats.get("consolidation_days", 0)
        if consolidation_days > 3:
            blocks.append(
                NARRATIVE_BLOCKS[lang][style]["CONSOLIDATION"].format(
                    days=consolidation_days
                )
            )
        if narrowing := stats.get("range_narrowing_ratio", 0) > 0.7:
            blocks.append(NARRATIVE_BLOCKS[lang][style]["RANGE_NARROWING"])

    return blocks or [NARRATIVE_BLOCKS[lang][style]["NO_KEY_LEVELS"]]


def generate_trigger_block(triggers: List[str], lang: str) -> str:
    """Форматує блок тригерів"""
    if not triggers:
        return ""

    readable = []
    for t in triggers:
        base = t.replace("_calibrated", "")
        name = TRIGGER_NAMES.get(base, base)
        if "_calibrated" in t:
            name += " (калібр)" if lang == "UA" else " (calibr)"
        readable.append(name)

    header = "Ключові тригери: " if lang == "UA" else "Main triggers: "
    return header + ", ".join(readable)


def generate_anomaly_block(anomalies: Dict[str, bool], lang: str) -> List[str]:
    """Генерує блок аномалій"""
    lang_map = ANOMALY_MAP.get(lang, ANOMALY_MAP["UA"])
    return [lang_map[k] for k, v in anomalies.items() if v and k in lang_map]


def generate_cluster_block(factors: List[Dict], lang: str, style: str) -> str:
    """Форматує блок кластерного аналізу"""
    if not factors:
        return ""

    pos = sum(1 for f in factors if f.get("impact") == "positive")
    total = len(factors)
    conf = (pos / total) * 100 if total else 0

    template = CLUSTER_TEXT.get(lang, {}).get(style, "")
    if not template:
        return ""

    # Для профі версії додаємо драйвери
    if style == "pro":
        drivers = ", ".join(f["type"] for f in factors[:2])
        return template.format(
            pos=pos, neg=total - pos, total=total, conf=conf, drivers=drivers
        )

    return template.format(pos=pos, neg=total - pos, total=total, conf=conf)
