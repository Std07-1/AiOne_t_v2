# stage2\narrative_generator.py
"""
Модуль для генерації трейдерського нарративу Stage2.
Включає формування текстового опису ринкової ситуації на основі контексту, аномалій та тригерів.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List, Optional
from .config_NLP import SCENARIO_MAP, TRIGGER_NAMES

# Налаштування логування
logger = logging.getLogger("stage2.narrative_generator")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _generate_trader_narrative(
    context: Dict[str, Any],
    anomalies: Dict[str, bool],
    triggers: List[str],
    stats: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Генерація трейдерського нарративу українською мовою.
    Args:
        context (dict): Ринковий контекст.
        anomalies (dict): Ознаки аномалій.
        triggers (list): Список тригерів.
        stats (dict): Додаткові статистики ринку (опціонально).
    Returns:
        str: Згенерований текстовий нарратив.
    """
    narrative = []
    symbol = context.get("symbol", "активу")

    # Отримуємо поточну ціну з контексту або статистики
    current_price = context.get("current_price")
    if not current_price and stats:
        current_price = stats.get("current_price", 0)
    current_price = current_price or 0

    # Базовий сценарій
    scenario = context["scenario"]
    scenario_text = SCENARIO_MAP.get(scenario, SCENARIO_MAP["DEFAULT"]).format(
        symbol=symbol
    )
    narrative.append(scenario_text)

    # Ключові рівні
    levels = context["key_levels"]
    support = levels.get("immediate_support")
    resistance = levels.get("immediate_resistance")

    # Перевірка наявності рівнів
    if support is None or resistance is None:
        narrative.append("Ключові рівні не визначені.")
    else:
        # Правильний розрахунок відстаней у відсотках
        dist_to_support = (
            ((current_price - support) / support * 100)
            if support and support > 0
            else 0
        )
        dist_to_resistance = (
            ((resistance - current_price) / resistance * 100)
            if resistance and resistance > 0
            else 0
        )

        # Форматування рядка з рівнями
        levels_info = [
            f"Найближчий рівень підтримки: {support:.4f}",
            (
                f"Дистанція: {abs(dist_to_support):.2f}% {'нижче' if dist_to_support > 0 else 'вище'}"
                if support
                else ""
            ),
            f"Найближчий рівень опору: {resistance:.4f}",
            (
                f"Дистанція: {abs(dist_to_resistance):.2f}% {'вище' if dist_to_resistance > 0 else 'нижче'}"
                if resistance
                else ""
            ),
        ]
        narrative.append(" ".join([item for item in levels_info if item]))

        # Додаткова інформація для RANGE_BOUND
        if scenario == "RANGE_BOUND":
            range_info = []
            # Перевірка близькості до рівнів
            if abs(dist_to_support) < 1:
                range_info.append("Ціна близько до підтримки - можливий відскок")
            elif abs(dist_to_resistance) < 1:
                range_info.append("Ціна близько до опору - можливий відбій")

            # Аналіз тривалості консолідації
            if stats:
                consolidation_days = stats.get("consolidation_days", 0)
                if consolidation_days > 3:
                    range_info.append(f"Консолідація триває {consolidation_days} днів")

                # Аналіз звуження діапазону
                range_narrowing = stats.get("range_narrowing_ratio", 0)
                if range_narrowing > 0.7:
                    range_info.append("Діапазон звужується - очікуйте пробою")

            if range_info:
                narrative.append(" | ".join(range_info))

    # Ймовірності
    breakout_prob = context.get("breakout_probability", 0)
    pullback_prob = context.get("pullback_probability", 0)

    if breakout_prob > 0.7:
        narrative.append(f"Висока ймовірність пробою (>70%) рівня {resistance:.4f}")
    elif pullback_prob > 0.6:
        narrative.append(f"Імовірний відскок (>60%) від рівня {support:.4f}")
    elif breakout_prob < 0.3 and pullback_prob < 0.3:
        narrative.append("Невизначеність: низька ймовірність як пробою, так і відскоку")

    # Тригери
    if triggers:
        readable_triggers = [
            TRIGGER_NAMES.get(t, t)
            for t in triggers
            if t in TRIGGER_NAMES or "_calibrated" in t
        ]
        if readable_triggers:
            narrative.append(f"🔔 Ключові тригери: {', '.join(readable_triggers)}")

    # Попередження про аномалії
    if anomalies.get("suspected_manipulation", False):
        narrative.append("⚠️ УВАГА: потенційні ознаки маніпуляції обсягами")
    if anomalies.get("liquidity_issues", False):
        narrative.append("⚠️ ОБЕРЕЖНО: низька ліквідність може посилити коливання")
    if anomalies.get("unusual_volume_pattern", False):
        narrative.append("⚠️ НЕЗВИЧНА АНОМАЛІЯ: підозрілий патерн обсягів")

    # Додатковий аналіз кластерів
    if stats:
        cluster_factors = stats.get("cluster_factors", [])
        if cluster_factors:
            positive_factors = sum(
                1 for f in cluster_factors if f.get("impact") == "positive"
            )
            total_factors = len(cluster_factors)
            if total_factors > 0:
                cluster_confidence = positive_factors / total_factors * 100
                narrative.append(
                    f"📊 Кластерний аналіз: {cluster_confidence:.1f}% позитивних факторів "
                    f"({positive_factors}/{total_factors})"
                )

    return "\n".join(narrative)
