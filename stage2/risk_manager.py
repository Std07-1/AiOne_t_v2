# stage2\risk_manager.py
"""
Модуль для обробки ризиків Stage2.
Включає отримання ризикових параметрів, застосування ризиків до сигналів та управління кешем ризиків.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any


# Налаштування логування
logger = logging.getLogger("stage2.risk_manager")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _calculate_risk_parameters(
    stats: Dict[str, Any],
    context: Dict[str, Any],
    calibrated_params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Розрахунок ризикових параметрів (TP, SL, RR) з fallback-логікою.
    Args:
        stats (dict): Статистики ринку.
        context (dict): Ринковий контекст.
        calibrated_params (dict): Калібровані параметри.
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
    multipliers = calibrated_params.get("fallback_tp_multipliers", [1.0, 1.5, 2.0])

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
