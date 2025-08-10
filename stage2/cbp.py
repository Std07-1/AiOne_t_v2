# stage2\calculate_breakout_probability.py

"""
Розрахунок ймовірності пробою на основі статистики ринку та каліброваних ваг.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any

# Налаштування логування
logger = logging.getLogger("stage2.calculate_breakout_probability")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def calculate_breakout_probability(
    stats: Dict[str, Any], weights: Dict[str, float]
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
