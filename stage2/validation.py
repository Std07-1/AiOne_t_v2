# stage2.validation.py

"""Модуль для валідації вхідних даних Stage2.
Включає перевірки наявності ключів, реалістичності цін та діапазонів.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Any, Dict
import math

# Налаштування логування для stage2.validation
logger = logging.getLogger("stage2.validation")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def validate_input(stats: Dict[str, Any]):
    """
    Валідація вхідних даних з додатковими перевірками.
    Args:
        stats (dict): Статистики ринку для перевірки.
    Raises:
        ValueError: Якщо дані некоректні або відсутні ключі.
    """
    required_keys = [
        "current_price",
        "vwap",
        "atr",
        "daily_high",
        "daily_low",
        # "key_levels",
    ]

    for key in required_keys:
        if key not in stats:
            raise ValueError(f"Відсутній обов'язковий ключ: {key}")

        value = stats[key]
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            raise ValueError(f"Невірне значення для {key}: {value}")

    if stats["atr"] <= 0:
        raise ValueError("ATR має бути додатнім значенням")

    # Перевірка реалістичності ціни
    price = stats["current_price"]
    if price <= 0 or price > 1000000:
        raise ValueError(f"Нереалістична ціна: {price}")

    # Перевірка діапазону цін
    if stats["daily_high"] <= stats["daily_low"]:
        raise ValueError(
            f"Невірний діапазон цін: high={stats['daily_high']}, low={stats['daily_low']}"
        )
