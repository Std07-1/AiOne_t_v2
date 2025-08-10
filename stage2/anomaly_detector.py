# stage2\anomaly_detector.py
"""
Модуль для виявлення аномалій у ринкових даних Stage2.
Включає виявлення маніпуляцій, проблем з ліквідністю та незвичайних патернів обсягу.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List


# Налаштування логування
logger = logging.getLogger("stage2.anomaly_detector")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def detect_anomalies(
    stats: Dict[str, Any],
    trigger_reasons: List[str],
    calibrated_params: Dict[str, Any],
) -> Dict[str, bool]:
    """
    Розширене виявлення аномалій з каліброваними порогами.
    Args:
        stats (dict): Статистики ринку.
        trigger_reasons (list): Список тригерів.
        calibrated_params (dict): Калібровані параметри.
    Returns:
        dict: Ознаки аномалій (маніпуляції, ліквідність, патерни).
    """
    logger.debug("Виявлення аномалій у ринкових даних")
    sensitivity = calibrated_params.get("sensitivity_adjustments", {})

    # Виявлення маніпуляцій обсягами
    volume_spike_sensitivity = max(sensitivity.get("volume_spike", 1.0), 0.1)
    volume_threshold = 3.0 * volume_spike_sensitivity
    suspected_manipulation = (
        "volume_spike" in trigger_reasons
        and stats.get("volume_z", 0) > volume_threshold
    )

    # Аналіз ліквідності
    price_range = stats["daily_high"] - stats["daily_low"]
    liquidity_issues = price_range < (0.005 * stats["current_price"])

    # Незвичайні вольюм-патерни
    unusual_volume = (
        "volume_spike_calibrated" in trigger_reasons
        and stats.get("volume_z", 0) > 2.5
        and stats.get("rsi", 50) < 40
    )

    return {
        "suspected_manipulation": suspected_manipulation,
        "liquidity_issues": liquidity_issues,
        "unusual_volume_pattern": unusual_volume,
    }
