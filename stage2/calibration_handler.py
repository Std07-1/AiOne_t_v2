# stage2\calibration_handler.py
"""
Модуль для обробки калібрування параметрів Stage2.
Включає отримання каліброваних параметрів, застосування калібрування до сигналів та управління кешем калібрування.
"""

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, Callable
from datetime import datetime, timedelta

# Налаштування логування
logger = logging.getLogger("stage2.calibration_handler")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


async def get_calibrated_params(
    symbol: str,
    timeframe: str,
    calib_cache: Dict,
    calib_queue: Any,
    get_current_price_func: Callable,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Отримання каліброваних параметрів з кешу або черги калібрування.

    Args:
        symbol (str): Тікер/символ активу
        timeframe (str): Таймфрейм даних
        calib_cache (Dict): Кеш калібрування
        calib_queue (Any): Черга калібрування
        get_current_price_func (Callable): Функція отримання ціни
        config (Dict): Базова конфігурація

    Returns:
        dict: Калібровані параметри для аналізу
    """
    cache_key = f"{symbol}:{timeframe}"

    # Перевірка кешу
    if cache_key in calib_cache:
        cache_entry = calib_cache[cache_key]
        if cache_entry["expiry"] > datetime.utcnow():
            logger.debug(f"Використано кешовані параметри для {symbol}")
            return cache_entry["params"]

    # Отримання з черги калібрування
    logger.info(f"Запит каліброваних параметрів для {symbol}")
    params = await calib_queue.get_cached(symbol, timeframe)

    if not params:
        logger.warning(
            f"Калібровані параметри для {symbol} не знайдені. Використовуються дефолтні"
        )
        params = default_calibration(config)
        params["is_default"] = True

    # Оновлення кешу з можливістю примусового оновлення
    current_price = get_current_price_func(symbol)
    cache_duration = 30  # хвилин

    if current_price:
        last_price = calib_cache.get(cache_key, {}).get("last_price", 0)
        price_change = abs(current_price - last_price)

        # Примусове оновлення при зміні >3%
        if price_change > current_price * 0.03:
            logger.info(f"Значна зміна ціни ({price_change:.2f}). Оновлення кешу")
            cache_duration = 5

    # Оновлення кешу
    calib_cache[cache_key] = {
        "params": params,
        "expiry": datetime.utcnow() + timedelta(minutes=cache_duration),
        "last_price": current_price,
    }

    return params


def default_calibration(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Дефолтні калібрувальні параметри для fallback-логіки.

    Args:
        config (Dict): Базова конфігурація

    Returns:
        dict: Стандартний набір калібрувальних параметрів
    """
    return {
        **config,
        "weights": {"volume": 0.4, "rsi": 0.3, "price_position": 0.3},
        "volatility_thresholds": {"high": 0.015, "low": 0.005},
        "sensitivity_adjustments": {"volume_spike": 0.2, "rsi_oversold": 0.15},
        "fallback_tp_multipliers": [1.0, 1.5, 2.0],
        "is_default": True,
    }


def apply_calibration(signal: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Застосування каліброваних параметрів до сигналу.

    Args:
        signal (dict): Вхідний сигнал Stage1
        params (dict): Калібровані параметри

    Returns:
        dict: Сигнал з урахуванням калібрування
    """
    if not isinstance(params, dict):
        logger.error(f"Невірний тип параметрів калібрування: {type(params)}")
        return signal

    calibrated = signal.copy()
    calibrated_triggers = signal["trigger_reasons"].copy()

    # Калібрування тригерів
    for trigger in signal["trigger_reasons"]:
        if trigger in params.get("sensitivity_adjustments", {}):
            calibrated_triggers.append(f"{trigger}_calibrated")

    calibrated["trigger_reasons"] = calibrated_triggers

    # Калібрування технічних параметрів
    stats = calibrated["stats"]
    if "volatility_thresholds" in params:
        thresholds = params["volatility_thresholds"]
        atr = stats.get("atr", 0)
        daily_range = stats["daily_high"] - stats["daily_low"]

        if daily_range <= 0:
            volatility_ratio = 0
        else:
            volatility_ratio = atr / daily_range

        # Визначення профілю волатильності
        if volatility_ratio > thresholds.get("high", 0.015):
            stats["volatility_profile"] = "HIGH"
        elif volatility_ratio < thresholds.get("low", 0.005):
            stats["volatility_profile"] = "LOW"
        else:
            stats["volatility_profile"] = "MEDIUM"

    return calibrated
