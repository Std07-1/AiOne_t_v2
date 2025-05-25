"""
signal_analyzer.py

Переписаний модуль аналізу сигналів для AiOne_t.
"""

import os
import json
import logging
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import numpy as np

# Налаштування логування
logger = logging.getLogger("signal_analyzer")
logger.setLevel(logging.INFO)

def _validate_inputs(stats: Dict[str, Any]) -> Optional[str]:
    """
    Перевірка наявності обов'язкових статистик.

    Args:
        stats: Словник з ключами stats.

    Returns:
        None, або повідомлення про помилку.
    """
    required = ["current_price", "vwap", "atr"]
    for key in required:
        val = stats.get(key)
        if val is None or (isinstance(val, (int, float)) and val == 0):
            return f"Невірні або відсутні дані: {key}"
    return None


def _generate_triggers(
    df: pd.DataFrame,
    stats: Dict[str, Any],
    config: Dict[str, Any]
) -> List[str]:
    """
    Резервне генерування тригерів, якщо вони не передані у stats.

    Args:
        df: DataFrame з OHLCV.
        stats: Словник статистик.
        config: Конфіг з порогами.

    Returns:
        Список рядків trigger_reasons.
    """
    triggers: List[str] = []
    rsi = stats.get("rsi", 0.0)
    dyn_ov = stats.get("dynamic_overbought", np.inf)
    dyn_os = stats.get("dynamic_oversold", -np.inf)
    vol_z = stats.get("volume_z", 0.0)
    cp = stats.get("current_price", 0.0)
    vwap = stats.get("vwap", 0.0)
    buff = config.get("vwap_buffer_pct", 0.0)
    vz_thr = config.get("volume_z_thresh", 1.0)

    if rsi > dyn_ov:
        triggers.append("rsi_overbought")
    if rsi < dyn_os:
        triggers.append("rsi_oversold")
    if vol_z > vz_thr:
        triggers.append("volume_spike")
    if cp > vwap * (1 + buff):
        triggers.append("vwap_breakout")
    if cp < vwap * (1 - buff):
        triggers.append("vwap_pullback")
    return triggers


def _determine_scenario(
    df: pd.DataFrame,
    stats: Dict[str, Any],
    config: Dict[str, Any]
) -> str:
    """
    Розширене визначення ринкових сценаріїв.

    Args:
        df: DataFrame з OHLCV (для тренду/волатильності).
        stats: Словник статистик.
        config: Конфіг з порогами.

    Returns:
        Один зі сценаріїв:
        "breakout", "pullback", "false_breakout",
        "neutral", "consolidation",
        "breakout_volume", "pullback_volume", "false_breakout_volume"
    """
    cp = stats["current_price"]
    vwap = stats["vwap"]
    buff = config["vwap_buffer_pct"]
    vol_z = stats.get("volume_z", 0.0)
    dyn_ov = stats.get("dynamic_overbought", np.inf)
    dyn_os = stats.get("dynamic_oversold", -np.inf)

    # Параметри для обсягу
    vz_thr = config.get("volume_z_thresh", 1.0)
    # Параметри волатильності
    volatility = stats.get("atr", 0.0) / (
        stats.get("daily_range", stats.get("atr", 1.0)) or 1.0
    )
    vol_thr = config.get("volatility_thresh", 0.2)

    # Пробій з обсягом
    if cp > vwap * (1 + buff) and vol_z > vz_thr:
        return "breakout_volume"
    # Відкат з обсягом та дивергенцією RSI
    if cp < vwap * (1 - buff) and vol_z > vz_thr and stats.get("rsi", 0) < dyn_os:
        return "pullback_volume"
    # Хибний пробій з обсягом
    if (
        vwap * (1 - buff) < cp < vwap * (1 + buff)
        and vol_z > vz_thr
        and stats.get("rsi", 0) > dyn_ov
    ):
        return "false_breakout_volume"
    # Явний пробій / відкат
    if cp > vwap * (1 + buff):
        return "breakout"
    if cp < vwap * (1 - buff):
        return "pullback"
    # Нейтральна зона
    if vwap * (1 - buff) <= cp <= vwap * (1 + buff):
        return "neutral"
    # Консолідація
    if volatility < vol_thr:
        return "consolidation"
    return "false_breakout"


def _normalize_minmax(
    value: float, min_val: float, max_val: float
) -> float:
    """
    Min-max нормалізація в діапазоні [0,1].

    Args:
        value: Вхідне значення.
        min_val: Мінімум.
        max_val: Максимум.

    Returns:
        Нормалізоване значення.
    """
    if max_val - min_val == 0:
        return 0.0
    return float(np.clip((value - min_val) / (max_val - min_val), 0.0, 1.0))


def _calculate_confidence_score(
    triggers: List[str],
    stats: Dict[str, Any],
    config: Dict[str, Any]
) -> float:
    """
    Обчислення confidence_score з вагами та нормалізацією.

    Args:
        triggers: Список trigger_reasons.
        stats: Словник статистик.
        config: Конфіг з weights.

    Returns:
        Оцінка від 0.0 до 1.0.
    """
    weights = config.get("weights", {})
    # Триггерна складова
    max_tr = config.get("max_triggers", 5)
    trig_score = min(len(triggers) / max_tr, 1.0)

    # Price movement
    mov_score = min(
        abs(stats.get("price_change", 0.0))
        / (stats.get("atr", 1.0) or 1.0),
        1.0
    )

    # Volatility score (ATR / daily_range)
    volatility_score = min(
        stats.get("atr", 0.0)
        / (stats.get("daily_range", stats.get("atr", 1.0)) or 1.0),
        1.0
    )

    # Volume Z-score normalized from [-3,3] → [0,1]
    volume_z = stats.get("volume_z", 0.0)
    volume_z_score = _normalize_minmax(volume_z, -3.0, 3.0)

    # Sharpe ratio normalized from [-3,3] → [0,1]
    sharpe = stats.get("sharpe_ratio", 0.0)
    sharpe_score = _normalize_minmax(sharpe, -3.0, 3.0)

    # Комбінована оцінка
    score = (
        weights.get("triggers", 0.0) * trig_score +
        weights.get("price_movement", 0.0) * mov_score +
        weights.get("volatility", 0.0) * volatility_score +
        weights.get("volume_z", 0.0) * volume_z_score +
        weights.get("sharpe_ratio", 0.0) * sharpe_score
    )
    return float(np.clip(score, 0.0, 1.0))


def _format_entry_zone(
    vwap: float, buffer_pct: float
) -> Tuple[float, float]:
    """
    Обчислення зони входу навколо VWAP.

    Args:
        vwap: VWAP.
        buffer_pct: Буфер у відсотках.

    Returns:
        Кортеж (нижня_ціна, верхня_ціна).
    """
    lower = vwap * (1 - buffer_pct)
    upper = vwap * (1 + buffer_pct)
    return float(np.round(lower, 8)), float(np.round(upper, 8))


def _calculate_tp_sl(
    stats: Dict[str, Any],
    config: Dict[str, Any]
) -> Tuple[float, float]:
    """
    Адаптивний розрахунок TP/SL за ATR.

    Args:
        stats: Словник статистик.
        config: Конфіг з множниками.

    Returns:
        (tp_price, sl_price)
    """
    cp = stats["current_price"]
    atr = stats["atr"]
    tp_mul = config.get("tp_multiplier", 1.5)
    sl_mul = config.get("sl_multiplier", 1.0)
    tp = cp + atr * tp_mul
    sl = cp - atr * sl_mul
    return float(np.round(tp, 8)), float(np.round(sl, 8))


async def analyze_signals(
    symbol: str,
    df: pd.DataFrame,
    stats: Dict[str, Any],
    config: Dict[str, Any],
    cache_handler: Any
) -> Dict[str, Any]:
    """
    Головна функція аналізу сигналів.

    Args:
        symbol: Тікер активу.
        df: DataFrame з ["timestamp","open","high","low","close","volume"].
        stats: Попередньо розраховані статистики.
        config: Конфіг параметрів.
        cache_handler: Екземпляр Redis-кешу з async методами.

    Returns:
        Результат аналізу:
        {
            "symbol": str,
            "signal": "long"/"short"/"flat",
            "entry_zone": (float, float),
            "confidence_score": float,
            "scenario": str,
            "tp": float,
            "sl": float,
            "timeframe": str,
            "horizon_hours": int,
            "notes": Optional[str]
        }
    """
    # Валідація мінімальної довжини
    min_bars = config.get("min_bars", 24)
    if df.shape[0] < min_bars:
        notes = f"Недостатньо барів: {df.shape[0]} (мін. {min_bars})"
        
        logger.error(notes)
        return {
            "symbol": symbol,
            "signal": "flat",
            "entry_zone": (0.0, 0.0),
            "confidence_score": 0.0,
            "scenario": "insufficient_data",
            "tp": 0.0,
            "sl": 0.0,
            "timeframe": config["timeframe"],
            "horizon_hours": config["horizon_hours"],
            "notes": notes
        }

    # Перевірка обов'язкових статистик
    err = _validate_inputs(stats)
    if err:
        logger.debug(err)
        return {
            "symbol": symbol,
            "signal": "flat",
            "entry_zone": (0.0, 0.0),
            "confidence_score": 0.0,
            "scenario": "invalid_stats",
            "tp": 0.0,
            "sl": 0.0,
            "timeframe": config["timeframe"],
            "horizon_hours": config["horizon_hours"],
            "notes": "недостатньо даних для аналізу"
        }

    # Триггери: з stats або резервні
    triggers = stats.get("trigger_reasons") or _generate_triggers(df, stats, config)

    # Визначаємо сценарій
    scenario = _determine_scenario(df, stats, config)
    logger.debug("Сценарій: %s", scenario)

    # Обчислюємо confidence_score
    confidence = _calculate_confidence_score(triggers, stats, config)
    logger.debug("confidence_score: %s", confidence)

    # Вирішуємо напрямок
    if confidence < config["confidence_threshold"]:
        signal = "flat"
    else:
        signal = "long" if "breakout" in scenario else "short"

    # Зона входу
    entry_zone = _format_entry_zone(
        vwap=stats["vwap"],
        buffer_pct=config["vwap_buffer_pct"]
    )
    logger.debug("entry_zone: %s", entry_zone)

    # TP/SL
    tp, sl = _calculate_tp_sl(stats, config)
    logger.debug("TP: %s, SL: %s", tp, sl)

    # Підготовка результату
    result: Dict[str, Any] = {
        "symbol": symbol,
        "signal": signal,
        "entry_zone": entry_zone,
        "confidence_score": confidence,
        "scenario": scenario,
        "tp": tp,
        "sl": sl,
        "timeframe": config["timeframe"],
        "horizon_hours": config["horizon_hours"],
        "notes": "; ".join(triggers) if triggers else None
    }

    # Кешування з контролем TTL
    key = f"signal:{symbol}:{config['timeframe']}"
    try:
        ttl_remain = await cache_handler.get_remaining_ttl(
            symbol=symbol,
            interval=config["timeframe"],
            prefix="signal"
        )
        if ttl_remain is None or ttl_remain < 30:
            await cache_handler.set_json(
                symbol=symbol,
                interval=config["timeframe"],
                obj=result,
                ttl=config.get("redis_ttl_sec", 300),
                prefix="signal"
            )
            logger.debug(
                "✅ Сигнал збережено в кеші: %s", key
            )
        else:
            logger.debug(
                "⏳ TTL ще залишився (%s сек), пропускаємо перезапис: %s",
                ttl_remain, key
            )
    except Exception as e:
        logger.error("❌ Помилка збереження сигналу в кеш: %s", e)

    return result
