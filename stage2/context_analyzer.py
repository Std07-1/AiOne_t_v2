# stage2/context_analyzer.py

import logging
from rich.console import Console
from rich.logging import RichHandler
from typing import Dict, Any, List
from .config_NLP import SCENARIO_MAP
from .cbp import calculate_breakout_probability
from .level_manager import LevelManager

# Налаштування логування
logger = logging.getLogger("stage2.context_analyzer")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def analyze_market_context(
    stats: Dict[str, Any],
    calibrated_params: Dict[str, Any],
    level_manager: LevelManager,  # Додаємо LevelManager
    trigger_reasons: List[str],  # Додано: список тригерів
    anomalies: Dict[str, bool],  # Додано: словник аномалій
) -> Dict[str, Any]:
    """
    Аналіз ринкового контексту з каліброваними параметрами.

    Формує ринковий контекст із використанням LevelSystem v2:
    - nearest support/resistance із зваженим пошуком;
    - коридор (mid, band_pct, confidence) і дистанції у відсотках;
    - м'який seed денними рівнями на холодному старті.

    Args:
        stats (dict): Статистики ринку.
        calibrated_params (dict): Калібровані параметри.
        level_manager (LevelManager): Менеджер рівнів
    Returns:
        dict: Опис ринкового контексту, ймовірності, ключові рівні, індекси.
    """
    logger.debug("Початок аналізу ринкового контексту")
    symbol = stats.get("symbol", "UNKNOWN")
    current = float(stats.get("current_price") or 0.0)
    if current <= 0:
        logger.error("Нульова/некоректна ціна, контекст не сформовано")
        return {"scenario": "INVALID_DATA", "error": "Invalid price"}

    # Коридор рівнів (v2). Якщо порожньо — буде seed на daily low/high.
    corridor = level_manager.get_corridor(
        symbol=symbol,
        price=current,
        daily_low=stats.get("daily_low"),
        daily_high=stats.get("daily_high"),
    )
    support = corridor.get("support")
    resistance = corridor.get("resistance")

    # Наступний значний рівень (другий по черзі зверху або знизу)
    all_levels = level_manager.get_all_levels(symbol) or [
        stats.get("daily_low"),
        stats.get("daily_high"),
    ]
    all_levels = sorted([lv for lv in all_levels if isinstance(lv, (int, float))])
    next_major_level = resistance
    if all_levels:
        ups = [l for l in all_levels if l > current]
        if len(ups) > 1:
            next_major_level = ups[1]
        else:
            dns = [l for l in all_levels if l < current]
            if len(dns) > 1:
                next_major_level = dns[-2]

    # Індикаторні метрики
    rsi = float(stats.get("rsi", 50.0))
    vwap = float(stats.get("vwap", current))
    atr = float(stats.get("atr", 0.0))
    daily_range = float(
        stats.get("daily_high", current) - stats.get("daily_low", current)
    )
    volatility_ratio = daily_range / max(current, 1e-9)

    # Легасі-сценарій (тільки як додаткова опора; для primary сценарію тепер QDE). :contentReference[oaicite:1]{index=1}
    scenario_legacy = "DEFAULT"
    suspected_manip = bool(anomalies.get("suspected_manipulation", False))
    if suspected_manip:
        scenario_legacy = "MANIPULATED"
    elif ("breakout_up" in trigger_reasons) or (
        stats.get("volume_z", 0) > 1.5 and resistance and current > resistance * 0.97
    ):
        scenario_legacy = "BULLISH_BREAKOUT"
    elif (
        ("rsi_divergence" in trigger_reasons or "bearish_div" in trigger_reasons)
        and rsi > 70
        and stats.get("volume_z", 0) > 1.2
    ):
        scenario_legacy = "BEARISH_REVERSAL"
    elif volatility_ratio < 0.005 and stats.get("consolidation_days", 0) > 3:
        scenario_legacy = "RANGE_BOUND"
    elif current >= vwap * 1.01 and rsi > 55:
        scenario_legacy = "BULLISH_CONTROL"
    elif current <= vwap * 0.99 and rsi < 45:
        scenario_legacy = "BEARISH_CONTROL"
    elif (atr / max(current, 1e-9)) > 0.03:
        scenario_legacy = "HIGH_VOLATILITY"
    elif daily_range < 0.005 * current:
        scenario_legacy = "RANGE_BOUND"
    else:
        if volatility_ratio > 0.02:
            scenario_legacy = "HIGH_VOLATILITY" if rsi < 60 else "BULLISH_BREAKOUT"
        else:
            if current > vwap * 1.005:
                scenario_legacy = "BULLISH_CONTROL"
            elif current < vwap * 0.995:
                scenario_legacy = "BEARISH_CONTROL"
            else:
                scenario_legacy = "RANGE_BOUND"

    # NLP-узгодження (на випадок використання текстових нарративів / SCENARIO_MAP)
    lang, style = "UA", "pro"
    if not (
        lang in SCENARIO_MAP
        and style in SCENARIO_MAP[lang]
        and scenario_legacy in SCENARIO_MAP[lang][style]
    ):
        logger.info(
            "Невідомий сценарій для %s/%s: %s — встановлено DEFAULT",
            lang,
            style,
            scenario_legacy,
        )
        scenario_legacy = "DEFAULT"

    # Ймовірність пробою (ваги калібрування, якщо надані)
    weights = calibrated_params.get("weights", {})
    breakout_prob = float(calculate_breakout_probability(stats, weights))

    # Формуємо контекст
    key_levels = {
        "immediate_support": support,
        "immediate_resistance": resistance,
        "next_major_level": next_major_level,
    }
    key_levels_meta = {
        "band_pct": corridor.get("band_pct"),
        "confidence": corridor.get("confidence"),
        "mid": corridor.get("mid"),
        "dist_to_support_pct": corridor.get("dist_to_support_pct"),
        "dist_to_resistance_pct": corridor.get("dist_to_resistance_pct"),
    }

    return {
        "current_price": current,
        "scenario": scenario_legacy,  # QDE буде primary; це — допоміжне
        "breakout_probability": breakout_prob,
        "pullback_probability": max(0.0, 1.0 - breakout_prob),
        "key_levels": key_levels,
        "key_levels_meta": key_levels_meta,
        "volume_analysis": (
            "CONFIRMING" if stats.get("volume_z", 0) > 1.5 else "DIVERGING"
        ),
        "sentiment_index": ((rsi - 50.0) / 50.0),  # -1..1 приблизно
    }
