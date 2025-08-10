# stage2/confidence_calculator.py
"""
Модуль для обробки впевненості в торгових рішеннях Stage2.
Включає розрахунок метрик впевненості на основі ринкового контексту та тригерів.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List, Iterable, Union
from rich.console import Console
from rich.logging import RichHandler

# Налаштування логування
logger = logging.getLogger("stage2.confidence_calculator")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


# ──────────────────────────────────────────────────────────────────────────────
# Хелпери
# ──────────────────────────────────────────────────────────────────────────────
def _clamp01(x: Union[float, int]) -> float:
    try:
        x = float(x)
    except Exception:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _as_trigger_list(triggers: Iterable[Any]) -> List[str]:
    """
    Приводить будь-який формат "списку причин" до списку рядків.
    Дозволяє як ["volume_spike", ...], так і [{"code":"volume_spike", ...}, ...].
    """
    out: List[str] = []
    if not triggers:
        return out
    for t in triggers:
        if isinstance(t, str):
            out.append(t)
        elif isinstance(t, dict):
            # найчастіше ключі: code / name / reason
            val = t.get("code") or t.get("name") or t.get("reason")
            if isinstance(val, str):
                out.append(val)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Основна функція
# ──────────────────────────────────────────────────────────────────────────────
def calculate_confidence_metrics(
    context: Dict[str, Any], triggers: List[str]
) -> Dict[str, float]:
    """
    Розрахунок метрик впевненості (technical, volume, trend, composite).

    Args:
        context: Ринковий контекст (очікуються ключі:
            - scenario (str),
            - breakout_probability (0..1) [може бути відсутній],
            - pullback_probability (0..1) [може бути відсутній],
            - key_levels_meta: {band_pct (%, може бути None), confidence (0..1), ...},
            - volume_analysis: "CONFIRMING" | "DIVERGING" | None,
            - stats (опц.).
        triggers: Список тригерів (рядки або словники).

    Returns:
        dict: {
            "technical_confidence": float,
            "volume_confidence": float,
            "trend_confidence": float,
            "composite_confidence": float
        }
    """

    # --- 0) Безпечні зчитування з контексту
    scenario = str(context.get("scenario", "UNCERTAIN")).upper()
    km = context.get("key_levels_meta", {}) or {}
    band_pct = km.get("band_pct")  # у %, може бути None
    corridor_conf = _clamp01(km.get("confidence", 0.5))

    # QDE/евристики: ці поля можуть бути відсутні — тому .get + clamp
    breakout_p = _clamp01(
        context.get(
            "breakout_probability",
            context.get("decision_matrix", {}).get("BULLISH_BREAKOUT", 0.3),
        )
    )
    pullback_p = _clamp01(
        context.get(
            "pullback_probability",
            context.get("decision_matrix", {}).get("BEARISH_REVERSAL", 0.3),
        )
    )

    # --- 1) Технічна впевненість (за тригерами)
    trig_list = _as_trigger_list(triggers)
    # легкі ваги для деяких ключових тригерів; решта — базова вага
    WEIGHTS = {
        "volume_spike": 1.0,
        "breakout_up": 1.0,
        "breakout_down": 1.0,
        "rsi_divergence": 0.8,
        "low_volatility": 0.4,
        "near_high": 0.5,
        "near_low": 0.5,
        "low_atr": 0.4,
        "high_atr": 0.7,
    }
    base_w = 0.4
    score = 0.0
    for t in trig_list:
        score += WEIGHTS.get(t, base_w)
    # нормування (5 тригерів "середньої" сили ≈ 1.0)
    tech_confidence = max(0.0, min(1.0, score / 5.0))

    # --- 2) Впевненість по обсягах
    vol_analysis = str(context.get("volume_analysis", "")).upper()
    # Базові рівні, з невеликою підпіркою на "CONFIRMING"
    if vol_analysis == "CONFIRMING":
        volume_confidence = 0.6
    elif vol_analysis == "DIVERGING":
        volume_confidence = 0.2
    else:
        volume_confidence = 0.3

    # --- 3) Трендова впевненість
    # Усереднена логіка:
    # - Для BREAKOUT-сценарію головну роль грає breakout_p.
    # - Для REVERSAL — pullback_p.
    # - Для RANGE — mean-reversion: 1 - breakout_p; підсилюємо коридорною впевненістю.
    # Накладаємо corridor_conf як мультиплікатор 0.5..1.0.
    if "BREAKOUT" in scenario:
        trend_base = breakout_p
        # вузький коридор (малий band) — частіше прориви; якщо band невідомий, пропускаємо
        if isinstance(band_pct, (int, float)):
            # 0..2% → 1..0 за лінійною шкалою; беремо половину впливу
            band_factor = 1.0 - max(0.0, min(2.0, float(band_pct))) / 2.0
            trend_base *= 0.75 + 0.25 * band_factor  # м’яко
    elif "REVERSAL" in scenario:
        trend_base = pullback_p
    elif "RANGE" in scenario:
        trend_base = 1.0 - breakout_p  # mean-reversion
    else:
        # UNCERTAIN/MANIPULATED/інші
        trend_base = 0.5 * (1.0 - breakout_p) + 0.5 * pullback_p

    # Підсаджуємо коридорну впевненість (0.5..1.0)
    trend_confidence = _clamp01(trend_base * (0.5 + 0.5 * corridor_conf))

    # --- 4) Композит
    # зберігаємо твої ваги, але можна винести у конфіг при бажанні
    composite = 0.4 * tech_confidence + 0.3 * volume_confidence + 0.3 * trend_confidence

    logger.debug(
        "Confidence: tech=%.3f, volume=%.3f, trend=%.3f, composite=%.3f | scen=%s bp=%.3f pp=%.3f band=%s conf=%.3f",
        tech_confidence,
        volume_confidence,
        trend_confidence,
        composite,
        scenario,
        breakout_p,
        pullback_p,
        f"{band_pct:.3f}%" if isinstance(band_pct, (int, float)) else "NA",
        corridor_conf,
    )

    return {
        "technical_confidence": round(float(tech_confidence), 2),
        "volume_confidence": round(float(volume_confidence), 2),
        "trend_confidence": round(float(trend_confidence), 2),
        "composite_confidence": round(float(composite), 2),
    }
