# stage2/risk_manager.py
"""
Модуль для обробки ризиків Stage2 (TP, SL, RR) з урахуванням LevelSystem v2.
"""

import logging
from typing import Dict, Any
from rich.console import Console
from rich.logging import RichHandler

logger = logging.getLogger("stage2.risk_manager")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def calculate_risk_parameters(
    stats: Dict[str, Any],
    context: Dict[str, Any],
    calibrated_params: Dict[str, Any],
) -> Dict[str, Any]:
    """
    TP/SL/RR з урахуванням corridor v2 + мінімальних підлог.
    """
    price = float(stats["current_price"])
    atr = max(float(stats.get("atr", 0.001)), 1e-6)

    levels = context.get("key_levels", {}) or {}
    meta = context.get("key_levels_meta", {}) or {}

    support = levels.get("immediate_support")
    resistance = levels.get("immediate_resistance")
    next_major = levels.get("next_major_level")

    # Коридорні метадані
    band_pct = meta.get("band_pct")  # у %, може бути None
    dist_s_pct = meta.get("dist_to_support_pct")
    dist_r_pct = meta.get("dist_to_resistance_pct")
    corridor_conf = float(meta.get("confidence") or 0.5)

    # Мінімальні підлоги (у %)
    MIN_TP_PCT = float(calibrated_params.get("min_tp_pct", 0.15))
    MIN_SL_PCT = float(calibrated_params.get("min_sl_pct", 0.10))

    # Базові множники (можеш потім підкрутити в калібруванні)
    base_tp_mults = calibrated_params.get("fallback_tp_multipliers", [1.0, 1.5, 2.0])
    tp_mult = float(calibrated_params.get("tp_mult", 1.0))
    sl_mult = float(calibrated_params.get("sl_mult", 1.0))

    # 1) Визначаємо TP/SL у %:
    if isinstance(band_pct, (int, float)) and band_pct > 0:
        # TP/SL як частка від band, але з підлогами
        tp_pct = max(MIN_TP_PCT, min(3.0, 0.6 * float(band_pct)))
        sl_pct = max(MIN_SL_PCT, min(2.0, 0.4 * float(band_pct)))
    else:
        # Fallback від ATR
        tp_pct = max(MIN_TP_PCT, min(3.0, tp_mult * (atr / max(price, 1e-9)) * 100.0))
        sl_pct = max(MIN_SL_PCT, min(2.0, sl_mult * (atr / max(price, 1e-9)) * 100.0))

    # 2) Формуємо TP-мітки
    tp_targets = [price * (1 + (tp_pct / 100.0) * m) for m in base_tp_mults]

    # Якщо є next_major_level — не перелітай перший TP
    if isinstance(next_major, (int, float)) and next_major > 0:
        tp_targets = [min(tp, float(next_major)) for tp in tp_targets]

    # 3) Стоп-лос: нижче підтримки на невеликій відстані, але не смішно близько
    #    (мінімум = price*(1 - sl_pct%), також дивимось на support, якщо він є)
    sl_candidate = price * (1 - sl_pct / 100.0)
    if isinstance(support, (int, float)):
        # зсунути ще трохи під рівень (0.5 * SL у відсотках)
        pad = max(0.5 * sl_pct / 100.0 * price, 1e-9)
        sl_level = min(sl_candidate, float(support) - pad)
    else:
        sl_level = sl_candidate
    sl_level = max(sl_level, 0.0)

    # 4) Зона входу — між ціною та найближчою межею
    entry_low = min(
        price, float(support) if isinstance(support, (int, float)) else price
    )
    entry_high = max(
        price, float(resistance) if isinstance(resistance, (int, float)) else price
    )

    # 5) RR
    rr_ratio = 1.0
    if price > sl_level:
        rr_ratio = (tp_targets[0] - price) / max(price - sl_level, 1e-9)
    rr_ratio = max(0.1, min(5.0, rr_ratio))

    return {
        "optimal_entry": [round(entry_low, 6), round(entry_high, 6)],
        "tp_targets": [round(tp, 6) for tp in tp_targets],
        "sl_level": round(sl_level, 6),
        "risk_reward_ratio": round(rr_ratio, 3),
        "corridor": {
            "band_pct": round(float(band_pct or 0.0), 3),
            "confidence": round(corridor_conf, 3),
            "dist_to_support_pct": round(float(dist_s_pct or 0.0), 3),
            "dist_to_resistance_pct": round(float(dist_r_pct or 0.0), 3),
        },
    }
