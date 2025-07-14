from typing import Dict, Any


def unify_stage2_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Гарантує, що словник містить всі уніфіковані ключі Stage2.
    Якщо є лише atr_multiplier — присвоює tp_mult і sl_mult.
    Додає дефолтні значення для відсутніх ключів.
    """
    unified_keys = {
        "volume_z_threshold": 2.0,
        "tp_mult": 2.0,
        "sl_mult": 1.5,
        "min_confidence": 0.7,
        "rsi_oversold": 25.0,
        "rsi_overbought": 75.0,
        "vwap_threshold": 0.005,
        "macd_threshold": 0.02,
        "stoch_oversold": 20.0,
        "stoch_overbought": 80.0,
    }
    if "atr_multiplier" in params:
        params["tp_mult"] = params["sl_mult"] = params["atr_multiplier"]
    result = {}
    for k, v in unified_keys.items():
        result[k] = float(params[k]) if k in params else v
    return result
