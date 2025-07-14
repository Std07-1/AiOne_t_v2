# stage2/config.py
# Єдиний конфіг для Stage2 аналізу та калібрування

STAGE2_CONFIG = {
    "volume_z_threshold": 1.2,  # сплеск обсягу ≥1.2σ (уніфіковано)
    "rsi_oversold": 30.0,
    "rsi_overbought": 70.0,
    "stoch_oversold": 20.0,
    "stoch_overbought": 80.0,
    "macd_threshold": 0.02,
    "ema_cross_threshold": 0.005,
    "vwap_threshold": 0.001,  # 0.1% відхилення
    "min_volume_usd": 10000,
    "min_atr_percent": 0.002,
    "exclude_hours": [0, 1, 2, 3],
    "cooldown_period": 300,
    "max_correlation_threshold": 0.85,
    "tp_mult": 3.0,
    "sl_mult": 1.0,
    "min_risk_reward": 2.0,
    "entry_spread_percent": 0.05,
    "factor_weights": {
        "volume": 0.25,
        "rsi": 0.18,
        "macd": 0.20,
        "ema_cross": 0.22,
        "stochastic": 0.15,
        "orderbook": 0.20,
        "vwap": 0.25,
        "velocity": 0.18,
        "volume_profile": 0.20,
    },
    "min_cluster": 4,
    "min_confidence": 0.75,
    "context_min_correlation": 0.7,
    # залишаємо sr_window, tp_buffer_eps для SR/TP сумісності
    "sr_window": 50,
    "tp_buffer_eps": 0.0005,
}
