# stage2/config.py
# Єдиний конфіг для Stage2 аналізу та калібрування

ASSET_CLASS_MAPPING = {
    "spot": [
        ".*BTC.*",
        ".*ETH.*",
        ".*XRP.*",
        ".*LTC.*",
        ".*ADA.*",
        ".*SOL.*",
        ".*DOT.*",
        ".*LINK.*",
    ],
    "futures": [
        ".*BTCUSD.*",
        ".*ETHUSD.*",
        ".*XRPUSD.*",
        ".*LTCUSD.*",
        ".*ADAUSD.*",
        ".*SOLUSD.*",
        ".*DOTUSD.*",
        ".*LINKUSD.*",
    ],
    "meme": [
        ".*DOGE.*",
        ".*SHIB.*",
        ".*PEPE.*",
        ".*FLOKI.*",
        ".*KISHU.*",
        ".*HOGE.*",
        ".*SAITAMA.*",
    ],
    "defi": [
        ".*UNI.*",
        ".*AAVE.*",
        ".*COMP.*",
        ".*MKR.*",
        ".*CRV.*",
        ".*SUSHI.*",
        ".*YFI.*",
        ".*LDO.*",
        ".*RUNE.*",
    ],
    "nft": [".*APE.*", ".*SAND.*", ".*MANA.*", ".*BLUR.*", ".*RARI.*"],
    "metaverse": [".*ENJ.*", ".*AXS.*", ".*GALA.*", ".*ILV.*", ".*HIGH.*"],
    "ai": [".*AGIX.*", ".*FET.*", ".*OCEAN.*", ".*RNDR.*", ".*AKT.*"],
    "stable": [".*USDT$", ".*BUSD$", ".*DAI$", ".*USD$", ".*FDUSD$"],
}

STAGE2_CONFIG = {
    "asset_class_mapping": ASSET_CLASS_MAPPING,
    "priority_levels": {  # Додаємо пріоритети для класів
        "futures": 1.0,  # Найвищий пріоритет для ф'ючерсів
        "spot": 0.7,  # Високий пріоритет для споту
        "meme": 0.6,  # Середній пріоритет для мем-коінів
        "defi": 0.6,  # Середній пріоритет для DeFi
        "nft": 0.5,  # Низький пріоритет для NFT
        "metaverse": 0.4,  # Низький пріоритет для метавсесвіту
        "ai": 0.3,  # Низький пріоритет для AI
        "stable": 0.2,  # Низький пріоритет для стейблкоїнів
        "default": 0.1,  # Для невизначених класів
    },
    "rsi_period": 10,  # Період RSI
    "volume_window": 30,  # Вікно обсягу для аналізу
    "atr_period": 10,  # Період ATR
    "volume_z_threshold": 1.2,  # сплеск обсягу ≥1.2σ (уніфіковано)
    "rsi_oversold": 30.0,  # Рівень перепроданності RSI
    "rsi_overbought": 70.0,  # Рівень перекупленості RSI
    "stoch_oversold": 20.0,  # Рівень перепроданності стохастика
    "stoch_overbought": 80.0,  # Рівень перекупленості стохастика
    "macd_threshold": 0.02,  # Поріг MACD для сигналів
    "ema_cross_threshold": 0.005,  # Поріг EMA перетину
    "vwap_threshold": 0.001,  # 0.1% відхилення
    "min_volume_usd": 10000,  # Мінімальний об'єм в USD
    "min_atr_percent": 0.002,  # Мінімальний ATR у відсотках
    "exclude_hours": [0, 1, 2, 3],  # Години, які виключаємо з аналізу
    "cooldown_period": 300,  # 5 хвилин охолодження між запитами
    "max_correlation_threshold": 0.85,  # Максимальний поріг кореляції
    "tp_mult": 3.0,  # Множник для Take Profit
    "sl_mult": 1.0,  # Множник для Stop Loss
    "min_risk_reward": 2.0,  # Мінімальне співвідношення ризику до винагороди
    "entry_spread_percent": 0.05,  # Відсоток спреду для входу
    "factor_weights": {
        "volume": 0.25,  # Вага обсягу
        "rsi": 0.18,  # Вага RSI
        "macd": 0.20,  # Вага MACD
        "ema_cross": 0.22,  # Вага EMA перетину
        "stochastic": 0.15,  # Вага стохастика
        "atr": 0.20,  # Вага ATR
        "orderbook": 0.20,  # Вага глибини orderbook
        "vwap": 0.25,  # Вага VWAP
        "velocity": 0.18,  # Вага швидкості
        "volume_profile": 0.20,  # Вага об'ємного профілю
    },
    "min_cluster": 4,  # Мінімальна кількість кластерів
    "min_confidence": 0.75,  # Мінімальна впевненість сигналу
    "context_min_correlation": 0.7,  # Мінімальна кореляція для контексту
    "max_concurrent": 5,  # Максимум 5 паралельних калібрувань
    # залишаємо sr_window, tp_buffer_eps для SR/TP сумісності
    "sr_window": 50,  # Вікно для SR розрахунків
    "tp_buffer_eps": 0.0005,  # Буфер для TP розрахунків
}

OPTUNA_PARAM_RANGES = {
    "volume_z_threshold": (0.5, 3.0),
    "rsi_oversold": (15.0, 40.0),
    "rsi_overbought": (60.0, 85.0),
    "tp_mult": (0.5, 5.0),
    "sl_mult": (0.5, 5.0),
    "min_confidence": (0.3, 0.9),
    "min_score": (1.0, 2.5),
    "min_atr_ratio": (0.0005, 0.01),
    "volatility_ratio": (0.5, 1.2),
    "fallback_confidence": (0.4, 0.7),
}
