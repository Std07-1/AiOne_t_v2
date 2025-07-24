# stage2\calibration\calibration_config.py

"""calibration_config.py: Конфігурація калібрування"""

from pydantic import BaseModel
from stage2.config import ASSET_CLASS_MAPPING


class CalibrationConfig(BaseModel):
    """Конфігурація калібрування торгових стратегій"""

    min_score: float = 2.0  # Мінімальний комбінований score для відбору
    min_atr_ratio: float = 0.001  # Мінімальний ATR відносно ціни
    volatility_ratio: float = 0.7  # Відношення волатильності до ATR
    fallback_confidence: float = 0.5  # Мінімальна впевненість для fallback
    volume_z_threshold: float = 1.2  # Поріг Z-показника обсягу
    rsi_oversold: float = 30.0  # Рівень перепроданності RSI
    rsi_overbought: float = 70.0  # Рівень перекупленості RSI
    tp_mult: float = 1.8  # Множник для Take Profit
    sl_mult: float = 1.2  # Множник для Stop Loss
    min_confidence: float = 0.6  # Мінімальна впевненість сигналу
    n_trials: int = 30  # Зменшена кількість спроб оптимізації для швидкості
    lookback_days: int = 3  # 3 дні історії (~4320 барів)
    max_concurrent: int = 15  # Макс. паралельних калібрувань
    asset_class_mapping: dict = ASSET_CLASS_MAPPING
