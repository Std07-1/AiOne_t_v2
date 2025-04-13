# monitor/indicators/volatility_analysis.py

"""
Модуль для аналізу волатильності активів (monitor/indicators/volatility_analysis.py)

Реалізовано розрахунок логарифмічних доходностей, обчислення волатильності за різними періодами,
визначення стану волатильності на основі історичних даних та збереження/зчитування історичних даних
з кешу. Логування здійснюється за допомогою RichHandler для красивого кольорового відображення.
"""

import numpy as np
import pandas as pd
from typing import Dict, Any
import json
import logging
from rich.console import Console
from rich.logging import RichHandler

# Отримуємо консоль Rich
console = Console()

# Налаштування логування для модуля volatility_analysis
logger = logging.getLogger("volatility_analysis")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    rich_handler = RichHandler(console=console, level=logging.DEBUG, show_path=False)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    rich_handler.setFormatter(formatter)
    logger.addHandler(rich_handler)
logger.propagate = False


from data.cache_handler import SimpleCacheHandler  # Імпорт класу кешування


def log_returns(df: pd.DataFrame) -> pd.Series:
    """Розрахунок логарифмічних доходностей на основі колонки 'close'."""
    return np.log(df["close"] / df["close"].shift(1)).dropna()

def calculate_volatility(
    df: pd.DataFrame,
    period: int,
    annualize_factor: int = None
) -> float:
    """
    Розрахунок волатильності за останні 'period' свічок.
    
    Якщо annualize_factor заданий, множимо на sqrt(annualize_factor), що дозволяє отримати:
      - daily: period=14, annualize_factor=252
      - weekly: period=5, annualize_factor=52
      - monthly: period=21, annualize_factor=12
    """
    if df is None or df.empty or len(df) < period:
        logger.warning(f"Недостатньо даних: потрібно {period}, отримано {len(df) if df is not None else 0}")
        return 0.0

    returns = log_returns(df).tail(period)
    volatility = returns.std()
    if annualize_factor:
        volatility *= np.sqrt(annualize_factor)
    return volatility


def define_volatility_state(current_vol: float, historical_vol: pd.Series) -> str:
    """
    Визначає стан волатильності за відхиленням поточної волатильності від історичної.
    
    Повертає:
      - "very_low", "low", "normal", "high", "very_high"
      - або спеціальні рядки при відсутніх або некоректних даних.
    """
    if current_vol <= 0.0:
        return "invalid_data"
    if historical_vol.empty or len(historical_vol) < 5:
        return "initial_data_collecting"
    mean_vol = historical_vol.mean()
    if mean_vol <= 0.0:
        return "invalid_historical_data"
    deviation = (current_vol - mean_vol) / mean_vol
    if deviation < -0.5:
        return "very_low"
    elif -0.5 <= deviation < -0.2:
        return "low"
    elif -0.2 <= deviation <= 0.2:
        return "normal"
    elif 0.2 < deviation <= 0.5:
        return "high"
    else:
        return "very_high"


async def get_historical_volatility(
    symbol: str,
    cache_handler: SimpleCacheHandler
) -> Dict[str, pd.Series]:
    """
    Зчитує історичні дані волатильності (лише 3 ключі: daily, weekly, monthly)
    з кешу (symbol="historical_volatility", interval=<symbol>, prefix=None).
    Ключ буде виглядати 'historical_volatility:BTCUSDT'.
    """
    cached_data = await cache_handler.fetch_from_cache(
        symbol="historical_volatility",
        interval=symbol,
        prefix=None
    ) or {}

    historical_volatility = {}

    for key in ['daily', 'weekly', 'monthly']:
        data = cached_data.get(key, {})
        if isinstance(data, dict) and 'values' in data and 'dates' in data:
            series = pd.Series(
                data["values"],
                index=pd.to_datetime(data["dates"]),
                dtype=np.float64
            ).dropna()
        elif isinstance(data, list):
            series = pd.Series(data, dtype=np.float64).dropna()
        else:
            series = pd.Series(dtype=np.float64)

        historical_volatility[key] = series

    return historical_volatility


async def save_historical_volatility(
    symbol: str,
    volatility_data: Dict[str, pd.Series],
    cache_handler: SimpleCacheHandler
):
    """
    Зберігаємо історичні дані (daily, weekly, monthly) в Redis.
    Обрізаємо до MAX_HISTORY_LENGTH.
    """
    MAX_HISTORY_LENGTH = 365
    serializable_data = {}



    # Лише daily, weekly, monthly
    for key in ['daily', 'weekly', 'monthly']:
        series = volatility_data.get(key, pd.Series(dtype=np.float64))
        trimmed = series.tail(MAX_HISTORY_LENGTH)
        if not pd.api.types.is_datetime64_any_dtype(trimmed.index):
            trimmed.index = pd.to_datetime(trimmed.index, errors="coerce")

        serializable_data[key] = {
            "values": trimmed.values.tolist(),
            "dates": trimmed.index.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        }

    # Зберігаємо під ключем 'historical_volatility:<symbol>'
    await cache_handler.store_in_cache(
        symbol="historical_volatility",
        interval=symbol,
        data_json=json.dumps(serializable_data),
        ttl=86400*30,
        prefix=None
    )
    logger.debug(f"[{symbol}] Історичні дані (daily/weekly/monthly) волатильності збережено.")


async def analyze_volatility(
    symbol: str,
    df_daily: pd.DataFrame,
    cache_handler: SimpleCacheHandler
) -> Dict[str, Any]:
    """
    Комплексний аналіз: daily(14, ×√252), weekly(5, ×√52), monthly(21, ×√12).
    Без annualized (252) версії.

    1) Зчитуємо historical_data (daily, weekly, monthly).
    2) Якщо df_daily порожній -> повертаємо 0.
    3) Розраховуємо 3 волатильності.
    4) Оновлюємо історію, визначаємо стан (low, normal, high, ...).
    5) Зберігаємо результат.
    """
    # 1) Зчитуємо історичні
    historical_data = await get_historical_volatility(symbol, cache_handler)

    # 2) Перевірка df_daily
    if df_daily is None or df_daily.empty:
        logger.warning(f"[{symbol}] Відсутні щоденні дані для волатильності.")
        return {
            "symbol": symbol,
            "daily_volatility": 0.0,
            "weekly_volatility": 0.0,
            "monthly_volatility": 0.0,
            "volatility_state": {
                "daily": "no_data",
                "weekly": "no_data",
                "monthly": "no_data"
            },
            "timestamp": pd.Timestamp.utcnow().isoformat()
        }

    # 3) Розрахунок
    daily_vol = calculate_volatility(df_daily, 14, annualize_factor=252)  # short-term
    weekly_vol = calculate_volatility(df_daily, 5, annualize_factor=52)   # дуже коротка
    monthly_vol = calculate_volatility(df_daily, 21, annualize_factor=12) # середня

    current_time = pd.Timestamp.utcnow()

    # Якщо історія повністю пуста
    if all(hist.empty for hist in historical_data.values()):
        logger.info(f"[{symbol}] Ініціалізовано історію волатильності (daily/weekly/monthly).")
        historical_data["daily"] = pd.Series([daily_vol], index=[current_time])
        historical_data["weekly"] = pd.Series([weekly_vol], index=[current_time])
        historical_data["monthly"] = pd.Series([monthly_vol], index=[current_time])
    else:
        # Додати нове значення
        for key, vol_value in [
            ("daily", daily_vol),
            ("weekly", weekly_vol),
            ("monthly", monthly_vol)
        ]:
            old_series = historical_data[key]
            new_series = pd.Series([vol_value], index=[current_time])
            historical_data[key] = pd.concat([old_series, new_series])

    # 4) Визначаємо стани
    vol_state = {
        "daily": define_volatility_state(daily_vol, historical_data["daily"]),
        "weekly": define_volatility_state(weekly_vol, historical_data["weekly"]),
        "monthly": define_volatility_state(monthly_vol, historical_data["monthly"])
    }

    result = {
        "symbol": symbol,
        "daily_volatility": daily_vol,
        "weekly_volatility": weekly_vol,
        "monthly_volatility": monthly_vol,
        "volatility_state": vol_state,
        "timestamp": current_time.isoformat()
    }
    logger.debug(f"[{symbol}] Стани волатильності (d/w/m): {vol_state}")

    # 5) Зберігаємо історію
    await save_historical_volatility(symbol, historical_data, cache_handler)

    return result
