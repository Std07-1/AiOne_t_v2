"""
indicators.py: функції для розрахунку технічних індикаторів
"""

import numpy as np
import pandas as pd
from functools import lru_cache


# --- Хелпер для хешу DataFrame (для кешування) ---
def df_hash(df: pd.DataFrame) -> int:
    """
    Обчислює хеш для DataFrame (використовується для кешування індикаторів).
    """
    return pd.util.hash_pandas_object(df, index=True).sum()


# --- Кешовані обчислення індикаторів (приклад для RSI, ATR) ---
@lru_cache(maxsize=32)
def cached_rsi(close_hash: int, period: int):
    # Демонстраційна функція, кешування реалізовано нижче
    pass


@lru_cache(maxsize=32)
def cached_atr(high_hash: int, low_hash: int, close_hash: int, period: int):
    pass


def _calc_rsi(close: pd.Series, period: int) -> pd.Series:
    """
    Розрахунок індикатора RSI для цін закриття.
    """
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _calc_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int
) -> pd.Series:
    """
    Розрахунок ATR (середній істинний діапазон) з використанням EMA.
    """
    hl = high - low
    hc = (high - close.shift()).abs()
    lc = (low - close.shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    # EMA для ATR
    return tr.ewm(span=period, min_periods=1, adjust=False).mean()


def calculate_indicators(df: pd.DataFrame, custom_periods: dict = None) -> pd.DataFrame:
    """
    Розраховує всі основні технічні індикатори для переданого DataFrame з історичними даними.
    :param df: DataFrame з історичними OHLCV-даними
    :param custom_periods: dict з кастомним періодом для кожного індикатора (наприклад, {'rsi_period': 10, ...})
    :return: DataFrame з доданими колонками індикаторів
    """
    df = df.copy()
    custom_periods = custom_periods or {}
    rsi_period = custom_periods.get("rsi_period", 14)
    volume_window = custom_periods.get("volume_window", 50)
    atr_period = custom_periods.get("atr_period", 14)
    # --- Доходність та волатильність ---
    df["returns"] = df["close"].pct_change()
    df["volatility"] = df["returns"].rolling(window=20, min_periods=1).std()
    # --- RSI ---
    df["rsi"] = _calc_rsi(df["close"], rsi_period)
    # --- Z-скор об'єму ---
    volume_mean = df["volume"].rolling(window=volume_window, min_periods=1).mean()
    volume_std = df["volume"].rolling(window=volume_window, min_periods=1).std()
    df["volume_z"] = (df["volume"] - volume_mean) / volume_std
    # --- VWAP ---
    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    cum_typical = (typical_price * df["volume"]).cumsum()
    cum_volume = df["volume"].cumsum()
    df["vwap"] = cum_typical / cum_volume
    df["vwap_deviation"] = (df["close"] - df["vwap"]) / df["vwap"]
    # --- ATR ---
    df["atr"] = _calc_atr(df["high"], df["low"], df["close"], atr_period)
    # --- MACD ---
    exp12 = df["close"].ewm(span=12, min_periods=1, adjust=False).mean()
    exp26 = df["close"].ewm(span=26, min_periods=1, adjust=False).mean()
    df["macd"] = exp12 - exp26
    df["macd_signal"] = df["macd"].ewm(span=9, min_periods=1, adjust=False).mean()
    # --- Стохастик ---
    stoch_period = custom_periods.get("stoch_period", 14)
    low_min = df["low"].rolling(window=stoch_period, min_periods=1).min()
    high_max = df["high"].rolling(window=stoch_period, min_periods=1).max()
    df["stoch_k"] = 100 * (df["close"] - low_min) / (high_max - low_min)
    df["stoch_d"] = df["stoch_k"].rolling(window=3, min_periods=1).mean()
    # --- EMA ---
    df["ema20"] = df["close"].ewm(span=20, min_periods=1, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=50, min_periods=1, adjust=False).mean()
    # --- Смуги Боллінджера (Bollinger Bands) ---
    df["sma20"] = df["close"].rolling(window=20, min_periods=1).mean()
    df["std20"] = df["close"].rolling(window=20, min_periods=1).std()
    df["bollinger_upper"] = df["sma20"] + (df["std20"] * 2)
    df["bollinger_lower"] = df["sma20"] - (df["std20"] * 2)
    # --- Очищення тимчасових колонок ---
    # Не зберігаємо cum_typical, cum_volume, typical_price
    # Всі rolling/ewm використовують min_periods=1 для мінімізації NaN
    return df
