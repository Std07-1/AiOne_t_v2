"""
monitor/asset_monitor/analyzers.py

Модуль для проведення аналітики активів.
Містить функції для розрахунку волатильності, VWAP, аналізу трендів,
визначення рівнів підтримки/опору та інших індикаторів.
"""
import logging
from datetime import datetime
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

logger = logging.getLogger("asset_monitor.analyzers")


def format_volume_usd(volume: float) -> str:
    """
    Форматує обсяг у USD для зручного відображення.
    """
    if volume >= 1e6:
        return f"{volume / 1e6:.2f}M USD"
    elif volume >= 1e3:
        return f"{volume / 1e3:.2f}K USD"
    else:
        return f"{volume:.2f} USD"


def determine_forecast_period(df: pd.DataFrame, volatility_threshold: float = 0.05) -> str:
    """
    Визначає прогнозований період на основі волатильності.
    """
    vol = df["close"].std() / df["close"].mean()
    forecast = "24 години" if vol > volatility_threshold else "48 години"
    logger.debug(f"Прогнозований період: {forecast} (волатильність = {vol:.4f})")
    return forecast


def calculate_volatility(df: pd.DataFrame, window: int = 14) -> float:
    """
    Обчислює відносну волатильність за останні 'window' записів.
    """
    if len(df) < window:
        logger.warning(f"Недостатньо даних для розрахунку волатильності: потрібно {window}, отримано {len(df)}")
        return 0.0
    recent = df["close"].tail(window)
    vol_value: float = recent.std() / recent.mean()
    logger.debug(f"Розрахована волатильність: {vol_value:.4f}")
    return vol_value


def determine_asset_category(df: pd.DataFrame, vol_std_threshold: float = 0.05) -> str:
    """
    Визначає категорію активу на основі волатильності.
    """
    if len(df) < 14:
        return "stable"
    vol = df["close"].std() / df["close"].mean()
    category = "high" if vol > vol_std_threshold else "stable"
    logger.debug(f"Категорія активу: {category} (волатильність = {vol:.4f})")
    return category


def choose_timeframe(category: str) -> dict:
    """
    Обирає оптимальні параметри аналізу залежно від категорії активу.
    """
    if category == "high":
        params = {"window_classic": 60, "local_prominence": 0.5, "sma_period": 20}
    else:
        params = {"window_classic": 180, "local_prominence": 1.0, "sma_period": 50}
    logger.debug(f"Обрані параметри для категорії {category}: {params}")
    return params


def normalize_asset_levels(df: pd.DataFrame, support: float, resistance: float) -> tuple:
    """
    Нормалізує рівні підтримки та опору на основі медіанного значення ціни.
    Якщо support або resistance є None, встановлює дефолтні значення на основі медіанного значення.
    
    Args:
        df (pd.DataFrame): Дані, що містять стовпець "close".
        support (float): Початкове значення рівня підтримки (може бути None).
        resistance (float): Початкове значення рівня опору (може бути None).
    
    Returns:
        tuple: Нормалізовані значення (normalized_support, normalized_resistance).
    """
    median_price = df["close"].median()
    
    # Якщо support або resistance не визначені, встановлюємо дефолтні значення
    if support is None:
        support = median_price * 1.5
    if resistance is None:
        resistance = median_price * 2

    if median_price < 1:
        normalized_support = min(support, median_price * 1.5)
        normalized_resistance = max(resistance, median_price * 2)
    elif median_price < 10:
        normalized_support = min(support, median_price * 1.2)
        normalized_resistance = max(resistance, median_price * 1.8)
    else:
        normalized_support, normalized_resistance = support, resistance

    logger.debug(f"Нормалізовані рівні: Підтримка = {normalized_support:.2f}, Опір = {normalized_resistance:.2f}")
    return normalized_support, normalized_resistance


def find_local_levels(prices: np.array, distance: int = 5, prominence: float = 1.0) -> dict:
    """
    Визначає локальні максимуми та мінімуми для побудови рівнів.
    """
    peaks, _ = find_peaks(prices, distance=distance, prominence=prominence)
    troughs, _ = find_peaks(-prices, distance=distance, prominence=prominence)
    levels = {"peaks": peaks.tolist(), "troughs": troughs.tolist()}
    logger.debug(f"Знайдено локальні рівні: {levels}")
    return levels


def determine_nearest_levels(current_price: float, local_levels: dict, prices: np.array) -> dict:
    """
    Визначає найближчі рівні підтримки та опору.
    
    Args:
        current_price (float): Поточна ціна активу.
        local_levels (dict): Словник із списками індексів локальних максимумів ("peaks")
                             та мінімумів ("troughs").
        prices (np.array): Масив значень ціни (наприклад, close-ціни).
    
    Returns:
        dict: Словник з ключами "support" та "resistance", що містять найближчі рівні.
    """
    nearest = {"support": None, "resistance": None}
    # Примусова конвертація списків індексів у ціле число
    peak_indices = np.array(local_levels.get("peaks", []), dtype=int)
    trough_indices = np.array(local_levels.get("troughs", []), dtype=int)
    
    # Отримуємо значення цін за цими індексами та відкидаємо None
    peak_prices = [float(prices[i]) for i in peak_indices if prices[i] is not None]
    trough_prices = [float(prices[i]) for i in trough_indices if prices[i] is not None]
    
    if trough_prices:
        supports = [price for price in trough_prices if price < current_price]
        if supports:
            nearest["support"] = max(supports)
    if peak_prices:
        resistances = [price for price in peak_prices if price > current_price]
        if resistances:
            nearest["resistance"] = min(resistances)
    
    logger.debug(f"Найближчі рівні для ціни {current_price}: {nearest}")
    return nearest


def calculate_volume_profile(df: pd.DataFrame, bin_size: float) -> pd.DataFrame:
    """
    Розбиває дані за ціною на біни та обчислює сумарний обсяг.
    """
    price_min = df["low"].min()
    price_max = df["high"].max()
    bins = np.arange(price_min, price_max + bin_size, bin_size)
    df = df.copy()
    df["price_bin"] = pd.cut(df["close"], bins=bins, labels=bins[:-1])
    volume_profile = df.groupby("price_bin", observed=False)["volume"].sum().reset_index()
    volume_profile["price_bin"] = volume_profile["price_bin"].astype(float)
    logger.debug(f"Розраховано volume_profile:\n{volume_profile.tail(3).to_string()}")
    return volume_profile


def calculate_vwap(df: pd.DataFrame, min_window: int = 20) -> float:
    """
    Обчислює VWAP (Volume Weighted Average Price).
    """
    if df.empty or len(df) < min_window:
        logger.warning("Недостатньо даних для розрахунку VWAP.")
        return float('nan')
    df = df.copy()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    vwap = (df["typical_price"] * df["volume"]).sum() / df["volume"].sum()
    logger.debug(f"Розраховано VWAP: {vwap:.2f}")
    return vwap


def analyze_trend_and_volume(df: pd.DataFrame, short_window: int = 5, long_window: int = 20, anomaly_multiplier: float = 2) -> pd.DataFrame:
    """
    Аналізує тренд та обсяг, додаючи індикатори до DataFrame.
    """
    df = df.copy()
    df["volume_avg"] = df["volume"].rolling(window=long_window, min_periods=1).mean()
    df["volume_std"] = df["volume"].rolling(window=long_window, min_periods=1).std()
    df["volume_anomaly"] = df["volume"] > (df["volume_avg"] + anomaly_multiplier * df["volume_std"])
    df["volume_class"] = np.select(
        [df["volume_anomaly"], df["volume"] < df["volume_avg"]],
        ["сплеск", "низький"],
        default="нормальний"
    )
    df["short_sma"] = df["close"].rolling(window=short_window, min_periods=1).mean()
    df["long_sma"] = df["close"].rolling(window=long_window, min_periods=1).mean()
    df["trend_direction"] = np.where(df["short_sma"] > df["long_sma"], "вгору", "донизу")
    df["trend_strength"] = abs(df["short_sma"] - df["long_sma"]) / df["close"]
    logger.debug(f"Підсумок аналізу тренду: {df[['trend_direction', 'trend_strength']].tail(1).to_dict(orient='records')}")
    return df


def is_volume_significant(df: pd.DataFrame, current_volume: float, threshold: float = 1.5) -> bool:
    """
    Перевіряє, чи поточний обсяг перевищує середній обсяг на заданий коефіцієнт.
    """
    avg_volume = df["volume"].mean()
    significant = current_volume >= threshold * avg_volume
    logger.debug(f"Перевірка обсягу: поточний {current_volume}, середній {avg_volume}, значний: {significant}")
    return significant


def dynamic_threshold(df: pd.DataFrame, window: int = 14) -> float:
    """
    Обчислює динамічний поріг наближення рівня на основі стандартного відхилення ціни.
    """
    if len(df) < window:
        return 2.0
    std = df["close"].rolling(window=window).std().iloc[-1]
    current_close = df["close"].iloc[-1]
    factor = 1.0 if current_close < 1 else 1.2 if current_close < 10 else 1.5
    threshold_pct = (factor * std / current_close) * 100
    logger.debug(f"Динамічний поріг: {threshold_pct:.2f}%")
    return threshold_pct
