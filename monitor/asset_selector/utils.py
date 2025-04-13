"""
monitor/asset_selector/utils.py

Модуль з утилітними функціями, такими як перевірка торгової сесії та форматування обсягів.
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo
import logging
import pandas as pd
import re
import numpy as np
import json
from rich.console import Console
from rich.logging import RichHandler


logger = logging.getLogger("asset_selector.utils")
# Налаштовуємо обробник через RichHandler для більш красивого форматування
console = Console()
rich_handler = RichHandler(console=console, level=logging.INFO, show_path=False)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
rich_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(rich_handler)
logger.propagate = False


def is_us_session(current_time: datetime) -> bool:
    """
    Перевіряє, чи поточний час входить до робочих годин американської торгової сесії (9:30–16:00 ET).
    """
    try:
        eastern = current_time.astimezone(ZoneInfo("America/New_York"))
    except Exception as e:
        logger.error(f"Помилка конвертації часу: {e}")
        return False
    start = time(9, 30)
    end = time(16, 0)
    result = eastern.weekday() < 5 and start <= eastern.time() <= end
    logger.debug(f"[is_us_session] Поточний час (ET): {eastern.time()} — US сесія = {result}")
    return True  # або повернути result для реальної перевірки


def format_volume_usd(volume: float | str) -> str:
    """
    Форматує оборот у USD.
    Приймає як float, так і вже відформатований рядок —
    у другому випадку повертає його без змін.
    """
    if isinstance(volume, str):
        return volume

    if volume >= 1e12:
        return f"{volume/1e12:.2f}T USD"
    if volume >= 1e9:
        return f"{volume/1e9:.2f}G USD"
    if volume >= 1e6:
        return f"{volume/1e6:.2f}M USD"
    if volume >= 1e3:
        return f"{volume/1e3:.2f}K USD"
    return f"{volume:.2f} USD"



def standardize_format(df: pd.DataFrame, timezone: str = "UTC") -> pd.DataFrame:
    """
    Перетворює колонку 'timestamp' у формат datetime із заданим часовим поясом.
    Якщо дані числові, визначаємо, чи вони в секундах чи мілісекундах.
    Якщо часові мітки tz-naive, локалізуємо їх до UTC, а потім конвертуємо у заданий часовий пояс.
    
    Args:
        df (pd.DataFrame): Вхідний DataFrame із колонкою 'timestamp'.
        timezone (str): Часовий пояс (за замовчуванням "UTC").
        
    Returns:
        pd.DataFrame: DataFrame із коректно перетвореною колонкою 'timestamp'.
    """
    if "timestamp" in df.columns:
        # Якщо значення не у форматі datetime
        if not pd.api.types.is_datetime64tz_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["timestamp"] = df["timestamp"].dt.tz_convert(timezone)
        # Якщо часові мітки tz-naive, локалізуємо їх до UTC
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        # Конвертуємо до заданого часовго поясу
        df["timestamp"] = df["timestamp"].dt.tz_convert(timezone)
    return df

def dumps_safe(obj, **kwargs) -> str:
    """
    json.dumps, який вміє обробляти numpy типи.
    """
    def _conv(o):
        if isinstance(o, (np.floating, np.integer)):
            return o.item()           # перетворюємо на native float / int
        raise TypeError
    return json.dumps(obj, default=_conv, **kwargs)

def format_open_interest(oi: float) -> str:
    """
    Форматує значення Open Interest для зручного відображення.
    Якщо oi >= 1e9, повертає у мільярдах (B);
    якщо >= 1e6, повертає у мільйонах (M);
    якщо >= 1e3, повертає у тисячах (K);
    інакше повертає стандартне значення.
    """
    if oi >= 1e9:
        return f"{oi / 1e9:.2f}B"
    elif oi >= 1e6:
        return f"{oi / 1e6:.2f}M"
    elif oi >= 1e3:
        return f"{oi / 1e3:.2f}K"
    else:
        return f"{oi:.2f} USD"

def is_valid_symbol(symbol: str) -> bool:
    """
    Перевіряє валідність символу для Binance Futures.
    Символ повинен закінчуватись на 'USDT' та містити лише великі букви.
    
    Args:
        symbol (str): символ для перевірки

    Returns:
        bool: True, якщо символ валідний
    """
    return bool(re.match(r"^[A-Z0-9]+USDT$", symbol))

def get_ttl_for_interval(interval: str) -> int:
    """
    Повертає рекомендований TTL (у секундах) для заданого інтервалу.
    Можна розширювати за необхідності.

    Наприклад:
      - "1d" (daily): 24 години
      - "4h": 4 години
      - "1h": 1 година
      - "30m": 30 хвилин
    """
    mapping = {
        "1d": 24 * 3600,   # 24 години
        "4h": 4 * 3600,    # 4 години
        "1h": 3600,        # 1 година
        "30m": 1800        # 30 хвилин (приклад)
        # ... додай ще інтервали за потреби
    }
    return mapping.get(interval, 3600)  # За замовчуванням 1 година, якщо інше не вказано

def classify_volume_z(z: float) -> str:
    if np.isnan(z):
        return "no_data"
    if z >= 3:
        return "extreme_spike"
    if z >= 2:
        return "spike"
    if z >= 1:
        return "moderate"
    if z <= -2:
        return "very_low"
    return "normal"