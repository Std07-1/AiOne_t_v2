import pandas as pd
import logging
from datetime import datetime
from typing import Optional, Any, Dict
import io
import os

logger = logging.getLogger("calibration.data")


async def load_data(fetcher, buffer, symbol, timeframe, date_from, date_to, min_bars):
    """Уніфіковане завантаження даних з RAMBuffer та fetcher з валідацією"""
    # 1. Пробуємо RAMBuffer
    df = None
    if buffer is not None:
        bars = buffer.get(symbol, timeframe, count=min_bars)
        if bars is not None and len(bars) > 0:
            df = pd.DataFrame(bars)
    # 2. Якщо недостатньо — підкачуємо через fetcher
    if df is None or len(df) < min_bars:
        missing_bars = min_bars - len(df) if df is not None else min_bars
        fetched = await fetcher.get_data(
            symbol,
            timeframe,
            from_date=date_from,
            to_date=date_to,
            limit=missing_bars,
        )
        if df is not None and fetched is not None:
            df = pd.concat([df, fetched], ignore_index=True)
        elif fetched is not None:
            df = fetched
    # 3. Валідація та типізація
    if df is None or df.empty:
        logger.error(
            f"[{symbol}] Не вдалося отримати достатньо даних для калібрування!"
        )
        return None
    try:
        df = validate_and_convert(df)
    except Exception as e:
        logger.error(f"[{symbol}] Валідація даних не пройдена: {e}")
        return None
    return df


def validate_and_convert(df: pd.DataFrame) -> pd.DataFrame:
    """Уніфікована конвертація timestamp, перевірка колонок, фільтрація невалідних значень"""
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    required = ["open", "high", "low", "close", "volume"]
    if not all(col in df.columns for col in required):
        raise ValueError("Missing required columns")
    df = df[(df["close"] > 0) & (df["volume"] >= 0)].copy()
    return df


# --- Deprecate old functions ---
# load_and_validate_data, load_historical_data, load_bars_full are now replaced by load_data
