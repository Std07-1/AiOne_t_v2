import pandas as pd
import logging

from rich.console import Console
from rich.logging import RichHandler

# Налаштування логування
logger = logging.getLogger("calibration.data")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False  # ← Критично важливо!


async def load_data(fetcher, buffer, symbol, timeframe, date_from, date_to, min_bars):
    """
    Уніфіковане асинхронне завантаження даних для калібрування.
    1. Пробує отримати дані з RAMBuffer.
    2. Якщо недостатньо — підкачує через fetcher.
    3. Об'єднує, валідує та повертає DataFrame.

    Args:
        fetcher: об'єкт для асинхронного отримання даних (має get_data_for_calibration)
        buffer: RAMBuffer для швидкого доступу
        symbol: символ актива
        timeframe: таймфрейм
        date_from: початкова дата
        date_to: кінцева дата
        min_bars: мінімальна кількість барів

    Returns:
        pd.DataFrame або None, якщо даних недостатньо чи невалідні
    """
    logger.debug(f"[load_data] Старт для {symbol}/{timeframe}, min_bars={min_bars}")
    df = None
    # 1. Пробуємо RAMBuffer
    if buffer is not None:
        logger.debug(f"[load_data] Спроба отримати з RAMBuffer: {symbol}/{timeframe}")
        bars = buffer.get(symbol, timeframe, count=min_bars)
        if bars is not None and len(bars) > 0:
            logger.debug(
                f"[load_data] Отримано {len(bars)} барів з RAMBuffer для {symbol}/{timeframe}"
            )
            df = pd.DataFrame(bars)
        else:
            logger.debug(
                f"[load_data] RAMBuffer порожній або недостатньо барів для {symbol}/{timeframe}"
            )
    # 2. Якщо недостатньо — підкачуємо через fetcher
    if df is None or len(df) < min_bars:
        missing_bars = min_bars - len(df) if df is not None else min_bars
        logger.debug(
            f"[load_data] Потрібно дозавантажити {missing_bars} барів через fetcher для {symbol}/{timeframe}"
        )
        start_ms = int(date_from.timestamp() * 1000) if date_from else None
        end_ms = int(date_to.timestamp() * 1000) if date_to else None
        try:
            fetched = await fetcher.get_data_for_calibration(
                symbol,
                timeframe,
                start_time=start_ms,
                end_time=end_ms,
                limit=missing_bars,
            )
        except Exception as e:
            logger.error(f"[{symbol}] Помилка отримання даних через fetcher: {e}")
            return None
        if fetched is not None and isinstance(fetched, pd.DataFrame):
            logger.debug(
                f"[load_data] Отримано {len(fetched)} барів через fetcher для {symbol}/{timeframe}"
            )
            fetched = fetched.reset_index(drop=True)
        else:
            logger.error(f"[{symbol}] Не вдалося отримати дані через fetcher!")
            return None
        # 3. Об'єднуємо з RAMBuffer, якщо є
        if df is not None and not df.empty:
            logger.debug(
                f"[load_data] Об'єднання RAMBuffer та fetcher для {symbol}/{timeframe}"
            )
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
        logger.debug(
            f"[load_data] Валідація та конвертація даних для {symbol}/{timeframe}"
        )
        df = validate_and_convert(df)
    except Exception as e:
        logger.error(f"[{symbol}] Валідація даних не пройдена: {e}")
        return None
    logger.debug(f"[load_data] Дані для {symbol}/{timeframe} готові, shape={df.shape}")
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
