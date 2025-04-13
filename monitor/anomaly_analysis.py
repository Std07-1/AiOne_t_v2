"""
anomaly_analysis.py

Модуль для виявлення та аналізу аномалій у даних ринку.
Включає:
  - detect_anomalies_combined: виявлення аномалій за допомогою комбінації Z‑Score та MAD.
  - analyze_anomalies_basic: базовий аналіз змін "до" і "після" аномалій.
  - detect_preparatory_phase: перевірка ознак підготовчої фази перед аномалією.
  - deep_anomaly_analysis: глибокий аналіз трендів навколо аномалій із класифікацією та перевіркою contraction.
"""

import logging
import numpy as np
import pandas as pd
import asyncio
from typing import Any

logger = logging.getLogger(__name__)


def detect_anomalies_combined(df: pd.DataFrame, symbol: str, column: str = "close",
                              z_threshold: float = 2.5, mad_multiplier: float = 2.7) -> pd.DataFrame:
    """
    Виявляє аномалії за допомогою комбінації Z-Score та MAD.
    Обчислює Z-Score та MAD для колонки (за замовчуванням 'close') і повертає об'єднаний DataFrame аномалій.
    """
    try:
        # Розрахунок Z-Score
        mean_val = df[column].mean()
        std_val = df[column].std() or 1e-9
        df["z_score"] = (df[column] - mean_val) / std_val
        anomalies_z = df[df["z_score"].abs() > z_threshold]

        # Розрахунок MAD
        median_val = df[column].median()
        mad_val = np.median(np.abs(df[column] - median_val))
        mad_threshold = 1.4826 * mad_val
        lower_bound = median_val - mad_multiplier * mad_threshold
        upper_bound = median_val + mad_multiplier * mad_threshold
        anomalies_mad = df[(df[column] < lower_bound) | (df[column] > upper_bound)]

        # Об'єднуємо результати та видаляємо дублікати за timestamp
        anomalies_combined = pd.concat([anomalies_z, anomalies_mad]).drop_duplicates(subset=["timestamp"])
        logger.debug(f"[{symbol}] Виявлено {len(anomalies_combined)} аномалій (комбіновано).")
        return anomalies_combined
    except Exception as e:
        logger.error(f"[{symbol}] Помилка при виявленні комбінованих аномалій: {e}", exc_info=True)
        return pd.DataFrame()


def analyze_anomalies_basic(df_anomalies: pd.DataFrame, symbol: str) -> None:
    """
    Базовий аналіз "до" і "після" виявлених аномалій:
    обчислює процентну зміну ціни та обсягу для записів перед і після аномалії.
    """
    if df_anomalies.empty:
        logger.info(f"[{symbol}] Аномалій для базового аналізу немає.")
        return

    df_anomalies["price_change_before"] = df_anomalies["close"].pct_change(periods=-1)
    df_anomalies["price_change_after"] = df_anomalies["close"].pct_change(periods=1)
    df_anomalies["volume_change_before"] = df_anomalies["volume"].pct_change(periods=-1)
    df_anomalies["volume_change_after"] = df_anomalies["volume"].pct_change(periods=1)

    logger.debug(
        f"[{symbol}] Базовий аналіз аномалій:\n" +
        df_anomalies[['timestamp', 'close', 'price_change_before', 'price_change_after', 
                      'volume_change_before', 'volume_change_after']].to_string(index=False)
    )


def detect_preparatory_phase(df: pd.DataFrame, symbol: str, anomaly_time: pd.Timestamp,
                              lookback_hours: int = 12, vol_multiplier: float = 1.5,
                              rsi_threshold: float = 60, volume_threshold: float = 1.2) -> bool:
    """
    Перевіряє, чи є ознаки підготовчої фази за 'lookback_hours' до anomaly_time.
    Перевіряє зростання волатильності, RSI та обсягів.
    Повертає True, якщо підготовча фаза ймовірна.
    """
    start_time = anomaly_time - pd.Timedelta(hours=lookback_hours)
    window_df = df[(df["timestamp"] >= start_time) & (df["timestamp"] < anomaly_time)]
    if len(window_df) < 2:
        return False

    # Якщо колонка "volatility_abs" відсутня, розраховуємо її як різницю high-low
    if "volatility_abs" not in window_df.columns:
        window_df = window_df.assign(volatility_abs=window_df["high"] - window_df["low"])
    avg_volatility = window_df["volatility_abs"].mean()
    avg_volume = window_df["volume"].mean()
    last_rsi = window_df["rsi"].iloc[-1] if "rsi" in window_df.columns else 50

    # Глобальні середні для порівняння
    global_vol_avg = df["volatility_abs"].mean() if "volatility_abs" in df.columns else 0
    global_volume_avg = df["volume"].mean()

    if (avg_volatility > global_vol_avg * vol_multiplier) or \
       (avg_volume > global_volume_avg * volume_threshold) or \
       (last_rsi > rsi_threshold):
        logger.debug(f"[{symbol}] Виявлено ознаки підготовчої фази для аномалії @ {anomaly_time}.")
        return True
    return False


async def deep_anomaly_analysis(df: pd.DataFrame, df_anomalies: pd.DataFrame, symbol: str,
                                lookback_days: int = 1, lookforward_days: int = 3,
                                lookback_pre_hours: int = 12, contraction_threshold_stdev: float = 0.2) -> pd.DataFrame:
    """
    Виконує розширений аналіз для кожної аномалії, розглядаючи періоди "до" і "після" (1–3 дні).
    Додає класифікацію тренду, перевірку підготовчої фази та contraction.
    """
    if df_anomalies.empty:
        logger.info(f"[{symbol}] Аномалій для глибокого аналізу немає.")
        return df_anomalies

    # Перетворення timestamp у datetime, якщо потрібно
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if not pd.api.types.is_datetime64_any_dtype(df_anomalies["timestamp"]):
        df_anomalies["timestamp"] = pd.to_datetime(df_anomalies["timestamp"], utc=True)

    df = df.sort_values("timestamp").reset_index(drop=True)
    df_anomalies = df_anomalies.sort_values("timestamp").reset_index(drop=True)

    for idx, row in df_anomalies.iterrows():
        anomaly_time = row["timestamp"]
        start_time = anomaly_time - pd.Timedelta(days=lookback_days)
        end_time = anomaly_time + pd.Timedelta(days=lookforward_days)
        window_df = df[(df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)]
        if len(window_df) < 2:
            continue

        price_start = window_df["close"].iloc[0]
        price_end = window_df["close"].iloc[-1]
        price_change = (price_end - price_start) / price_start if price_start != 0 else 0

        # Додавання змін для аналізу "до" та "після"
        window_df["price_change_before"] = window_df["close"].pct_change(periods=-1)
        window_df["price_change_after"] = window_df["close"].pct_change(periods=1)
        window_df["volume_change_before"] = window_df["volume"].pct_change(periods=-1)
        window_df["volume_change_after"] = window_df["volume"].pct_change(periods=1)

        # Класифікація за зміною ціни (поріг ±1%)
        threshold = 0.01
        if price_change > threshold:
            trend_class = "POSITIVE"
        elif price_change < -threshold:
            trend_class = "NEGATIVE"
        else:
            trend_class = "NEUTRAL"
        df_anomalies.loc[idx, "trend_class"] = trend_class

        # Перевірка підготовчої фази
        preparatory_flag = detect_preparatory_phase(df, symbol, anomaly_time, lookback_hours=lookback_pre_hours)
        if preparatory_flag:
            df_anomalies.loc[idx, "event_type"] = "PREPARATORY"
            df_anomalies.loc[idx, "preparatory_flag"] = True

        # Перевірка contraction – якщо стандартне відхилення у вікні менше порогу
        if len(window_df) > 2:
            std_window = window_df["close"].std()
            if std_window < contraction_threshold_stdev:
                df_anomalies.loc[idx, "pre_contraction"] = True

        logger.debug(
            f"[{symbol}] Аномалія @ {anomaly_time}, зміна: {price_change:.2%}, тренд: {trend_class}, preparatory: {preparatory_flag}"
        )
    return df_anomalies


# Приклад інтегрованого циклу для аналізу аномалій у окремому процесі
async def anomaly_analysis_cycle(
    symbol: str,
    df: pd.DataFrame,
    file_manager: Any = None,
    cycle_period: int = 300  # наприклад, оновлення кожні 5 хвилин
):
    """
    Циклічний аналіз аномалій:
      - Виконує виявлення аномалій за комбінованим методом.
      - Проводить базовий аналіз "до/після".
      - Проводить глибокий аналіз трендів навколо аномалій.
      - (Опційно) зберігає результати у файл.
    """
    while True:
        logger.debug(f"[{symbol}] Початок аналізу аномалій.")
        # Виявлення аномалій комбінованим методом
        anomalies_combined = detect_anomalies_combined(df, symbol, column="close")
        if not anomalies_combined.empty:
            analyze_anomalies_basic(anomalies_combined, symbol)
            deep_analyzed = await deep_anomaly_analysis(df, anomalies_combined, symbol)
            logger.info(f"[{symbol}] Глибокий аналіз аномалій завершено, виявлено {len(deep_analyzed)} аномалій.")
            if file_manager:
                file_manager.save_file(symbol, context="anomaly_analysis",
                                         event_type="deep", data=deep_analyzed, extension="json")
        else:
            logger.debug(f"[{symbol}] Аномалій не виявлено у поточному циклі.")
        await asyncio.sleep(cycle_period)
