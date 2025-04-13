"""
technical_analysis.py

Модуль для технічного аналізу, що адаптується до різних таймфреймів та інтервалів.
Розраховує популярні технічні індикатори (RSI, MACD, ATR, Bollinger Bands, EMA, SMA, ADX)
та виконує базовий аналіз трендів та аномалій.
"""
import os
import logging
import json
import aiohttp
from io import StringIO
from aiogram import Bot, Dispatcher
import pandas as pd
import datetime as dt
from datetime import datetime
import numpy as np

npNaN = np.nan
import pandas_ta as ta
from typing import List, Dict, Tuple, Any, Optional, Union
from scipy.signal import find_peaks
from indicators.asg import AdaptiveSignalGenerator
from indicators.rsi import RSIIndicator
from indicators.monitor_rsi import RSIMonitor
from indicators.vwap_volume_analyzer import VWAPVolumeAnalyzer
from monitor.adaptive_thresholds import AdaptiveThresholds
from datafetcher.cache_handler import CacheHandler
from datafetcher.file_manager import FileManager
from datafetcher.utils import ColumnValidator

logger = logging.getLogger("TechnicalAnalyzer")
logger.setLevel(logging.DEBUG)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_API_KEY")
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
ADMIN_ID = 1501232696

def significant_change(old_val: float, new_val: float, threshold: float = 0.01) -> bool:
    """Перевіряє, чи змінився показник суттєво (відносна різниця більше threshold)."""
    return abs(new_val - old_val) / (old_val + 1e-9) > threshold

class TechnicalAnalyzer:
    def __init__(self, interval: str, bins: int = 50, 
                 file_manager: Optional[FileManager] = None,
                 cache_handler: Optional[CacheHandler] = None, 
                 validate: Optional[ColumnValidator] = None,
                 cache_ttl: int = 3600,
                 levels: int = 2
        ):
        self.interval = interval
        self.bins = bins
        self.file_manager = file_manager
        self.cache_handler = cache_handler
        self.validate = validate
        self.levels = int(levels),
        """
        Ініціалізація аналізатора з заданим інтервалом (наприклад, '1h', '1d', '5min' тощо).
        """

    async def add_indicators(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Асинхронно додає основні технічні індикатори до DataFrame:
          - RSI, MACD
          - Адаптивні сигнали (ATR, KAMA, сигнал) через AdaptiveSignalGenerator
          - Динамічні рівні підтримки та опору (support/resistance)
          - Bollinger Bands, EMA20, SMA50, ADX
        """
        try:
            # Виклик розширеного RSI через RSIIndicator:
            rsi_indicator = RSIIndicator(
                cache_handler=self.cache_handler,
                file_manager=self.file_manager,
                period=14,
            )

            rsi_df = await rsi_indicator.calculate(df, symbol, self.interval)
            if "rsi" in rsi_df.columns:
                df["rsi"] = rsi_df["rsi"]
            else:
                logger.warning(f"[{symbol}] Не вдалося розрахувати RSI: недостатньо даних")
                df["rsi"] = None  # або залиште без змін, або встановіть значення за замовчуванням

            logger.debug("Розраховано розширений RSI. Останні 3 значення:\n%s", df["rsi"].tail(3).to_string())
            
            # (2) Моніторинг RSI
            logger.debug(f"[{symbol}] Починаємо моніторинг rsi.")
            rsi_monitor = RSIMonitor(
                cache_handler=self.cache_handler,
                file_manager=self.file_manager,
            )
            rsi_analysis = await rsi_monitor.monitor_rsi(df, symbol, self.interval)
            if rsi_analysis:
                logger.debug(f"[{symbol}] Моніторинг rsi завершено: {rsi_analysis}")
            else:
                logger.warning(f"[{symbol}] Моніторинг rsi не дав результатів.")

            # MACD (fast=12, slow=26, signal=9)
            macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
            df = pd.concat([df, macd_df], axis=1)
            logger.debug("MACD додано.")

            # Виклик адаптивного генератора сигналів (асинхронно)
            adaptive_gen = AdaptiveSignalGenerator(
                df,
                kama_period=10,
                atr_period=14,
                volatility_threshold=0.02,
                cache_handler=self.cache_handler,
                file_manager=self.file_manager,
            )
            # Передаємо symbol як параметр
            df = await adaptive_gen.generate_signal(symbol)

            # Розрахунок динамічних рівнів на основі ATR
            #df = self._add_dynamic_levels(df, symbol)

            # Додатково – інтеграція VWAPVolumeAnalyzer
            vwap_analyzer = VWAPVolumeAnalyzer(redis_client=None, file_manager=self.file_manager,
                                               cache_handler=self.cache_handler)
            df = await vwap_analyzer.calculate_vwap_and_classify(df, symbol, volatility_threshold=5.0, min_window=20, use_cache=True)
            logger.debug("VWAP та класифікація обсягів додано. Останні 3 рядки (timestamp, VWAP, volume_class):\n%s",
                         df[["timestamp", "VWAP", "volume_class"]].tail(3).to_string())

            # Bollinger Bands (20 період, std=2)
            bbands_df = ta.bbands(df["close"], length=20, std=2.0)
            required_cols = ["BBL_20_2.0", "BBM_20_2.0", "BBU_20_2.0"]
            if all(col in bbands_df.columns for col in required_cols):
                df["bb_lower"] = bbands_df["BBL_20_2.0"]
                df["bb_middle"] = bbands_df["BBM_20_2.0"]
                df["bb_upper"] = bbands_df["BBU_20_2.0"]
                df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_middle"]
                logger.debug("Bollinger Bands додано.")
            else:
                logger.warning("Не вдалося розрахувати Bollinger Bands: відсутні потрібні колонки.")

            # EMA та SMA
            df["ema20"] = ta.ema(df["close"], length=20)
            df["sma50"] = ta.sma(df["close"], length=50)
            logger.debug("EMA20 та SMA50 додано.")

            # ADX (14 період)
            """
            Потрібно додати логіку обчислення ADX, +DI, -DI через pandas
            (rolling sums, без talib).

            Формули (rolling-варіант):
              1) upMove = high[i] - high[i-1], downMove = low[i-1] - low[i]
              2) +DM[i] = upMove if upMove > downMove and upMove > 0 else 0
                 -DM[i] = downMove if downMove > upMove and downMove > 0 else 0
              3) TR[i] = max( high[i]-low[i], |high[i]-close[i-1]|, |low[i]-close[i-1]| )
              4) sum_plusDM = rolling_sum(+DM, period)
                 sum_minusDM = rolling_sum(-DM, period)
                 sum_TR = rolling_sum(TR, period)
              5) +DI = 100 * (sum_plusDM / sum_TR)
                -DI = 100 * (sum_minusDM / sum_TR)
                 DX = 100 * abs(+DI - -DI) / (+DI + -DI)
              6) ADX = rolling_mean(DX, period)

            Wilder's smoothing можна зробити через власні формули
            """
            adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
            if "ADX_14" in adx_df.columns:
                df["adx"] = adx_df["ADX_14"]
                logger.debug("ADX додано.")
            else:
                df["adx"] = np.nan
                logger.warning("Не вдалося розрахувати ADX: колонка ADX_14 відсутня.")
            
            # Розрахунок ADL
            df["Money Flow Multiplier"] = (
                (df["close"] - df["low"]) - (df["high"] - df["close"])
            ) / (df["high"] - df["low"])
            df["Money Flow Volume"] = df["Money Flow Multiplier"] * df["volume"]
            df["adl"] = df["Money Flow Volume"].cumsum()

            # Логування результату
            print(
                f"ADL розраховано: перші 5 рядків:\n{df[['timestamp', 'adl']].head()}"
            )

            # Розрахунок Pivot Points
            df["pivot"] = (df["high"] + df["low"] + df["close"]) / 3
            df["r1"] = (2 * df["pivot"]) - df["low"]
            df["s1"] = (2 * df["pivot"]) - df["high"]
            df["r2"] = df["pivot"] + (df["high"] - df["low"])
            df["s2"] = df["pivot"] - (df["high"] - df["low"])

            # Розрахунок додаткових рівнів, якщо levels > 2
            for level in range(3, int(self.levels[0]) + 1):
                multiplier = level - 1
                df[f"r{level}"] = df["r2"] + multiplier * (df["r2"] - df["pivot"])
                df[f"s{level}"] = df["s2"] - multiplier * (df["pivot"] - df["s2"])

            # Логування результату
            print(
                f"Pivot Points розраховано::\n{df[['timestamp', 'pivot', 'r1', 's1']].tail(3).to_string()}"
            )

            # Розрахунок додаткових рівнів, якщо потрібно (тут можна додати додаткову логіку)

            # Розрахунок цінових діапазонів
            # Отримуємо не тільки мітки, а й інтервали:
            price_ranges, bins = pd.cut(df["close"], bins=self.bins, retbins=True)
            df["price_range"] = price_ranges

            # Групуємо по інтервалам:
            volume_profile = df.groupby("price_range")["volume"].sum().reset_index()

            # Обчислюємо мідпоінт для кожного інтервалу:
            volume_profile["price_level"] = volume_profile["price_range"].apply(lambda interval: (interval.left + interval.right) / 2)

            logger.debug(f"Volume Profile розраховано:\n{volume_profile.tail(3).to_string()}")


            self.file_manager.save_file(
                symbol=symbol,
                context="analyze_indicators",
                event_type="indicators",
                data=df,
                extension="json",
            )
            logger.debug(f"[{symbol}] Дані з індикаторами збережено (all_indicators).")
        except Exception as e:
            logger.error("Помилка розрахунку індикаторів: %s", e, exc_info=True)
        return df

    def _add_dynamic_levels(self, df: pd.DataFrame, symbol: str, atr_multiplier: float = 2.0, atr_threshold: Optional[float] = None) -> pd.DataFrame:
        """
        Розраховує динамічні рівні підтримки та опору на основі ATR.
        """
        if "atr" not in df.columns:
            logger.error(f"[{symbol}] ATR не розраховано, не можна обчислити динамічні рівні.")
            return df

        # Обчислення згладженого ATR (наприклад, ковзне середнє з періодом 3)
        df["atr_smoothed"] = df["atr"].rolling(window=3, min_periods=1).mean()
        
        # Обчислення адаптивного множника на основі середньої волатильності
        if "volatility" in df.columns:
            avg_volatility = df["volatility"].mean()
        else:
            avg_volatility = 1.0
        adaptive_multiplier = atr_multiplier * (1 + avg_volatility / 100)

        # Розрахунок динамічних рівнів
        df["dynamic_support"] = df["close"] - df["atr_smoothed"]
        df["dynamic_resistance"] = df["close"] + df["atr_smoothed"]

        # Розрахунок зон підтримки/опору
        df["support_zone"] = df["dynamic_support"] - adaptive_multiplier * df["atr_smoothed"]
        df["resistance_zone"] = df["dynamic_resistance"] + adaptive_multiplier * df["atr_smoothed"]

        # Фільтрація рівнів за порогом ATR, якщо задано
        if atr_threshold is not None:
            last_close = df["close"].iloc[-1]
            df["filtered_support"] = df["dynamic_support"].where(
                (df["atr"] <= atr_threshold) & (abs(df["dynamic_support"] - last_close) <= atr_threshold)
            )
            df["filtered_resistance"] = df["dynamic_resistance"].where(
                (df["atr"] <= atr_threshold) & (abs(df["dynamic_resistance"] - last_close) <= atr_threshold)
            )
        else:
            df["filtered_support"] = df["dynamic_support"]
            df["filtered_resistance"] = df["dynamic_resistance"]

        logger.info(f"[{symbol}] Розрахунок динамічних рівнів завершено.")
        return df

    def find_local_extremes(self, df: pd.DataFrame, column: str = "close", distance: int = 5):
        """
        Знаходить локальні максимуми та мінімуми у вказаній колонці.
        """
        try:
            peaks, _ = find_peaks(df[column].values, distance=distance)
            troughs, _ = find_peaks(-df[column].values, distance=distance)
            logger.debug("Локальні екстремуми знайдені: піки=%d, мінімуми=%d.", len(peaks), len(troughs))
            return peaks, troughs
        except Exception as e:
            logger.error("Помилка знаходження локальних екстремумів: %s", e, exc_info=True)
            return np.array([]), np.array([])

    async def calculate_vwap(self, df: pd.DataFrame, symbol: str) -> pd.Series:
        """
        Розраховує VWAP (Volume-Weighted Average Price).
        """
        try:
            vwap_analyzer = VWAPVolumeAnalyzer(redis_client=None, file_manager=self.file_manager,
                                               cache_handler=self.cache_handler)
            df = await vwap_analyzer.calculate_vwap_and_classify(df, symbol, volatility_threshold=5.0, min_window=20, use_cache=True)
            logger.debug("VWAP та класифікація обсягів додано. Останні 3 рядки (timestamp, VWAP, volume_class):\n%s",
                         df[["timestamp", "VWAP", "volume_class"]].tail(3).to_string())

        except Exception as e:
            logger.error("Помилка розрахунку VWAP: %s", e, exc_info=True)
            return pd.Series(dtype=float)

    def detect_anomalies_combined(self, df: pd.DataFrame, symbol: str, column: str = "close", 
                                        z_threshold: float = 2.5, mad_multiplier: float = 2.7) -> pd.DataFrame:
        """
        Виявляє аномалії за допомогою комбінації Z‑Score та MAD.
        Обчислює Z‑Score та MAD для заданої колонки і повертає записи, що перевищують пороги.
    
        Аргументи:
          df: DataFrame із даними, який має містити колонку "timestamp".
          symbol: Символ активу для логування.
          column: Назва колонки, для якої проводиться аналіз (за замовчуванням "close").
          z_threshold: Поріг для Z‑Score (за замовчуванням 2.5).
          mad_multiplier: Множник для MAD-порогу (за замовчуванням 2.7).
      
        Повертає:
          DataFrame з рядками, що позначені як аномальні за допомогою обох методик.
        """
        try:
            # Перевірка типу даних
            if isinstance(df, str):
                raise ValueError("Переданий параметр df є рядком, але очікується DataFrame.")
            if "timestamp" not in df.columns:
                raise ValueError("DataFrame повинен містити колонку 'timestamp'.")
            
            # Переконуємось, що колонка "timestamp" існує
            if "timestamp" not in df.columns:
                raise ValueError("DataFrame повинен містити колонку 'timestamp'.")
            # Якщо "timestamp" не є типом datetime, конвертуємо
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            
            # Визначаємо часовий пояс з першого запису
            tz = df["timestamp"].iloc[0].tz if df["timestamp"].iloc[0].tz is not None else "UTC"
            # Фільтруємо записи з датою не ранішою за 1 січня 2000 року
            valid_df = df[df["timestamp"] >= pd.Timestamp("2000-01-01", tz=tz)].copy()

            # Обчислення Z‑Score (якщо ще не розраховано)
            if "z_score" not in valid_df.columns:
                mean_val = valid_df[column].mean()
                std_val = valid_df[column].std() or 1e-9
                valid_df["z_score"] = (valid_df[column] - mean_val) / std_val

            anomalies_z = valid_df[valid_df["z_score"].abs() > z_threshold]

            # Розрахунок MAD
            median_val = valid_df[column].median()
            mad_val = np.median(np.abs(valid_df[column] - median_val))
            mad_threshold = 1.4826 * mad_val
            lower_bound = median_val - mad_multiplier * mad_threshold
            upper_bound = median_val + mad_multiplier * mad_threshold
            anomalies_mad = valid_df[(valid_df[column] < lower_bound) | (valid_df[column] > upper_bound)]

            # Об'єднання аномалій за обома методиками
            anomalies_combined = pd.concat([anomalies_z, anomalies_mad]).drop_duplicates(subset=["timestamp"])
            logger.debug(f"[{symbol}] Виявлено {len(anomalies_combined)} аномалій (комбіновано).")
            return anomalies_combined
        except Exception as e:
            logger.error(f"[{symbol}] Помилка при виявленні комбінованих аномалій: {e}", exc_info=True)
            return pd.DataFrame()


    def analyze_anomalies_basic(self, df_anomalies: pd.DataFrame, symbol: str) -> None:
        """
        Базовий аналіз "до" і "після" для виявлених аномалій:
          - Розрахунок процентної зміни ціни та обсягу для записів перед і після.
        """
        if df_anomalies.empty:
            logger.info(f"[{symbol}] Аномалій для базового аналізу немає.")
            return

        df_anomalies["price_change_before"] = df_anomalies["close"].pct_change(periods=-1)
        df_anomalies["price_change_after"] = df_anomalies["close"].pct_change(periods=1)
        df_anomalies["volume_change_before"] = df_anomalies["volume"].pct_change(periods=-1)
        df_anomalies["volume_change_after"] = df_anomalies["volume"].pct_change(periods=1)
        logger.debug(
            f"[{symbol}] Базовий аналіз аномалій:\n{df_anomalies[['timestamp', 'close', 'price_change_before', 'price_change_after', 'volume_change_before', 'volume_change_after']].to_string(index=False)}"
        )

    def detect_preparatory_phase(self, df: pd.DataFrame, anomaly_time: pd.Timestamp,
                                  lookback_hours: int = 12, vol_multiplier: float = 1.5,
                                  rsi_threshold: float = 60, volume_threshold: float = 1.2) -> bool:
        """
        Перевіряє, чи є ознаки підготовчої фази за 'lookback_hours' до anomaly_time.
        Перевіряє зростання волатильності, RSI та обсягів.
        """
        start_time = anomaly_time - pd.Timedelta(hours=lookback_hours)
        window_df = df[(df["timestamp"] >= start_time) & (df["timestamp"] < anomaly_time)]
        if len(window_df) < 2:
            return False

        # Розрахунок абсолютної волатильності
        if "volatility_abs" not in window_df.columns:
            window_df = window_df.assign(volatility_abs=window_df["high"] - window_df["low"])
        avg_volatility = window_df["volatility_abs"].mean()
        avg_volume = window_df["volume"].mean()
        last_rsi = window_df["rsi"].iloc[-1] if "rsi" in window_df.columns else 50

        global_vol_avg = df["volatility_abs"].mean() if "volatility_abs" in df.columns else 0
        global_volume_avg = df["volume"].mean()

        if (avg_volatility > global_vol_avg * vol_multiplier) or \
           (avg_volume > global_volume_avg * volume_threshold) or \
           (last_rsi > rsi_threshold):
            return True
        return False

    async def deep_anomaly_analysis(self, df: pd.DataFrame, df_anomalies: pd.DataFrame,
                                    symbol: str, lookback_days: int = 1, lookforward_days: int = 3,
                                    lookback_pre_hours: int = 12, contraction_threshold_stdev: float = 0.2) -> pd.DataFrame:
        """
        Виконує розширений аналіз кожної аномалії, розглядаючи періоди до і після аномалії.
        Додає класифікацію тренду, перевірку підготовчої фази та contraction.
        """
        if df_anomalies.empty:
            logger.info(f"[{symbol}] Аномалій для глибокого аналізу немає.")
            return df_anomalies

        # Перетворення timestamp, якщо потрібно
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

            # Додаємо процентні зміни
            window_df["price_change_before"] = window_df["close"].pct_change(periods=-1)
            window_df["price_change_after"] = window_df["close"].pct_change(periods=1)
            window_df["volume_change_before"] = window_df["volume"].pct_change(periods=-1)
            window_df["volume_change_after"] = window_df["volume"].pct_change(periods=1)

            threshold = 0.01
            trend_class = "POSITIVE" if price_change > threshold else ("NEGATIVE" if price_change < -threshold else "NEUTRAL")
            df_anomalies.loc[idx, "trend_class"] = trend_class

            preparatory = self.detect_preparatory_phase(df, anomaly_time, lookback_hours=lookback_pre_hours)
            if preparatory:
                df_anomalies.loc[idx, "event_type"] = "PREPARATORY"
                df_anomalies.loc[idx, "preparatory_flag"] = True

            if len(window_df) > 2:
                std_window = window_df["close"].std()
                if std_window < contraction_threshold_stdev:
                    df_anomalies.loc[idx, "pre_contraction"] = True

            logger.debug(
                f"[{symbol}] Аномалія @ {anomaly_time}, зміна: {price_change:.2%}, тренд: {trend_class}, preparatory: {preparatory}"
            )
        return df_anomalies



    async def run_full_analysis(self, df: pd.DataFrame, symbol: str, thresholds: Dict[str, float], timezone: str = "UTC") -> pd.DataFrame:
        """
        Повний цикл технічного аналізу:
          1. Додавання індикаторів (з урахуванням адаптивних сигналів та динамічних рівнів).
          2. Визначення локальних екстремумів.
          3. Розрахунок VWAP.
          4. (За потреби) повторне оновлення, якщо зміни суттєві.
        """
        if thresholds is None:
            thresholds = {}

        # Переконуємося, що колонка timestamp має правильний тип
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df["timestamp"] = df["timestamp"].dt.tz_convert(timezone)

        last_old = df.iloc[-1].copy()
        logger.debug(f"[{symbol}][TECH] Початок розрахунку індикаторів.")

        df = await self.add_indicators(df, symbol)
        logger.debug(
            f"[{symbol}][TECH] Перші 3 рядки після додавання індикаторів:\n{df.head(3).to_string()}"
        )
        logger.debug(
            f"[{symbol}][TECH] Останні 3 рядки після додавання індикаторів:\n{df.tail(3).to_string()}"
        )

        # Перевірка суттєвості змін (порівняння 'close' останньої свічки)
        last_new = df.iloc[-1]
        if not significant_change(last_old["close"], last_new["close"], threshold=0.01):
            logger.debug(f"[{symbol}][TECH] Зміни незначні, індикатори не перераховуються.")
            return df
        
        # Отримання адаптивних порогів
        adaptive_thresholds = AdaptiveThresholds(asset=symbol,
                                            base_window=21,
                                            volatility_multiplier=1.8,
                                            regime_clusters=2, 
                                            update_interval=7, 
                                            shock_sensitivity=0.25)
        current_day = (dt.date.today() - dt.date(2009, 1, 1)).days
        logger.debug(f"[TECH][{symbol}] Поточний день: {current_day}")

        thresholds = adaptive_thresholds.get_adaptive_thresholds(df, current_day)
        df["volatility_threshold"] = thresholds.get("volatility_threshold", 0.0)
        df["volume_threshold"] = thresholds.get("volume_threshold", 0.0)
        df["avg_volume"] = thresholds.get("avg_volume", 0.0)
        # Генерація звіту
        print(adaptive_thresholds.generate_report())

        # 11. Розширений аналіз рівнів підтримки/опору
        entry_signals = detect_entry_points(df, symbol)
        extended_levels = analyze_levels_extended(entry_signals, last_old["close"], adaptive_thresholds=thresholds)
        logger.debug(f"[{symbol}][TECH] Розширений аналіз рівнів.{extended_levels}")

        df["vwap"] = await self.calculate_vwap(df, symbol)
        logger.debug(
            f"[{symbol}][TECH] Повний аналіз завершено.\nHead(3):\n{df.head(3).to_string()}\nTail(3):\n{df.tail(3).to_string()}"
        )
               
        return df

# ====================================================================
# [mt_analysis] Функція для розширеного аналізу рівнів підтримки/опору з адаптивними порогами
# ====================================================================
def analyze_levels_extended(
    signals: Dict[str, Any],
    current_price: float,
    adaptive_thresholds: Optional[Dict[str, float]] = None
) -> Dict[str, Any]:
    """
    Розширений аналіз рівнів підтримки/опору.
    
    Повертає числові значення рівнів підтримки та опору,
    а також текстове повідомлення про те, як поточна ціна співвідноситься з цими рівнями.
    Якщо adaptive_thresholds задано, перевіряється, чи різниця між рівнями перевищує встановлений поріг.
    """
    supports = signals.get("support_levels", [])
    resistances = signals.get("resistance_levels", [])
    result = {"level_info": "", "support": None, "resistance": None}
    
    if supports and resistances:
        support_candidates = [level['close'] for level in supports if level['close'] < current_price]
        resistance_candidates = [level['close'] for level in resistances if level['close'] > current_price]
        support_level = max(support_candidates) if support_candidates else None
        resistance_level = min(resistance_candidates) if resistance_candidates else None
        
        if support_level is not None and resistance_level is not None:
            if adaptive_thresholds:
                vol_thresh = adaptive_thresholds.get("volatility_threshold", 0)
                if (resistance_level - support_level) < vol_thresh:
                    result["level_info"] = ("Неможливо визначити коректно рівні підтримки/опору для поточної ціни "
                                            f"через недостатній діапазон ({resistance_level - support_level:.4f} < {vol_thresh}).")
                else:
                    result["support"] = support_level
                    result["resistance"] = resistance_level
                    result["level_info"] = f"Ціна {current_price} між підтримкою {support_level} та опором {resistance_level}."
            else:
                result["support"] = support_level
                result["resistance"] = resistance_level
                result["level_info"] = f"Ціна {current_price} між підтримкою {support_level} та опором {resistance_level}."
        else:
            result["level_info"] = "Неможливо визначити рівні підтримки/опору – недостатньо кандидатів."
    else:
        result["level_info"] = "Дані про підтримку/опір відсутні або неповні."
    
    return result

# ====================================================================
# [mt_analysis] Функція для визначення точок входу на одному таймфреймі
# ====================================================================
def detect_entry_points(
    df: pd.DataFrame,
    symbol: str,
    prominence_factor: float = 0.005,
    distance: int = 5  # за замовчуванням встановлено мінімальну відстань між піками
) -> Dict[str, Any]:
    """
    [mt_analysis] Визначає потенційні точки входу з використанням:
      - Пошуку локальних мінімумів (зони підтримки)
      - Пошуку локальних максимумів (зони опору)
      - Аналізу перетину EMA та сплесків обсягу.
      
    Покращення:
      - Якщо в DataFrame вже є розраховані 'ema_short' та 'ema_long', то повторне обчислення не проводиться.
      - Використовується параметр distance для більшої стабільності пошуку піків.
    """
    entry_signals = {}
    logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] Розпочато аналіз даних, рядків: {len(df)}")

    # Адаптивне обчислення порога (prominence) на основі волатильності
    if 'volatility' in df.columns:
        prominence = df['volatility'].median() * prominence_factor
    else:
        prominence = prominence_factor

    # Використовуємо find_peaks для знаходження локальних максимумів і мінімумів
    prices = df['close'].values
    support_indices, _ = find_peaks(-prices, prominence=prominence, distance=distance)
    resistance_indices, _ = find_peaks(prices, prominence=prominence, distance=distance)

    entry_signals['support_levels'] = df.iloc[support_indices][['timestamp', 'close']].to_dict(orient='records')
    entry_signals['resistance_levels'] = df.iloc[resistance_indices][['timestamp', 'close']].to_dict(orient='records')
    logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] Підтримка та опір визначено.")

    # Обчислення EMA: перевіряємо, чи вже існують стовпці 'ema_short' і 'ema_long'
    if 'ema_short' not in df.columns or 'ema_long' not in df.columns:
        df['ema_short'] = df['close'].ewm(span=10, adjust=False).mean()
        df['ema_long'] = df['close'].ewm(span=30, adjust=False).mean()
    else:
        logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] Використовується попередньо розрахована EMA.")

    # Обчислюємо перетин EMA
    cross_up = (df['ema_short'] > df['ema_long']) & (df['ema_short'].shift() <= df['ema_long'].shift())
    cross_down = (df['ema_short'] < df['ema_long']) & (df['ema_short'].shift() >= df['ema_long'].shift())
    entry_signals['ema_cross_up'] = df[cross_up][['timestamp', 'close']].to_dict(orient='records')
    entry_signals['ema_cross_down'] = df[cross_down][['timestamp', 'close']].to_dict(orient='records')
    logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] EMA cross-up та cross-down визначено.")

    # Аналіз обсягу (volume spike)
    if 'volume' in df.columns:
        avg_volume = df['volume'].mean()
        volume_spike = df['volume'] > (1.5 * avg_volume)
        entry_signals['volume_spike'] = df[volume_spike][['timestamp', 'volume']].to_dict(orient='records')
        logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] Volume spikes визначено.")
    
    logger.debug(f"[mt_analysis][{symbol}][detect_entry_points] Аналіз завершено.")
    return entry_signals


# Функція для надсилання повідомлень в Telegram
async def send_telegram_alert(message, TELEGRAM_TOKEN, ADMIN_ID):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    #logger.debug(f"[send_telegram_alert] Надсилання повідомлення в Telegram: URL={url}, ADMIN_ID={ADMIN_ID}")
    #logger.debug(f"[send_telegram_alert] Довжина повідомлення: {len(message)} символів. Перші 100 символів: {message[:100]}")

    try:
        async with aiohttp.ClientSession() as session:
            payload = {"chat_id": ADMIN_ID, "text": message}
            #logger.debug(f"[send_telegram_alert] Payload: {payload}")

            async with session.post(url, json=payload) as response:
                #logger.debug(f"[send_telegram_alert] Статус відповіді Telegram API: {response.status}")
                try:
                    response_json = await response.json()
                    #logger.debug(f"[send_telegram_alert] Відповідь Telegram API: {response_json}")
                except Exception as parse_error:
                    logger.error(
                        f"[send_telegram_alert] Неможливо розпарсити відповідь від Telegram: {parse_error}",
                        exc_info=True,
                    )
                    response_json = {}

                if response.status == 200:
                    if response_json.get("ok"):
                        logger.info(
                            f"Повідомлення успішно надіслано..." #: {message[:50]}
                        )
                    else:
                        logger.error(f"Telegram API повернув помилку: {response_json}")
                else:
                    logger.error(
                        f"Помилка надсилання повідомлення в Telegram: Статус {response.status}. Повна відповідь: {response_json}"
                    )

    except aiohttp.ClientError as e:
        logger.error(f"Помилка підключення до Telegram API: {e}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Невідома помилка надсилання повідомлення в Telegram: {e}", exc_info=True
        )

# Приклад використання в циклі
if __name__ == "__main__":
    import datetime as dt

    # Припустимо, дані завантажені (наприклад, з CSV або API)
    # Для демонстрації створимо тестовий DataFrame з даними кожної години за 2 дні.
    start_timestamp = pd.Timestamp("2025-02-24 00:00:00", tz="UTC").value // 10**6
    timestamps = [start_timestamp + i * 3600000 for i in range(48)]
    data = {
        "timestamp": timestamps,
        "open": np.random.uniform(9000, 10000, 48),
        "high": np.random.uniform(10000, 10500, 48),
        "low": np.random.uniform(8500, 9000, 48),
        "close": np.random.uniform(9000, 10000, 48),
        "volume": np.random.uniform(100, 500, 48),
    }
    df_raw = pd.DataFrame(data)

    analyzer = TechnicalAnalyzer(interval="1h")
    df_analyzed = analyzer.run_full_analysis(df_raw, symbol="BTCUSDT")
    print("Перші 5 рядків після технічного аналізу:")
    print(df_analyzed.head(5))
