"""
Модуль monitoring.py

Інтегрований моніторинг активів, який використовує результати відбору з модуля asset_selector.
Модуль оновлює дані, аналізує активи за допомогою попередніх розрахунків та формує
уніфікований звіт для прийняття рішень, який може бути відправлений у Telegram.

Автор: [Std07.1]
Дата: [16.03.25]
"""

import asyncio
import logging
import time
import json
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any, Optional
import os

import pandas as pd
import numpy as np
import aiohttp
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher

# Налаштування логування
log_filename = "system.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_filename, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger("monitoring")

load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_API_KEY")
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
ADMIN_ID = 1501232696


# ----------------------- Декоратор для логування виконання функцій -----------------------

def log_execution(func):
    """Декоратор для логування початку, завершення та часу виконання асинхронних функцій."""
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.debug(f"[{func.__name__}] Початок виконання. Аргументи: {args}, {kwargs}")
        result = await func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.debug(f"[{func.__name__}] Завершено за {elapsed:.2f} сек.")
        return result
    return wrapper


# ----------------------- Функція для надсилання повідомлень в Telegram -----------------------

async def send_telegram_alert(message: str, telegram_token: str, admin_id: int) -> None:
    """Надсилає повідомлення в Telegram через API."""
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"chat_id": admin_id, "text": message}
            async with session.post(url, json=payload) as response:
                try:
                    response_json = await response.json()
                except Exception as parse_error:
                    logger.error(f"[send_telegram_alert] Помилка розбору відповіді: {parse_error}", exc_info=True)
                    response_json = {}
                if response.status == 200:
                    if response_json.get("ok"):
                        logger.info("Повідомлення успішно надіслано.")
                    else:
                        logger.error(f"Telegram API повернув помилку: {response_json}")
                else:
                    logger.error(f"Помилка надсилання повідомлення: Статус {response.status}. Відповідь: {response_json}")
    except aiohttp.ClientError as e:
        logger.error(f"Помилка підключення до Telegram API: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Невідома помилка при надсиланні повідомлення: {e}", exc_info=True)


# ----------------------- Функції оновлення даних -----------------------

async def hourly_update_cycle(symbol: str, interval: str = "1h", limit: int = 60,
                              cache_handler: Any = None, file_manager: Any = None,
                              period: int = 10) -> None:
    """
    Циклічне оновлення даних для 1-годинного інтервалу.
    Отримані дані зберігаються у файлі та (за потреби) в кеші.
    """
    last_df = None
    while True:
        logger.debug(f"[HOURLY] Початок оновлення даних для {symbol}.")
        # Ініціалізуємо DataFetcher із використанням кешу та файлового менеджера
        from datafetcher.raw_data import DataFetcher
        data_fetcher = DataFetcher(cache_handler=cache_handler, file_manager=file_manager)
        df = await data_fetcher.fetch_kline_data(symbol=symbol, interval=interval, limit=limit)
        if df is not None:
            logger.debug(f"[HOURLY] Оновлено дані, записів: {len(df)}")
        else:
            logger.error(f"[HOURLY] Не вдалося отримати дані для {symbol}.")

        if df is not None:
            new_last_ts = pd.to_datetime(df["timestamp"].max())
            if last_df is not None:
                prev_last_ts = pd.to_datetime(last_df["timestamp"].max())
                if new_last_ts == prev_last_ts:
                    logger.debug(f"[HOURLY] Дані не змінилися (остання мітка: {new_last_ts.strftime('%Y-%m-%d %H:%M:%S')}).")
                else:
                    logger.debug(f"[HOURLY] Зміни в даних (остання мітка: {new_last_ts.strftime('%Y-%m-%d %H:%M:%S')}). Оновлення файлу.")
                    if file_manager:
                        file_manager.save_file(symbol, context="raw_data_1h", event_type="one_step", data=df, extension="json")
                    last_df = df.copy()
            else:
                logger.debug(f"[HOURLY] Файл не існує, збереження нових даних для {symbol}.")
                if file_manager:
                    file_manager.save_file(symbol, context="raw_data_1h", event_type="one_step", data=df, extension="json")
                last_df = df.copy()
        await asyncio.sleep(period)


@log_execution
async def daily_update_cycle(symbol: str, interval: str = "1d", full_limit: int = 60,
                             incremental_limit: int = 2, cache_handler: Any = None,
                             file_manager: Any = None, period: int = 14400) -> None:
    """
    Інкрементальне оновлення денних даних.
    Дані зберігаються у кеші за ключем "daily:{symbol}".
    """
    cache_key = f"daily:{symbol}"
    cached_json = await cache_handler.fetch_from_cache(cache_key)
    if cached_json:
        try:
            df_cached = pd.read_json(StringIO(cached_json), orient="records")
            logger.debug(f"[daily_update_cycle][{symbol}] Завантажено з кешу {len(df_cached)} записів. Останні 3:\n{df_cached.tail(3)}")
        except Exception as e:
            logger.error(f"[daily_update_cycle][{symbol}] Помилка читання кешу: {e}")
            df_cached = pd.DataFrame()
    else:
        df_cached = pd.DataFrame()
        logger.debug(f"[daily_update_cycle][{symbol}] Кеш порожній.")

    from datafetcher.raw_data import DataFetcher
    data_fetcher = DataFetcher(cache_handler=cache_handler, file_manager=file_manager)

    while True:
        logger.debug(f"[daily_update_cycle][{symbol}] Початок циклу оновлення.")
        if len(df_cached) < full_limit:
            logger.info(f"[daily_update_cycle][{symbol}] Кеш містить лише {len(df_cached)} записів, очікується {full_limit}. Виконується повне завантаження.")
            new_df = await fetch_with_retry(data_fetcher, symbol, interval, full_limit)
        else:
            new_df = await fetch_with_retry(data_fetcher, symbol, interval, incremental_limit)

        if new_df is None or new_df.empty:
            logger.error(f"[daily_update_cycle][{symbol}] Не отримано нових даних.")
        else:
            logger.debug(f"[daily_update_cycle][{symbol}] Отримано {len(new_df)} нових записів. Останні 3:\n{new_df.tail(3)}")
            required_columns = {"timestamp", "close", "high", "low", "volume"}
            if not required_columns.issubset(new_df.columns):
                logger.error(f"[daily_update_cycle][{symbol}] Нові дані не містять необхідних колонок. Пропускаємо оновлення.")
                await asyncio.sleep(period)
                continue
            new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], utc=True)
            if not df_cached.empty:
                df_cached["timestamp"] = pd.to_datetime(df_cached["timestamp"], utc=True)
                last_cached_ts = df_cached["timestamp"].max()
                new_df = new_df[new_df["timestamp"] > last_cached_ts]
                logger.debug(f"[daily_update_cycle][{symbol}] Після фільтрації, нових записів: {len(new_df)}. Остання кешована мітка: {last_cached_ts}")
            dfs = [df for df in [df_cached, new_df] if not df.empty]
            if dfs:
                df_combined = pd.concat(dfs).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
            else:
                df_combined = pd.DataFrame()
            if not df_combined.empty:
                expected_dates = pd.date_range(start=df_combined["timestamp"].min(), end=df_combined["timestamp"].max(), freq="D")
                actual_dates = pd.to_datetime(df_combined["timestamp"].dt.date.unique())
                missing_dates = set(expected_dates.date) - set(actual_dates)
                if missing_dates:
                    logger.warning(f"[daily_update_cycle][{symbol}] Виявлено розриви в датах: {sorted(missing_dates)[-3:]}")
                else:
                    logger.debug(f"[daily_update_cycle][{symbol}] Розривів у датах не виявлено.")
            if (new_df["close"] <= 0).any() or (new_df["high"] < new_df["low"]).any():
                logger.error(f"[daily_update_cycle][{symbol}] Аномальні дані, оновлення пропущено.")
                await asyncio.sleep(period)
                continue
            df_updated = df_combined.tail(full_limit)
            logger.debug(f"[daily_update_cycle][{symbol}] Оновлено кеш до {len(df_updated)} записів. Останні 3:\n{df_updated.tail(3)}")
            await cache_handler.store_in_cache(cache_key, df_updated.to_json(orient="records"))
            logger.debug(f"[daily_update_cycle][{symbol}] Кеш успішно оновлено.")
            if file_manager:
                file_manager.save_file(symbol, context="raw_data_1d", event_type="one_step", data=df_updated, extension="json")
                logger.debug(f"[daily_update_cycle][{symbol}] Дані збережено у файл.")
            df_cached = df_updated.copy()
        period = min(period * 2, 3600) if (new_df is None or new_df.empty) else max(100, period // 2)
        logger.debug(f"[daily_update_cycle][{symbol}] Нових даних: {len(new_df) if new_df is not None else 0}. Встановлено період: {period} сек.")
        await asyncio.sleep(period)


async def fetch_with_retry(data_fetcher: Any, symbol: str, interval: str, limit: int,
                           retries: int = 3, delay: int = 5) -> Optional[pd.DataFrame]:
    """
    Retry-логіка для отримання даних з API.
    """
    for attempt in range(1, retries + 1):
        new_df = await data_fetcher.fetch_kline_data(symbol=symbol, interval=interval, limit=limit)
        if new_df is not None and not new_df.empty:
            logger.debug(f"[fetch_with_retry][{symbol}] Успішно отримано дані на спробі {attempt}.")
            return new_df
        logger.warning(f"[fetch_with_retry][{symbol}] Спроба {attempt} не вдалася, повтор через {delay} сек.")
        await asyncio.sleep(delay)
    logger.error(f"[fetch_with_retry][{symbol}] Всі {retries} спроби завершились невдачею.")
    return None


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
    Визначає прогнозований проміжок часу на основі волатильності.
    """
    vol = df["close"].std() / df["close"].mean()
    forecast = "24 години" if vol > volatility_threshold else "48 години"
    logger.debug(f"Прогнозований період: {forecast} (волатильність = {vol:.4f})")
    return forecast


def calculate_volatility(df: pd.DataFrame, symbol: str, window: int = 14) -> float:
    """
    Обчислює відносну волатильність за останні 'window' записів.
    """
    logger.debug(f"[{symbol}] Розрахунок волатильності за {window} записів:\n{df.tail(window)}")
    if len(df) < window:
        logger.warning(f"[{symbol}] Недостатньо даних для волатильності: потрібно {window}, отримано {len(df)}")
        return 0.0
    vol_value = df["close"].tail(window).std() / df["close"].tail(window).mean()
    logger.debug(f"[{symbol}] Волатильність: {vol_value:.4f}")
    return vol_value


def determine_asset_category(df: pd.DataFrame, volume_threshold: float = 1.5,
                             vol_std_threshold: float = 0.05) -> str:
    """
    Визначає категорію активу на основі волатильності та обсягів.
    """
    if len(df) < 14:
        return "stable"
    vol = df["close"].std() / df["close"].mean()
    return "high" if vol > vol_std_threshold else "stable"


def choose_timeframe(category: str) -> Dict[str, Any]:
    """
    Обирає оптимальні параметри залежно від категорії активу.
    """
    if category == "high":
        return {"window_classic": 60, "local_prominence": 0.5, "sma_period": 20}
    else:
        return {"window_classic": 180, "local_prominence": 1.0, "sma_period": 50}


def normalize_asset_levels(df: pd.DataFrame, support: float, resistance: float) -> Tuple[float, float]:
    """
    Нормалізує рівні підтримки та опору.
    """
    median_price = df["close"].median()
    if median_price < 1:
        normalized_support = min(support, median_price * 1.5)
        normalized_resistance = max(resistance, median_price * 2)
    elif median_price < 10:
        normalized_support = min(support, median_price * 1.2)
        normalized_resistance = max(resistance, median_price * 1.8)
    else:
        normalized_support, normalized_resistance = support, resistance
    return normalized_support, normalized_resistance


def find_local_levels(symbol: str, prices: np.array, distance: int = 5,
                      prominence: float = 1.0) -> Dict[str, Any]:
    """
    Визначає локальні максимуми та мінімуми для побудови рівнів.
    """
    from scipy.signal import find_peaks
    peaks, _ = find_peaks(prices, distance=distance, prominence=prominence)
    troughs, _ = find_peaks(-prices, distance=distance, prominence=prominence)
    levels = {"peaks": peaks.tolist(), "troughs": troughs.tolist()}
    logger.debug(f"[LocalLevels] [{symbol}] Локальні рівні: Peaks: {prices[peaks].tolist()}, Troughs: {(-prices[troughs]).tolist()}")
    return levels


def determine_nearest_levels(current_price: float, local_levels: Dict[str, Any],
                             prices: np.array) -> Dict[str, Optional[float]]:
    """
    Визначає найближчий рівень підтримки та опору.
    """
    nearest = {"support": None, "resistance": None}
    peak_prices = np.array(prices)[np.array(local_levels["peaks"])].tolist() if local_levels["peaks"] else []
    trough_prices = np.array(prices)[np.array(local_levels["troughs"])].tolist() if local_levels["troughs"] else []
    if trough_prices:
        supports = [price for price in trough_prices if price < current_price]
        if supports:
            nearest["support"] = max(supports)
    if peak_prices:
        resistances = [price for price in peak_prices if price > current_price]
        if resistances:
            nearest["resistance"] = min(resistances)
    return nearest


def calculate_volume_profile(df: pd.DataFrame, symbol: str, bin_size: float) -> pd.DataFrame:
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
    logger.debug(f"[{symbol}] Розраховано volume_profile (останні 3 записи):\n{volume_profile.tail(3).to_string()}")
    return volume_profile


def calculate_vwap(df: pd.DataFrame, symbol: str, min_window: int = 20) -> float:
    """
    Обчислює VWAP (Volume Weighted Average Price).
    """
    logger.debug(f"[{symbol}] Розрахунок VWAP. Останні 3 записи:\n{df.tail(3)}")
    if df.empty or len(df) < min_window:
        logger.warning(f"[{symbol}] Недостатньо даних для VWAP.")
        return float('nan')
    df = df.copy()
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    vwap = (df["typical_price"] * df["volume"]).sum() / df["volume"].sum()
    logger.debug(f"[{symbol}] VWAP: {vwap:.2f}")
    return vwap


def select_relevant_levels(df_daily: pd.DataFrame, symbol: str) -> Dict[str, Any]:
    """
    Обчислює релевантні рівні підтримки та опору на основі денних даних.
    """
    category = determine_asset_category(df_daily)
    params = choose_timeframe(category)
    window_vol = 14
    window_classic = params["window_classic"]

    if len(df_daily) < window_vol:
        rel_vol = 0.0
    else:
        recent = df_daily["close"].tail(window_vol)
        rel_vol = recent.std() / recent.mean()

    local_levels = find_local_levels(symbol, df_daily["close"].values, prominence=params["local_prominence"])
    current_price = df_daily["close"].iloc[-1]
    nearest_local = determine_nearest_levels(current_price, local_levels, df_daily["close"].values)

    support_classic = df_daily["low"].tail(window_classic).min()
    resistance_classic = df_daily["high"].tail(window_classic).max()
    method = "classic"
    support = support_classic
    resistance = resistance_classic

    if nearest_local["support"] is not None or nearest_local["resistance"] is not None:
        diff_support = current_price - nearest_local["support"] if nearest_local["support"] is not None else float('inf')
        diff_resistance = nearest_local["resistance"] - current_price if nearest_local["resistance"] is not None else float('inf')
        if diff_support < (current_price - support_classic) or diff_resistance < (resistance_classic - current_price):
            method = "local"
            support = nearest_local["support"] if nearest_local["support"] is not None else support_classic
            resistance = nearest_local["resistance"] if nearest_local["resistance"] is not None else resistance_classic

    avg_volume = df_daily["volume"].mean()
    current_volume = df_daily["volume"].iloc[-1]
    if current_volume > 1.5 * avg_volume:
        method = "local"
        support = nearest_local["support"] if nearest_local["support"] is not None else support_classic
        resistance = nearest_local["resistance"] if nearest_local["resistance"] is not None else resistance_classic

    normalized_support, normalized_resistance = normalize_asset_levels(df_daily, support, resistance)
    logger.debug(f"[{symbol}] Рівні підтримки/опору: Підтримка = {normalized_support:.2f}, Опір = {normalized_resistance:.2f} (метод: {method})")
    return {
        "support": normalized_support,
        "resistance": normalized_resistance,
        "method": method,
        "rel_vol": rel_vol,
        "avg_volume": avg_volume,
        "current_volume": current_volume,
        "category": category
    }


def analyze_trend_and_volume(df: pd.DataFrame, symbol: str,
                             short_window: int = 5, long_window: int = 20,
                             anomaly_multiplier: float = 2) -> pd.DataFrame:
    """
    Аналізує тренд та обсяг із адаптацією розмірів вікон.
    Додає колонки: short_sma, long_sma, trend_direction, trend_strength, volume_anomaly, volume_class.
    """
    logger.debug(f"[{symbol}] Аналіз тренду та обсягу. Останні 5 записів:\n{df.tail(5)}")
    df = df.copy()
    if "volatility" not in df.columns:
        df["volatility"] = (df["high"] - df["low"]) / df["close"]
    mean_vol = df["volatility"].mean()
    if mean_vol > 0.05:
        short_window, long_window = 5, 20
    else:
        short_window, long_window = 10, 50
    logger.info(f"[{symbol}] Адаптовані вікна: short = {short_window}, long = {long_window}")
    df["volume_avg"] = df["volume"].rolling(window=long_window, min_periods=1).mean()
    df["volume_std"] = df["volume"].rolling(window=long_window, min_periods=1).std()
    df["volume_anomaly"] = df["volume"] > (df["volume_avg"] + anomaly_multiplier * df["volume_std"])
    logger.debug(f"[{symbol}] Кількість аномалій обсягу: {df['volume_anomaly'].sum()}")
    df["volume_class"] = np.select(
        [df["volume_anomaly"], df["volume"] < df["volume_avg"]],
        ["сплеск", "низький"],
        default="нормальний"
    )
    if "taker_buy_quote" in df.columns:
        df["taker_buy_quote"] = pd.to_numeric(df["taker_buy_quote"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["net_volume_delta"] = df["taker_buy_quote"] - (df["volume"] - df["taker_buy_quote"])
        logger.debug(f"[{symbol}] Розраховано 'net_volume_delta':\n{df['net_volume_delta'].tail(3)}")
    else:
        df["net_volume_delta"] = np.nan
        logger.warning(f"[{symbol}] Дані 'taker_buy_quote' відсутні.")
    df["short_sma"] = df["close"].rolling(window=short_window, min_periods=1).mean()
    df["long_sma"] = df["close"].rolling(window=long_window, min_periods=1).mean()
    df["trend_direction"] = np.where(df["short_sma"] > df["long_sma"], "вгору", "донизу")
    df["trend_strength"] = abs(df["short_sma"] - df["long_sma"]) / df["close"]
    summary = (f"Тренд: {df.iloc[-1]['trend_direction']}, сила: {df.iloc[-1]['trend_strength']:.4f}, "
               f"short_sma: {df.iloc[-1]['short_sma']:.2f}, long_sma: {df.iloc[-1]['long_sma']:.2f}")
    logger.debug(f"[{symbol}] Підсумок аналізу тренду:\n{summary}")
    return df


def is_volume_significant(df: pd.DataFrame, symbol: str, current_volume: float, threshold: float = 1.5) -> bool:
    """
    Перевіряє, чи поточний обсяг перевищує середній обсяг на заданий коефіцієнт.
    """
    avg_volume = df["volume"].mean()
    if current_volume >= threshold * avg_volume:
        logger.debug(f"[{symbol}] Поточний обсяг ({format_volume_usd(current_volume)}) перевищує середній ({format_volume_usd(avg_volume)}).")
        return True
    logger.debug(f"[{symbol}] Поточний обсяг ({format_volume_usd(current_volume)}) не перевищує середній ({format_volume_usd(avg_volume)}).")
    return False


def generate_combined_signal(df: pd.DataFrame, symbol: str, current_price: float) -> Dict[str, Any]:
    """
    Побудова комбінованої моделі прийняття рішень на основі VWAP та волатильності.
    """
    vwap = calculate_vwap(df.tail(20), symbol)
    volatility = calculate_volatility(df, symbol)
    signal = "HOLD"
    signal_type = "без явного сигналу"
    if current_price < vwap and volatility < 0.05:
        signal = "BUY"
        signal_type = "відскок від VWAP, висхідний тренд"
    elif current_price > vwap and volatility < 0.05:
        signal = "SELL"
        signal_type = "корекція від VWAP, низхідний тренд"
    alert_msg = (f"[{symbol}] Поточна ціна: {current_price:.2f}, VWAP: {vwap:.2f}, "
                 f"волатильність: {volatility * 100:.2f}%. Сигнал: {signal} ({signal_type}).")
    logger.debug(f"[{symbol}] Згенеровано комбінований сигнал: {alert_msg}")
    return {
        "signal": signal,
        "alert": alert_msg,
        "vwap": vwap,
        "volatility": volatility
    }


def generate_alert(symbol: str, price: float, level: float, level_type: str,
                   approach: bool = False, forecast_period: Optional[str] = None) -> str:
    """
    Генерує текстове повідомлення-оповіщення про пробиття або наближення ключового рівня.
    """
    base_msg = f"[{symbol}] Ціна: {price:.2f} "
    if approach:
        base_msg += f"наближається до {level_type} {level:.2f}."
    else:
        base_msg += f"пробила {level_type} {level:.2f}."
    if forecast_period:
        base_msg += f" Прогноз: {forecast_period}."
    alert = f"ℹ️ {base_msg}" if approach else f"⚠️ {base_msg}"
    logger.debug(f"[{symbol}] Згенеровано оповіщення: {alert}")
    return alert


@log_execution
async def deep_daily_analysis(symbol: str, df: pd.DataFrame, file_manager: Any) -> None:
    """
    Виконує глибокий аналіз денних даних для активу.
    Якщо виявлено аномалії, запускає короткострокове оновлення (наприклад, для 1h).
    """
    logger.info(f"[{symbol}] Розпочато детальний аналіз денних даних...")
    from monitor.technical_analysis import TechnicalAnalyzer
    analyzer = TechnicalAnalyzer(interval="1d", file_manager=file_manager)
    anomalies = analyzer.detect_anomalies_combined(df, symbol, column="close")
    if not anomalies.empty:
        logger.warning(f"[{symbol}] Виявлено {len(anomalies)} аномалій! Запуск короткострокового оновлення.")
        await asyncio.gather(
            hourly_update_cycle(symbol, "1h", 60, cache_handler=None, file_manager=file_manager, period=600)
        )
    else:
        logger.info(f"[{symbol}] Аномалій не виявлено. Продовжуємо денний моніторинг.")


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
    logger.debug(f"Динамічний поріг: {threshold_pct:.2f}% (std = {std:.2f}, current_close = {current_close:.2f})")
    return threshold_pct


# ----------------------- Функції моніторингу та аналізу активів -----------------------

@log_execution
async def monitor_and_analyze(symbol: str, cache_handler: Any, file_manager: Any,
                              db_pool: Any, limit: int = 60) -> None:
    """
    Виконує аналіз денних даних для кожного активу з уніфікованим підходом.
    Використовує дані з кешу, формує єдиний звіт та за потреби надсилає alert.
    """
    logger.info(f"[monitor_and_analyze][{symbol}] Початок циклу аналізу.")
    cached_json = await cache_handler.fetch_from_cache(f"daily:{symbol}")
    if cached_json:
        df_daily = pd.read_json(StringIO(cached_json), orient="records")
        df_daily["timestamp"] = pd.to_datetime(df_daily["timestamp"], utc=True)
        logger.debug(f"[monitor_and_analyze][{symbol}] Отримано {len(df_daily)} записів з кешу. Останні 5:\n{df_daily.tail(5)}")
    else:
        logger.error(f"[monitor_and_analyze][{symbol}] Дані з кешу відсутні.")
        await asyncio.sleep(3600)
        return

    if len(df_daily) < 2:
        logger.error(f"[monitor_and_analyze][{symbol}] Недостатньо даних для аналізу.")
        await asyncio.sleep(3600)
        return

    last_close = df_daily.iloc[-1]["close"]
    prev_close = df_daily.iloc[-2]["close"]
    price_change_pct = (last_close - prev_close) / prev_close * 100
    logger.debug(f"[monitor_and_analyze][{symbol}] Зміна ціни: {price_change_pct:.2f}%")

    # Обчислення класичних рівнів
    window_classic = min(180, len(df_daily))
    support_classic = df_daily["low"].tail(window_classic).min()
    resistance_classic = df_daily["high"].tail(window_classic).max()
    center_classic = (support_classic + resistance_classic) / 2
    logger.info(f"[monitor_and_analyze][{symbol}] Поточна ціна: {last_close:.2f}, Підтримка: {support_classic:.2f}, Опір: {resistance_classic:.2f}")

    # Розрахунок динамічного порогу наближення
    atr_14 = df_daily["high"].rolling(window=14).max() - df_daily["low"].rolling(window=14).min()
    dynamic_threshold_pct = max(2.0, atr_14.mean() * 100 / last_close)
    logger.debug(f"[monitor_and_analyze][{symbol}] Динамічний поріг наближення: {dynamic_threshold_pct:.2f}%")

    forecast_period = determine_forecast_period(df_daily)
    logger.debug(f"[monitor_and_analyze][{symbol}] Прогнозований період: {forecast_period}")

    # Отримання адаптивних рівнів (метод із локального аналізу)
    levels_info = select_relevant_levels(df_daily, symbol)
    logger.info(f"[monitor_and_analyze][{symbol}] Метод рівнів: {levels_info.get('method', 'N/A')}. "
                f"Адаптивна підтримка: {levels_info.get('support', support_classic):.2f}, "
                f"Адаптивний опір: {levels_info.get('resistance', resistance_classic):.2f}.")

    # Розрахунок додаткових індикаторів
    vwap = calculate_vwap(df_daily, symbol)
    volatility = calculate_volatility(df_daily, symbol, window=14)
    trend_df = analyze_trend_and_volume(df_daily, symbol)
    trend_direction = trend_df.iloc[-1]["trend_direction"] if not trend_df.empty else "N/A"
    current_volume = df_daily.iloc[-1]["volume"]
    volume_significant = is_volume_significant(df_daily, symbol, current_volume)

    logger.debug(f"[monitor_and_analyze][{symbol}] VWAP: {vwap:.2f}, Волатильність: {volatility*100:.2f}%, "
                 f"Тренд: {trend_direction}, Обсяг: {current_volume:.2f} (значний: {volume_significant}).")

    combined_signal = generate_combined_signal(df_daily, symbol, last_close)
    if combined_signal:
        await send_telegram_alert(combined_signal.get("alert", "N/A"), TELEGRAM_TOKEN, ADMIN_ID)
    logger.info(f"[monitor_and_analyze][{symbol}] Комбінований сигнал: {combined_signal.get('alert', 'N/A')}")
    
    # Формування alert залежно від умов
    alert = None
    if abs(price_change_pct) > 2 or last_close < support_classic or last_close > resistance_classic:
        if last_close < support_classic and current_volume > df_daily["volume"].rolling(window=14).mean():
            level_for_alert = levels_info.get("support", support_classic)
            level_type = "Підтримка"
        else:
            level_for_alert = levels_info.get("resistance", resistance_classic)
            level_type = "Опір"
        alert = generate_alert(symbol, last_close, level_for_alert, level_type,
                               approach=False, forecast_period=forecast_period)
        logger.warning(f"[monitor_and_analyze][{symbol}] Тригер спрацював, alert: {alert}")
        await deep_daily_analysis(symbol, df_daily, file_manager)
    else:
        distance_to_support = abs(last_close - levels_info.get("support", support_classic)) / levels_info.get("support", support_classic) * 100
        distance_to_resistance = abs(levels_info.get("resistance", resistance_classic) - last_close) / levels_info.get("resistance", resistance_classic) * 100
        if distance_to_support <= dynamic_threshold(df_daily):
            alert = generate_alert(symbol, last_close, levels_info.get("support", support_classic), "Підтримка",
                                   approach=True, forecast_period=forecast_period)
            logger.info(f"[monitor_and_analyze][{symbol}] Наближення підтримки, alert: {alert}")
        elif distance_to_resistance <= dynamic_threshold(df_daily):
            alert = generate_alert(symbol, last_close, levels_info.get("resistance", resistance_classic), "Опір",
                                   approach=True, forecast_period=forecast_period)
            logger.info(f"[monitor_and_analyze][{symbol}] Наближення опору, alert: {alert}")

    if volume_significant:
        logger.info(f"[monitor_and_analyze][{symbol}] Значний обсяг: {current_volume:.2f}.")
    if trend_direction == "вгору" and last_close < vwap:
        logger.info(f"[monitor_and_analyze][{symbol}] Тренд висхідний, ціна нижча за VWAP. Можливий сигнал BUY.")
    elif trend_direction == "донизу" and last_close > vwap:
        logger.info(f"[monitor_and_analyze][{symbol}] Тренд низхідний, ціна вище за VWAP. Можливий сигнал SELL.")

    if combined_signal.get("signal", "HOLD") != "HOLD":
        alert = combined_signal.get("alert")
        logger.info(f"[monitor_and_analyze][{symbol}] Оновлено комбінований сигнал: {alert}")

    analysis_record = {
        "timestamp": datetime.now().isoformat(),
        "symbol": symbol,
        "current_price": last_close,
        "classic_support": support_classic,
        "classic_resistance": resistance_classic,
        "center_classic": center_classic,
        "adaptive_support": levels_info.get("support", support_classic),
        "adaptive_resistance": levels_info.get("resistance", resistance_classic),
        "forecast_period": forecast_period,
        "price_change_percent": price_change_pct,
        "dynamic_threshold_percent": dynamic_threshold_pct,
        "vwap": vwap,
        "volatility": volatility,
        "trend_direction": trend_direction,
        "current_volume": current_volume,
        "volume_significant": volume_significant,
        "combined_signal": combined_signal,
        "levels_info": levels_info
    }
    file_manager.save_file(symbol, context="analysis_event", event_type="monitor", data=analysis_record, extension="json")
    logger.info(f"[monitor_and_analyze][{symbol}] Аналіз подій збережено.")
    await asyncio.sleep(3600)


@log_execution
async def monitor_single_asset(symbol: str, cache_handler: Any, file_manager: Any) -> None:
    """
    Моніторинг одного активу для тестування нових алгоритмів.
    """
    logger.info(f"[monitor_single_asset][{symbol}] Запуск моніторингу одного активу.")
    await asyncio.gather(
        daily_update_cycle(
            symbol=symbol,
            interval="1d",
            full_limit=180,
            incremental_limit=2,
            cache_handler=cache_handler,
            file_manager=file_manager,
            period=100
        ),
        monitor_and_analyze(
            symbol=symbol,
            cache_handler=cache_handler,
            file_manager=file_manager,
            db_pool=None,
            limit=60
        )
    )


@log_execution
async def monitor_multiple_assets(symbols: List[str], cache_handler: Any, file_manager: Any) -> None:
    """
    Паралельний моніторинг декількох активів.
    """
    logger.info("[monitor_multiple_assets] Запуск моніторингу декількох активів.")
    tasks = []
    for symbol in symbols:
        logger.info(f"[monitor_multiple_assets][{symbol}] Планування задач моніторингу.")
        tasks.append(
            asyncio.gather(
                daily_update_cycle(
                    symbol=symbol,
                    interval="1d",
                    full_limit=180,
                    incremental_limit=2,
                    cache_handler=cache_handler,
                    file_manager=file_manager,
                    period=100
                ),
                monitor_and_analyze(
                    symbol=symbol,
                    cache_handler=cache_handler,
                    file_manager=file_manager,
                    db_pool=None,
                    limit=60
                )
            )
        )
    await asyncio.gather(*tasks)


# ----------------------- Функції відбору активів та інтегрованого моніторингу -----------------------

async def run_asset_selection_mode(cache_handler: Any, data_fetcher: Any,
                                   file_manager: Any,
                                   selection_interval: int = 3600) -> None:
    """
    Циклічно виконує відбір активів протягом US сесії.
    Після успішного відбору негайно запускає моніторинг відібраних активів.
    """
    selection_runs = 0
    MAX_SELECTION_RUNS = 3
    logger.info("Запуск режиму відбору активів...")
    
    while True:
        current_time = datetime.now()
        from monitor.asset_selector import (
            is_us_session, fetch_candidates, fetch_market_data,
            select_assets, get_adaptive_thresholds
        )
        if is_us_session(current_time):
            if selection_runs < MAX_SELECTION_RUNS:
                logger.info("US сесія активна. Виконується відбір активів.")
                try:
                    candidates = await fetch_candidates(data_fetcher)
                    market_data = await fetch_market_data(data_fetcher)
                    if candidates:
                        adaptive_thresholds = get_adaptive_thresholds(pd.DataFrame(candidates))
                    else:
                        adaptive_thresholds = {}
                    thresholds = {
                        "min_avg_volume": 500000,
                        "min_volatility": adaptive_thresholds.get("min_volatility", 0.01),
                        "min_structure_score": adaptive_thresholds.get("min_structure_score", 0.7),
                        "min_rel_strength": adaptive_thresholds.get("min_rel_strength", 1.0),
                        "min_relative_volatility": adaptive_thresholds.get("min_relative_volatility", 0.8)
                    }
                    selected = select_assets(candidates, market_data, thresholds)
                    if selected:
                        tickers = [asset.get("ticker", "N/A") for asset in selected]
                        logger.info(f"Відібрано активів: {tickers}")
                        await cache_handler.store_in_cache("selected_assets", selected, ttl=0)
                        await monitor_selected_assets(cache_handler, file_manager)
                    else:
                        logger.info("Немає активів, що відповідають критеріям.")
                    selection_runs += 1
                except Exception as e:
                    logger.error(f"Помилка відбору активів: {e}")
            else:
                logger.info("Ліміт запусків відбору досягнуто для поточної сесії.")
        else:
            if selection_runs > 0:
                logger.info("Поза межами US сесії. Скидаємо лічильник запусків.")
                selection_runs = 0
            else:
                logger.info("Поза межами US сесії.")
        await asyncio.sleep(selection_interval)


async def monitor_selected_assets(cache_handler: Any, file_manager: Any) -> None:
    """
    Запускає моніторинг активів, що були відібрані.
    Якщо список активів з кешу не порожній, викликається моніторинг для кожного активу.
    """
    selected = await cache_handler.fetch_from_cache("selected_assets")
    if not selected:
        logger.info("Список відібраних активів порожній. Моніторинг не запущено.")
        return
    tickers: List[str] = [asset.get("ticker", "N/A") for asset in selected]
    logger.info(f"Запуск моніторингу для активів: {tickers}")
    await monitor_multiple_assets(tickers, cache_handler, file_manager)


async def asset_selection_and_monitoring(cache_handler: Any, file_manager: Any, data_fetcher: Any) -> None:
    """
    Інтегрує запуск режиму відбору активів та моніторингу.
    Обидва процеси запускаються паралельно; при кожному циклі відбору, якщо активи відібрані,
    запускається негайний моніторинг цих активів.
    """
    logger.info("Запуск інтегрованого режиму відбору активів та моніторингу.")
    await asyncio.gather(
        run_asset_selection_mode(cache_handler, data_fetcher, file_manager),
        monitor_selected_assets(cache_handler, file_manager)
    )


# =============================================================================
# Приклад запуску:
# async def main():
#     # Ініціалізація компонентів: cache_handler, file_manager, data_fetcher
#     # Наприклад:
#     # redis_client, cache_handler, db_pool = await init_system()
#     # file_manager = FileManager(redis_client=None)
#     # data_fetcher = DataFetcher(cache_handler=cache_handler, file_manager=file_manager)
#     await asset_selection_and_monitoring(cache_handler, file_manager, data_fetcher)
#
# if __name__ == "__main__":
#     asyncio.run(main())
