"""
monitor/asset_selector/analyzers.py

Модуль для розрахунку технічних показників та оптимізації параметрів.
"""
import numpy as np
import pandas as pd
from scipy.signal import find_peaks
import optuna
import logging


logger = logging.getLogger("asset_selector/analyzers")
logger.setLevel(logging.DEBUG)

def calculate_daily_avg_volume_in_usd(df: pd.DataFrame) -> float:
    """
    Середньоденний обсяг торгів у USD для таймфрейму 1d.

    ▸ Якщо у DataFrame є колонка **quoteVolume** (USDT‑оборот від Binance) –
      використовуємо її напряму, без додаткових множень.

    ▸ Якщо quoteVolume відсутня – обсяг береться як
      volume (контракти) × close (USDT).

    ▸ Якщо поточна доба ще не завершена (< 24 годинних свічок),
      додаємо дані попередньої доби, аби мати повний день.

    Повертає float (USDT).
    """
    if df is None or df.empty:
        return 0.0

    # гарантуємо сортування за часом
    df = df.sort_values("timestamp").copy()

    # — 1. конвертуємо timestamp, якщо треба
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")

    # — 2. відокремлюємо сьогоднішні та вчорашні рядки
    last_date = df["timestamp"].iloc[-1].date()
    df_today = df[df["timestamp"].dt.date == last_date]

    if len(df_today) < 24:
        # доба ще не закрита – додаємо попередню
        yesterday = last_date - pd.Timedelta(days=1)
        df_use = pd.concat([df[df["timestamp"].dt.date == yesterday], df_today])
    else:
        df_use = df_today

    # — 3. рахуємо обсяг у USDT для кожної свічки
    if "quoteVolume" in df_use.columns:
        vol_usd_series = pd.to_numeric(df_use["quoteVolume"], errors="coerce")
    else: # spot‑klines або futures‑klines без quoteVolume
        vol_usd_series = (
            pd.to_numeric(df_use["volume"], errors="coerce")
            * pd.to_numeric(df_use["close"], errors="coerce")
        )

    return float(vol_usd_series.mean(skipna=True))


def calculate_volatility(df: pd.DataFrame) -> float:
    """
    Обчислює волатильність як стандартне відхилення логарифмічних доходів.
    """
    if len(df) < 2:
        return 0.0
    log_returns = np.log(df["close"] / df["close"].shift(1)).dropna()
    volatility = log_returns.std() if not log_returns.empty else 0.0
    return volatility


def calculate_structure_score(prices: pd.Series, distance: int = 5, prominence: float = 1.0) -> float:
    """
    Обчислює показник впорядкованості руху ціни на основі кількості локальних екстремумів.
    """
    if len(prices) < distance:
        return 0.0
    peaks, _ = find_peaks(prices, distance=distance, prominence=prominence)
    troughs, _ = find_peaks(-prices, distance=distance, prominence=prominence)
    extremes_ratio = (len(peaks) + len(troughs)) / (len(prices) / distance)
    return max(0.0, min(1 - extremes_ratio, 1.0))


def is_volume_growing(df: pd.DataFrame, lookback: int = 5) -> bool:
    """
    Перевіряє, чи зростає обсяг торгів за останні записи.
    """
    if len(df) < lookback + 1:
        return False
    current_volume = df.iloc[-1]["volume"]
    recent_avg = df.iloc[-(lookback + 1):-1]["volume"].mean()
    return current_volume > recent_avg


def compute_relative_strength(price_change: float, market_trend: float) -> float:
    """
    Відносна сила (RS):

        RS = (1 + Δ_asset) / (1 + Δ_market) − 1

    ▸  RS > 0  →  актив випереджає BTC (сильніший).  
    ▸  RS < 0  →  актив відстає.  
    ▸  Якщо |Δ_market| < 1e‑9 — повертаємо 0 (уникаємо /0).
    """
    denom = 1 + market_trend
    if abs(denom) < 1e-9:
        return 0.0
    return (1 + price_change) / denom - 1



def simulate_trading_strategy(historical_data: pd.DataFrame, params: dict) -> float:
    """
    Симулює торгову стратегію на історичних даних із заданими параметрами.
    Відбирає активи за критеріями волатильності, структури тренду і відносної сили.
    Повертає середню прибутковість відібраних активів.

    Args:
        historical_data (pd.DataFrame): історичні дані активів.
        params (dict): порогові значення для відбору активів.

    Returns:
        float: середня зміна ціни відібраних активів (як метрика прибутковості).
    """
    required_columns = {'volatility', 'structure_score', 'price_change'}

    # Перевірка необхідних колонок
    missing_cols = required_columns - set(historical_data.columns)
    if missing_cols:
        logger.error(f"simulate_trading_strategy: Відсутні колонки {missing_cols}")
        return 0.0

    # Перевірка наявності колонки rel_strength
    if 'rel_strength' not in historical_data.columns:
        # Визначаємо market_trend як середню зміну ціни всіх активів
        market_trend = historical_data['price_change'].mean()
        if market_trend == 0:
            historical_data['rel_strength'] = 0.0
        else:
            historical_data['rel_strength'] = historical_data['price_change'] / market_trend
        logger.info("simulate_trading_strategy: колонку rel_strength обчислено автоматично")

    # Відбір активів за параметрами
    selected = historical_data[
        (historical_data['volatility'] >= params['min_volatility']) &
        (historical_data['structure_score'] >= params['min_structure_score']) &
        (historical_data['rel_strength'] >= params['min_rel_strength'])
    ]

    if selected.empty:
        logger.debug("simulate_trading_strategy: активи не відібрані за заданими параметрами")
        return 0.0

    average_profit = selected['price_change'].mean()
    logger.debug(f"simulate_trading_strategy: середній прибуток = {average_profit:.4f}")

    return average_profit


def objective_function(params: dict, historical_data: pd.DataFrame) -> float:
    """
    Функція цілі для оптимізації параметрів торгової стратегії.
    Повертає негативну прибутковість для мінімізації в Optuna.

    Args:
        params (dict): параметри оптимізації.
        historical_data (pd.DataFrame): історичні дані активів.

    Returns:
        float: негативна прибутковість.
    """
    profit = simulate_trading_strategy(historical_data, params)
    logger.debug(f"objective_function: розрахований прибуток = {profit:.4f}")
    return -profit



def optimize_thresholds(historical_data: pd.DataFrame) -> dict:
    """
    Використовує Optuna для оптимізації гіперпараметрів торгової стратегії.
    Оптимізуються наступні параметри:
      - min_volatility
      - min_structure_score
      - min_rel_strength
      - min_relative_volatility
    """
    def objective(trial):
        params = {
            "min_avg_volume": None,  # Ліквідність залишаємо постійною
            "min_volatility": trial.suggest_float("min_volatility", 0.005, 0.02),
            "min_structure_score": trial.suggest_float("min_structure_score", 0.6, 0.8),
            "min_rel_strength": trial.suggest_float("min_rel_strength", 0.1, 0.6),
            "min_relative_volatility": trial.suggest_float("min_relative_volatility", 0.7, 1.0)
        }
        return objective_function(params, historical_data)

    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50)
    best_params = study.best_params
    return best_params


def get_adaptive_thresholds(historical_data: pd.DataFrame) -> dict:
    """
    Повертає адаптивні порогові значення на основі оптимізації.
    """
    return optimize_thresholds(historical_data)
