"""
calibration.py: Optuna-логіка, objective, запуск калібрування
"""

import optuna
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import warnings
import math

warnings.filterwarnings("ignore", category=RuntimeWarning)

# Максимальне значення для метрик
MAX_METRIC_VALUE = 5.0  # Зменшили з 10 до 5


def safe_metric_value(value: float, default: float = 0.0) -> float:
    """
    Безпечний розрахунок метрики з логарифмічним стисканням.
    Якщо значення не є скінченним або NaN, повертає дефолтне значення.
    Якщо значення менше 1.0, повертає його без змін.
    Якщо більше 1.0, застосовує логарифмічне стискання.
    Якщо менше -1.0, застосовує логарифмічне стискання з від'ємним значенням.
    """
    if not np.isfinite(value):
        return default

    # Змінимо логарифмічне стискання на більш лінійне
    if abs(value) < 1.0:
        return value
    elif value > 0:
        return min(MAX_METRIC_VALUE, 1.0 + math.log1p(value - 1.0))
    else:
        return max(-MAX_METRIC_VALUE, -1.0 - math.log1p(-value - 1.0))


def calculate_sharpe(trades: list) -> float:
    if not trades or len(trades) < 3:  # Зменшили мінімум до 3 трейдів
        return 0.0
    returns = np.array([t["pnl"] for t in trades])
    if np.std(returns) == 0:
        return 0.0
    sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252)
    return safe_metric_value(sharpe)


def calculate_sortino(trades: list) -> float:
    if not trades or len(trades) < 3:
        return 0.0
    returns = np.array([t["pnl"] for t in trades])
    mean_return = np.mean(returns)
    downside_returns = returns[returns < 0]
    if len(downside_returns) == 0:
        return safe_metric_value(1.5)  # Помірне позитивне значення
    downside_risk = np.std(downside_returns)
    if downside_risk == 0:
        return safe_metric_value(1.5)
    sortino = mean_return / downside_risk * np.sqrt(252)
    return safe_metric_value(sortino)


def calculate_profit_factor(trades: list) -> float:
    """Безпечний розрахунок Profit Factor з логарифмічним стисканням"""
    if not trades:
        return 0.0
    profits = np.array([t["pnl"] for t in trades])
    gains = profits[profits > 0].sum()
    losses = -profits[profits < 0].sum()
    if losses == 0:
        return safe_metric_value(3.0)  # Помірне значення
    profit_factor = gains / losses
    return safe_metric_value(profit_factor)


def calculate_weighted_score(summary: dict, trades: list) -> float:
    """Композитний показник, що враховує кількість трейдів"""
    if not trades:
        return 0.0
    n_trades = len(trades)
    pf = calculate_profit_factor(trades)
    sharpe = calculate_sharpe(trades)
    win_rate = summary.get("win_rate", 0.0)
    # Ваги
    weights = {
        "profit_factor": 0.4,
        "sharpe": 0.3,
        "win_rate": 0.3,
    }
    # Штраф за малу кількість трейдів
    trade_count_factor = min(1.0, n_trades / 10)
    # Композитний показник
    composite = (
        weights["profit_factor"] * pf
        + weights["sharpe"] * sharpe
        + weights["win_rate"] * win_rate
    )
    return composite * trade_count_factor


def objective(
    trial: optuna.Trial,
    df: pd.DataFrame,
    base_config: Optional[Dict] = None,
    metric_weights: Optional[Dict[str, float]] = None,
    run_backtest_fn=None,
    calculate_summary_fn=None,
    metric: str = "weighted_score",  # Змінено на композитний показник
    min_trades: int = 1,  # Залишаємо мінімум 1 трейд
) -> float:
    trial.set_user_attr("symbol", "N/A")
    if df is not None and not df.empty:
        trial.set_user_attr("symbol", df.iloc[0].get("symbol", "N/A"))
        trial.set_user_attr("timeframe", df.iloc[0].get("timeframe", "N/A"))
    try:
        # Параметри з розширеними діапазонами
        volume_thresh = trial.suggest_float("volume_z_threshold", 0.1, 3.0)
        rsi_low = trial.suggest_float("rsi_oversold", 15.0, 45.0)
        rsi_high = trial.suggest_float("rsi_overbought", 55.0, 85.0)
        tp_mult = trial.suggest_float("tp_mult", 0.3, 5.0)
        sl_mult = trial.suggest_float("sl_mult", 0.1, 3.0)
        min_conf = trial.suggest_float("min_confidence", 0.3, 0.9)
        # Послаблення умови для RSI
        if rsi_low >= rsi_high - 10:  # Зменшити різницю до 10
            reason = f"Діапазон RSI замалий ({rsi_low} >= {rsi_high-10})"
            trial.set_user_attr("reason", reason)
            return 0.0
        params = {
            "volume_z_threshold": volume_thresh,
            "rsi_oversold": rsi_low,
            "rsi_overbought": rsi_high,
            "tp_mult": tp_mult,
            "sl_mult": sl_mult,
            "min_confidence": min_conf,
        }
        if base_config:
            params = {**base_config, **params}
        try:
            trades = run_backtest_fn(df, params) if run_backtest_fn else []
            if not trades:
                from .core import logger

                logger.warning(f"No trades generated for params: {params}")
                trial.set_user_attr("reason", "No trades generated")
                return 0.0
            # Додаткова перевірка якості трейдів
            pnls = [t["pnl"] for t in trades]
            if all(pnl <= 0 for pnl in pnls):
                from .core import logger

                logger.debug(f"All trades are losing for params: {params}")
                trial.set_user_attr("reason", "All trades losing")
            summary = calculate_summary_fn(trades) if calculate_summary_fn else {}
        except Exception as e:
            trial.set_user_attr("reason", f"Backtest error: {str(e)}")
            return 0.0
        # Після отримання trades
        if trades:
            trial.set_user_attr("trades_count", len(trades))
            trial.set_user_attr("avg_pnl", np.mean([t["pnl"] for t in trades]))
            trial.set_user_attr("trades_count", len(trades))
            trial.set_user_attr("win_rate", summary.get("win_rate", 0))
            trial.set_user_attr("profit_factor", summary.get("profit_factor", 0))
            trial.set_user_attr("avg_profit", np.mean([t["pnl"] for t in trades]))
        # Перевірка кількості трейдів
        total_trades = summary.get("total_trades", 0)
        if total_trades < min_trades:
            reason = f"Insufficient trades ({total_trades})"
            trial.set_user_attr("reason", reason)
            return 0.0
        # Використання композитного показника за замовчуванням
        score = calculate_weighted_score(summary, trades)
        return max(score, 0.0)
    except Exception as e:
        trial.set_user_attr("reason", f"Objective error: {str(e)}")
        return 0.0
