"""
calibration.py: Optuna-логіка, objective, запуск калібрування
Всі коментарі, docstrings, логування — українською мовою.
"""

import logging

import optuna
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import warnings
import math

from rich.logging import RichHandler
from rich.console import Console

logger = logging.getLogger("calibration")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False
warnings.filterwarnings("ignore", category=RuntimeWarning)

# Максимальне значення для метрик
MAX_METRIC_VALUE = 5.0  # Зменшили з 10 до 5


def safe_metric_value(value: float, symbol: str, default: float = 0.0) -> float:
    """
    Безпечний розрахунок метрики з логарифмічним стисканням.
    Якщо значення не є скінченним або NaN, повертає дефолтне значення.
    Якщо значення < 1.0 — повертає як є.
    Якщо > 1.0 — логарифмічне стискання.
    Якщо < -1.0 — логарифмічне стискання з від'ємним знаком.
    """
    logger.debug(f"[safe_metric_value][{symbol}] value={value}, default={default}")
    if not np.isfinite(value):
        return default

    # Змінимо логарифмічне стискання на більш лінійне
    if abs(value) < 1.0:
        return value
    elif value > 0:
        return min(MAX_METRIC_VALUE, 1.0 + math.log1p(value - 1.0))
    else:
        return max(-MAX_METRIC_VALUE, -1.0 - math.log1p(-value - 1.0))


def calculate_sharpe(trades: list, symbol: str) -> float:
    """
    Розрахунок коефіцієнта Sharpe для списку трейдів.
    Повертає скориговане значення з логарифмічним стисканням.
    """
    logger.debug(f"[calculate_sharpe][{symbol}] trades={len(trades)}")
    # Мінімум 3 трейдів для коректної метрики
    if not trades or len(trades) < 3:
        logger.info(
            f"[calculate_sharpe][{symbol}] Недостатньо трейдів для розрахунку Sharpe"
        )
        return 0.0
    returns = np.array([t["pnl"] for t in trades])  # Масив прибутків
    if np.std(returns) == 0:  # Якщо немає волатильності
        logger.info(
            f"[calculate_sharpe][{symbol}] Стандартне відхилення нульове, Sharpe=0"
        )
        return 0.0
    sharpe = np.mean(returns) / np.std(returns) * np.sqrt(252)  # Розрахунок Sharpe
    logger.debug(f"[calculate_sharpe][{symbol}] Sharpe raw={sharpe}")
    return safe_metric_value(sharpe, symbol=symbol)


def calculate_sortino(trades: list, symbol: str) -> float:
    """
    Розрахунок коефіцієнта Sortino для списку трейдів.
    Повертає скориговане значення з логарифмічним стисканням.
    """
    logger.debug(f"[calculate_sortino][{symbol}] trades={len(trades)}")
    # Мінімум 3 трейдів для коректної метрики
    if not trades or len(trades) < 3:
        logger.info(
            f"[calculate_sortino][{symbol}] Недостатньо трейдів для розрахунку Sortino"
        )
        return 0.0
    returns = np.array([t["pnl"] for t in trades])  # Масив прибутків
    mean_return = np.mean(returns)  # Середній прибуток
    downside_returns = returns[returns < 0]  # Від'ємні прибутки
    if len(downside_returns) == 0:
        logger.info(
            f"[calculate_sortino][{symbol}] Немає збиткових трейдів, Sortino=1.5"
        )
        return safe_metric_value(1.5, symbol=symbol)
    downside_risk = np.std(downside_returns)  # Волатильність збитків
    if downside_risk == 0:
        logger.info(
            f"[calculate_sortino][{symbol}] Волатильність збитків нульова, Sortino=1.5"
        )
        return safe_metric_value(1.5, symbol=symbol)
    sortino = mean_return / downside_risk * np.sqrt(252)  # Розрахунок Sortino
    logger.debug(f"[calculate_sortino][{symbol}] Sortino raw={sortino}")
    return safe_metric_value(sortino, symbol=symbol)


def calculate_profit_factor(trades: list, symbol: str) -> float:
    """
    Розрахунок Profit Factor для списку трейдів з логарифмічним стисканням.
    """
    logger.debug(f"[calculate_profit_factor][{symbol}] trades={len(trades)}")
    if not trades:
        logger.info(
            f"[calculate_profit_factor][{symbol}] Немає трейдів для розрахунку Profit Factor"
        )
        return 0.0
    profits = np.array([t["pnl"] for t in trades])
    gains = profits[profits > 0].sum()
    losses = -profits[profits < 0].sum()
    if losses == 0:
        logger.info(
            f"[calculate_profit_factor][{symbol}] Немає збитків, Profit Factor=3.0"
        )
        return safe_metric_value(3.0, symbol=symbol)
    profit_factor = gains / losses
    logger.debug(
        f"[calculate_profit_factor][{symbol}] Profit Factor raw={profit_factor}"
    )
    return safe_metric_value(profit_factor, symbol=symbol)


def calculate_weighted_score(summary: dict, trades: list, symbol: str) -> float:
    """
    Композитний показник якості стратегії, що враховує кількість трейдів.
    Враховує Profit Factor, Sharpe, Win Rate з вагами.
    """
    logger.debug(
        f"[calculate_weighted_score][{symbol}] trades={len(trades)}, summary={summary}"
    )
    if not trades:
        logger.info(
            f"[calculate_weighted_score][{symbol}] Немає трейдів для розрахунку композитного показника"
        )
        return 0.0
    n_trades = len(trades)
    pf = calculate_profit_factor(trades, symbol=symbol)
    sharpe = calculate_sharpe(trades, symbol=symbol)
    win_rate = summary.get("win_rate", 0.0)
    # Ваги для метрик
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
    logger.info(
        f"[calculate_weighted_score][{symbol}] Композитний показник: {composite * trade_count_factor:.4f} (трейдів: {n_trades})"
    )
    return composite * trade_count_factor


def objective(
    trial: optuna.Trial,
    df: pd.DataFrame,
    symbol: str,
    base_config: Optional[Dict] = None,
    metric_weights: Optional[Dict[str, float]] = None,
    run_backtest_fn=None,
    calculate_summary_fn=None,
    metric: str = "weighted_score",  # Змінено на композитний показник
    min_trades: int = (1,),  # Залишаємо мінімум 1 трейд
) -> float:
    """
    Optuna objective: основна функція для оптимізації параметрів стратегії.
    Всі логування та коментарі — українською.
    """
    logger.info(
        f"[objective][{symbol}] Запуск objective Optuna для калібрування стратегії"
    )
    df["symbol"] = symbol
    # Зберігаємо символ та таймфрейм для тріалу
    trial.set_user_attr("symbol", symbol)
    if df is not None and not df.empty:
        symbol = df.iloc[0].get("symbol", symbol)
        trial.set_user_attr("symbol", symbol)
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
            logger.warning(f"[objective][{symbol}] WARNING: {reason}")
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
                logger.warning(
                    f"[objective][{symbol}] WARNING: Жодної угоди не згенеровано для параметрів: {params}"
                )
                trial.set_user_attr("reason", "Жодної угоди не згенеровано")
                return 0.0
            # Додаткова перевірка якості трейдів
            pnls = [t["pnl"] for t in trades]
            if all(pnl <= 0 for pnl in pnls):
                logger.debug(
                    f"[objective][{symbol}] Всі угоди збиткові для параметрів: {params}"
                )
                trial.set_user_attr("reason", "Всі угоди збиткові")
            summary = calculate_summary_fn(trades) if calculate_summary_fn else {}
        except Exception as e:
            logger.error(f"[objective][{symbol}] Помилка бектесту: {str(e)}")
            trial.set_user_attr("reason", f"Помилка бектесту: {str(e)}")
            return 0.0
        # Після отримання trades
        if trades:
            trial.set_user_attr("trades_count", len(trades))
            trial.set_user_attr("avg_pnl", np.mean([t["pnl"] for t in trades]))
            trial.set_user_attr("win_rate", summary.get("win_rate", 0))
            trial.set_user_attr("profit_factor", summary.get("profit_factor", 0))
            trial.set_user_attr("avg_profit", np.mean([t["pnl"] for t in trades]))
        # Перевірка кількості трейдів
        total_trades = summary.get("total_trades", 0)
        if total_trades < min_trades:
            reason = f"Недостатньо угод ({total_trades})"
            logger.warning(f"[objective][{symbol}] WARNING: {reason}")
            trial.set_user_attr("reason", reason)
            return 0.0
        # Використання композитного показника за замовчуванням
        score = calculate_weighted_score(summary, trades, symbol=symbol)
        logger.info(f"[objective][{symbol}] Score для тріалу: {score:.4f}")
        return max(score, 0.0)
    except Exception as e:
        logger.error(f"[objective][{symbol}] Помилка objective: {str(e)}")
        trial.set_user_attr("reason", f"Помилка objective: {str(e)}")
        return 0.0
