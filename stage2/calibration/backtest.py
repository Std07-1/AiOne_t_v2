"""
backtest.py: логіка бектесту, симуляції стратегії, підрахунок метрик
"""

from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd


def run_backtest(
    df: pd.DataFrame,
    params: Dict[str, Any],
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> List[Dict[str, Any]]:
    required_cols = ["volume_z", "rsi", "vwap_deviation", "atr"]
    for col in required_cols:
        if col not in df.columns:
            return []
    df = df.dropna(subset=required_cols)
    if df is None or df.empty or len(df) < 20:
        from .calibration.core import logger

        logger.warning(f"Empty DataFrame received for backtest")
        return []
    results = []
    buy_signals = (
        (df["volume_z"] > params.get("volume_z_threshold", 1.0))
        & (
            (df["rsi"] < params.get("rsi_oversold", 30.0))
            | (df["stoch_k"] < params.get("stoch_oversold", 20.0))
        )
        & (
            (df["vwap_deviation"] < -params.get("vwap_threshold", 0.005))
            | (df["close"] < df["bollinger_lower"])
            | ((df["macd"] - df["macd_signal"]) < -params.get("macd_threshold", 0.01))
        )
    )
    sell_signals = (
        (df["volume_z"] > params.get("volume_z_threshold", 1.0))
        & (
            (df["rsi"] > params.get("rsi_overbought", 70.0))
            | (df["stoch_k"] > params.get("stoch_overbought", 80.0))
        )
        & (
            (df["vwap_deviation"] > params.get("vwap_threshold", 0.005))
            | (df["close"] > df["bollinger_upper"])
            | ((df["macd"] - df["macd_signal"]) > params.get("macd_threshold", 0.01))
        )
    )
    # Логування кількості сигналів
    from .core import logger

    logger.debug(
        f"Buy signals: {buy_signals.sum()}, Sell signals: {sell_signals.sum()} for backtest"
    )
    position = None
    trade_log = []
    for i in range(len(df)):
        row = df.iloc[i]
        if position:
            current_price = row["close"]
            close_position = False
            close_reason = ""
            if position["direction"] == "buy":
                if current_price >= position["tp"]:
                    close_position = True
                    close_reason = "TP"
                elif current_price <= position["sl"]:
                    close_position = True
                    close_reason = "SL"
            elif position["direction"] == "sell":
                if current_price <= position["tp"]:
                    close_position = True
                    close_reason = "TP"
                elif current_price >= position["sl"]:
                    close_position = True
                    close_reason = "SL"
            if (row["timestamp"] - position["entry_time"]).days > 1:
                close_position = True
                close_reason = "Time Exit"
            if close_position:
                pnl = (current_price - position["entry_price"]) / position[
                    "entry_price"
                ]
                if position["direction"] == "sell":
                    pnl = -pnl
                trade_result = {
                    "exit_time": row["timestamp"],
                    "exit_price": current_price,
                    "pnl": pnl,
                    "close_reason": close_reason,
                    "duration": (
                        row["timestamp"] - position["entry_time"]
                    ).total_seconds()
                    / 60,
                }
                trade_log.append({**position, **trade_result})
                position = None
        if not position:
            confidence = (
                calculate_confidence(row, params, "buy")
                if buy_signals.iloc[i]
                else (
                    calculate_confidence(row, params, "sell")
                    if sell_signals.iloc[i]
                    else 0.0
                )
            )
            direction = (
                "buy"
                if buy_signals.iloc[i]
                else ("sell" if sell_signals.iloc[i] else None)
            )
            if direction and confidence >= params.get("min_confidence", 0.7):
                entry_price = row["close"]
                atr = row["atr"]
                if direction == "buy":
                    tp = entry_price + params["tp_mult"] * atr
                    sl = entry_price - params["sl_mult"] * atr
                else:
                    tp = entry_price - params["tp_mult"] * atr
                    sl = entry_price + params["sl_mult"] * atr
                position = {
                    "entry_time": row["timestamp"],
                    "entry_price": entry_price,
                    "direction": direction,
                    "tp": tp,
                    "sl": sl,
                    "confidence": confidence,
                    "symbol": symbol,
                    "timeframe": timeframe,
                }
    for trade in trade_log:
        entry_idx = df.index[df["timestamp"] == trade["entry_time"]][0]
        trade["factors"] = {
            "volume_z": df.loc[entry_idx, "volume_z"],
            "rsi": df.loc[entry_idx, "rsi"],
            "vwap_deviation": df.loc[entry_idx, "vwap_deviation"],
            "atr": df.loc[entry_idx, "atr"],
            "macd": df.loc[entry_idx, "macd"] - df.loc[entry_idx, "macd_signal"],
            "stoch_k": df.loc[entry_idx, "stoch_k"],
        }
    return trade_log


def calculate_summary(trade_log: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trade_log:
        return {
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_profit": 0.0,
            "max_drawdown": 0.0,
            "total_trades": 0,
        }
    profits = [t["pnl"] for t in trade_log]
    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p <= 0]
    win_rate = len(wins) / len(profits) if profits else 0.0
    gross_profit = sum(wins)
    gross_loss = -sum(losses) if losses else 1e-9
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    avg_profit = np.mean(profits) if profits else 0.0
    equity = np.cumsum(profits)
    highwater = np.maximum.accumulate(equity)
    drawdowns = highwater - equity
    max_drawdown = np.max(drawdowns) if len(drawdowns) > 0 else 0.0
    return {
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "avg_profit": avg_profit,
        "max_drawdown": max_drawdown,
        "total_trades": len(profits),
    }


def calculate_confidence(
    row: pd.Series, params: Dict[str, Any], direction: str
) -> float:
    confidence = 0.4
    if row.get("volume_z", 0) > params.get("volume_z_threshold", 2.0):
        confidence += 0.2
    if direction == "buy" and row.get("rsi", 50) < params.get("rsi_oversold", 25.0):
        confidence += 0.2
    if direction == "sell" and row.get("rsi", 50) > params.get("rsi_overbought", 75.0):
        confidence += 0.2
    if direction == "buy" and row.get("vwap_deviation", 0) < -params.get(
        "vwap_threshold", 0.005
    ):
        confidence += 0.1
    if direction == "sell" and row.get("vwap_deviation", 0) > params.get(
        "vwap_threshold", 0.005
    ):
        confidence += 0.1
    return min(confidence, 0.98)


def vectorized_backtest(
    df: pd.DataFrame, params: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Векторизований бектест: швидкий розрахунок входів, TP/SL та PnL для великих датафреймів.
    :param df: DataFrame з індикаторами
    :param params: Параметри стратегії
    :return: Список трейдів (dict)
    """
    required_cols = ["volume_z", "rsi", "vwap_deviation", "atr", "close", "timestamp"]
    for col in required_cols:
        if col not in df.columns:
            return []
    df = df.dropna(subset=required_cols)
    if df.empty or len(df) < 20:
        return []
    # --- Векторизовані сигнали входу ---
    long_entries = (
        (df["volume_z"] > params["volume_z_threshold"])
        & (
            (df["rsi"] < params["rsi_oversold"])
            | (df["stoch_k"] < params.get("stoch_oversold", 20.0))
        )
        & (
            (df["vwap_deviation"] < -params.get("vwap_threshold", 0.005))
            | (df["close"] < df.get("bollinger_lower", df["close"]))
            | ((df["macd"] - df["macd_signal"]) < -params.get("macd_threshold", 0.01))
        )
    )
    short_entries = (
        (df["volume_z"] > params["volume_z_threshold"])
        & (
            (df["rsi"] > params["rsi_overbought"])
            | (df["stoch_k"] > params.get("stoch_overbought", 80.0))
        )
        & (
            (df["vwap_deviation"] > params.get("vwap_threshold", 0.005))
            | (df["close"] > df.get("bollinger_upper", df["close"]))
            | ((df["macd"] - df["macd_signal"]) > params.get("macd_threshold", 0.01))
        )
    )
    # --- Резервні сигнали, якщо немає жодного основного сигналу ---
    if not (long_entries.any() or short_entries.any()):
        long_entries = df["macd"] > df["macd_signal"] + params.get(
            "macd_threshold", 0.01
        )
        short_entries = df["macd"] < df["macd_signal"] - params.get(
            "macd_threshold", 0.01
        )
    # --- TP/SL для кожного входу ---
    df["tp"] = np.where(
        long_entries,
        df["close"] + params["tp_mult"] * df["atr"],
        np.where(short_entries, df["close"] - params["tp_mult"] * df["atr"], np.nan),
    )
    df["sl"] = np.where(
        long_entries,
        df["close"] - params["sl_mult"] * df["atr"],
        np.where(short_entries, df["close"] + params["sl_mult"] * df["atr"], np.nan),
    )
    df["direction"] = np.where(
        long_entries, "buy", np.where(short_entries, "sell", None)
    )
    # --- Векторизований розрахунок PnL (TP/SL досягається на наступній свічці) ---
    trades = []
    entry_idxs = df.index[df["direction"].notnull()].tolist()
    for idx in entry_idxs:
        row = df.loc[idx]
        direction = row["direction"]
        entry_price = row["close"]
        tp = row["tp"]
        sl = row["sl"]
        entry_time = row["timestamp"]
        # Перевіряємо TP/SL на наступних 10 барах (можна змінити)
        exit_idx = None
        exit_price = None
        close_reason = None
        for look_ahead in range(1, 11):
            if idx + look_ahead >= len(df):
                break
            next_row = df.iloc[idx + look_ahead]
            price = next_row["close"]
            if direction == "buy":
                if price >= tp:
                    exit_idx = idx + look_ahead
                    exit_price = tp
                    close_reason = "TP"
                    break
                elif price <= sl:
                    exit_idx = idx + look_ahead
                    exit_price = sl
                    close_reason = "SL"
                    break
            elif direction == "sell":
                if price <= tp:
                    exit_idx = idx + look_ahead
                    exit_price = tp
                    close_reason = "TP"
                    break
                elif price >= sl:
                    exit_idx = idx + look_ahead
                    exit_price = sl
                    close_reason = "SL"
                    break
        if exit_idx is None:
            # Якщо TP/SL не досягнуто — вихід по часу
            exit_idx = min(idx + 10, len(df) - 1)
            exit_price = df.iloc[exit_idx]["close"]
            close_reason = "Time Exit"
        pnl = (
            (exit_price - entry_price) / entry_price
            if direction == "buy"
            else (entry_price - exit_price) / entry_price
        )
        trades.append(
            {
                "entry_time": entry_time,
                "entry_price": entry_price,
                "direction": direction,
                "tp": tp,
                "sl": sl,
                "exit_time": df.iloc[exit_idx]["timestamp"],
                "exit_price": exit_price,
                "pnl": pnl,
                "close_reason": close_reason,
                "duration": (
                    df.iloc[exit_idx]["timestamp"] - entry_time
                ).total_seconds()
                / 60,
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
            }
        )
    return trades
