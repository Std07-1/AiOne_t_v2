import pandas as pd
from typing import Dict, Any, List


def simulate_strategy(df: pd.DataFrame, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    trades = []
    position = None
    df = df.copy()
    df["rsi"] = _calc_rsi(df["close"], 14)
    df["atr"] = _calc_atr(df["high"], df["low"], df["close"], 14)
    df["volume_z"] = (df["volume"] - df["volume"].rolling(20).mean()) / df[
        "volume"
    ].rolling(20).std()
    for i in range(1, len(df)):
        if position:
            current_price = df.iloc[i]["close"]
            direction = position["direction"]
            entry_price = position["entry_price"]
            if (
                (direction == "BUY" and current_price >= position["tp"])
                or (direction == "BUY" and current_price <= position["sl"])
                or (direction == "SELL" and current_price <= position["tp"])
                or (direction == "SELL" and current_price >= position["sl"])
            ):
                pnl = (current_price / entry_price - 1) * (
                    1 if direction == "BUY" else -1
                )
                trades.append(
                    {
                        "entry_price": entry_price,
                        "exit_price": current_price,
                        "direction": direction,
                        "pnl": pnl,
                    }
                )
                position = None
        if not position:
            current_rsi = df.iloc[i]["rsi"]
            current_volume_z = df.iloc[i]["volume_z"]
            if (
                current_volume_z > params["volume_z_threshold"]
                and current_rsi < params["rsi_oversold"]
            ):
                position = {
                    "entry_price": df.iloc[i]["close"],
                    "direction": "BUY",
                    "tp": df.iloc[i]["close"] + params["tp_mult"] * df.iloc[i]["atr"],
                    "sl": df.iloc[i]["close"] - params["sl_mult"] * df.iloc[i]["atr"],
                }
            elif (
                current_volume_z > params["volume_z_threshold"]
                and current_rsi > params["rsi_overbought"]
            ):
                position = {
                    "entry_price": df.iloc[i]["close"],
                    "direction": "SELL",
                    "tp": df.iloc[i]["close"] - params["tp_mult"] * df.iloc[i]["atr"],
                    "sl": df.iloc[i]["close"] + params["sl_mult"] * df.iloc[i]["atr"],
                }
    return trades


def _calc_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def _calc_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int
) -> pd.Series:
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=period).mean()
    return atr


def calculate_performance_metrics(trades: List[Dict[str, Any]]) -> tuple:
    if not trades:
        return 0.0, 0.0
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    win_rate = len(wins) / len(pnls) if pnls else 0.0
    gross_profit = sum(wins) if wins else 0.01
    gross_loss = abs(sum(losses)) if losses else 0.01
    profit_factor = gross_profit / gross_loss
    return win_rate, profit_factor
