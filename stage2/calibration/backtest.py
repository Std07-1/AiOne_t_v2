"""backtest.py: логіка бектесту, симуляції стратегії, підрахунок метрик"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# --- Логування ---
logger = logging.getLogger("backtest")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def calculate_signal_strength(
    row: pd.Series, params: Dict[str, Any], direction: str
) -> Tuple[bool, float]:
    """Розраховує силу сигналу за системою балів"""
    score = 0.0
    max_score = 0.0

    # Визначаємо необхідні параметри з fallback-значеннями
    vol_thresh = params.get("volume_z_threshold", 1.0)
    rsi_os = params.get("rsi_oversold", 30.0)
    rsi_ob = params.get("rsi_overbought", 70.0)
    vwap_thresh = params.get("vwap_threshold", 0.005)
    macd_thresh = params.get("macd_threshold", 0.01)
    stoch_os = params.get("stoch_oversold", 20.0)
    stoch_ob = params.get("stoch_overbought", 80.0)

    # Система балів для різних факторів
    factors = []

    # Volume spike (обов'язковий фактор)
    if row.get("volume_z", 0) > vol_thresh:
        score += 2.0
        max_score += 2.0
        factors.append(f"vol_z:{row['volume_z']:.2f}")

    # Осцилятори (RSI/Stoch)
    if direction == "buy":
        if row.get("rsi", 50) < rsi_os:
            weight = max(1.5, (rsi_os - row["rsi"]) / 5)  # Динамічний вага
            score += weight
            max_score += 2.0
            factors.append(f"rsi:{row['rsi']:.1f}")
        if row.get("stoch_k", 50) < stoch_os:
            weight = max(1.0, (stoch_os - row["stoch_k"]) / 10)
            score += weight
            max_score += 1.5
            factors.append(f"stoch:{row['stoch_k']:.1f}")
    else:  # sell
        if row.get("rsi", 50) > rsi_ob:
            weight = max(1.5, (row["rsi"] - rsi_ob) / 5)
            score += weight
            max_score += 2.0
            factors.append(f"rsi:{row['rsi']:.1f}")
        if row.get("stoch_k", 50) > stoch_ob:
            weight = max(1.0, (row["stoch_k"] - stoch_ob) / 10)
            score += weight
            max_score += 1.5
            factors.append(f"stoch:{row['stoch_k']:.1f}")

    # Конфірмаційні фактори (VWAP/Bollinger/MACD)
    confirm_score = 0.0
    confirm_max = 0.0

    if direction == "buy":
        if row.get("vwap_deviation", 0) < -vwap_thresh:
            confirm_score += 1.0
            factors.append(f"vwap:{row['vwap_deviation']:.3f}")

        if "bollinger_lower" in row and row["close"] < row["bollinger_lower"]:
            confirm_score += 1.2
            factors.append("bb_lower")

        macd_diff = row.get("macd", 0) - row.get("macd_signal", 0)
        if macd_diff < -macd_thresh:
            confirm_score += 1.0
            factors.append(f"macd:{macd_diff:.4f}")
    else:  # sell
        if row.get("vwap_deviation", 0) > vwap_thresh:
            confirm_score += 1.0
            factors.append(f"vwap:{row['vwap_deviation']:.3f}")

        if "bollinger_upper" in row and row["close"] > row["bollinger_upper"]:
            confirm_score += 1.2
            factors.append("bb_upper")

        macd_diff = row.get("macd", 0) - row.get("macd_signal", 0)
        if macd_diff > macd_thresh:
            confirm_score += 1.0
            factors.append(f"macd:{macd_diff:.4f}")

    # Динамічний поріг для конфірмації
    confirm_threshold = max(
        0.8, min(1.5, score / 3)
    )  # Залежить від сили основних сигналів
    if confirm_score >= confirm_threshold:
        score += confirm_score
        max_score += confirm_threshold

    # Адаптивний мінімальний ATR (фільтр низької волатильності)
    atr_min = params.get("atr_min_percent", 0.001)
    atr_required = row["close"] * atr_min

    # Визначаємо чи дійсний сигнал
    min_score = params.get("min_score", 3.5)
    is_valid = score >= min_score and row["atr"] > atr_required and max_score > 0

    confidence = min(0.99, score / max_score) if max_score > 0 else 0.0
    return is_valid, confidence, ",".join(factors)


def run_backtest(
    df: pd.DataFrame,
    params: Dict[str, Any],
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
) -> List[Dict[str, Any]]:
    required_cols = ["volume_z", "rsi", "vwap_deviation", "atr", "close", "timestamp"]
    for col in required_cols:
        if col not in df.columns:
            logger.warning(f"Missing column: {col}")
            return []

    df = df.dropna(subset=required_cols).copy()
    if df.empty or len(df) < 20:
        logger.warning("Empty DataFrame or too short for backtest")
        return []

    # Додаємо стовпці для сигналів
    df["buy_signal"] = False
    df["sell_signal"] = False
    df["confidence"] = 0.0
    df["factors"] = ""

    # Визначаємо сигнали
    for i, row in df.iterrows():
        # Покупка
        buy_valid, buy_conf, buy_factors = calculate_signal_strength(row, params, "buy")
        if buy_valid:
            df.at[i, "buy_signal"] = True
            df.at[i, "confidence"] = buy_conf
            df.at[i, "factors"] = buy_factors

        # Продаж
        sell_valid, sell_conf, sell_factors = calculate_signal_strength(
            row, params, "sell"
        )
        if sell_valid:
            df.at[i, "sell_signal"] = True
            df.at[i, "confidence"] = max(df.at[i, "confidence"], sell_conf)
            df.at[i, "factors"] = (
                sell_factors
                if not buy_valid
                else f"{df.at[i, 'factors']}|{sell_factors}"
            )

    # Fallback на MACD якщо немає основних сигналів
    macd_fallback = params.get("macd_fallback", True)
    if macd_fallback:
        macd_diff = df["macd"] - df["macd_signal"]
        macd_threshold = params.get("macd_threshold", 0.01)

        macd_buy = (macd_diff > macd_threshold) & ~df["buy_signal"]
        macd_sell = (macd_diff < -macd_threshold) & ~df["sell_signal"]

        # Застосовуємо тільки якщо є підтвердження іншими факторами
        for i in df.index[macd_buy]:
            if df.at[i, "confidence"] > 0.4:  # Мінімальна довіра
                df.at[i, "buy_signal"] = True
                df.at[i, "factors"] += (
                    "|macd_fallback" if df.at[i, "factors"] else "macd_fallback"
                )

        for i in df.index[macd_sell]:
            if df.at[i, "confidence"] > 0.4:
                df.at[i, "sell_signal"] = True
                df.at[i, "factors"] += (
                    "|macd_fallback" if df.at[i, "factors"] else "macd_fallback"
                )

    # Симуляція торгів
    position = None
    trade_log = []

    for i, row in df.iterrows():
        current_time = row["timestamp"]

        # Закриття позиції
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

            # Тайм-аут (макс 1 день)
            if (current_time - position["entry_time"]).days > 0:
                close_position = True
                close_reason = "Time Exit"

            if close_position:
                # Розрахунок PnL
                pnl_pct = (current_price - position["entry_price"]) / position[
                    "entry_price"
                ]
                if position["direction"] == "sell":
                    pnl_pct = -pnl_pct

                trade_result = {
                    "exit_time": current_time,
                    "exit_price": current_price,
                    "pnl": pnl_pct,
                    "close_reason": close_reason,
                    "duration": (current_time - position["entry_time"]).total_seconds()
                    / 60,
                }
                trade_log.append({**position, **trade_result})
                position = None

        # Відкриття нової позиції
        if not position:
            min_confidence = params.get("min_confidence", 0.65)

            if row["buy_signal"] and row["confidence"] >= min_confidence:
                direction = "buy"
            elif row["sell_signal"] and row["confidence"] >= min_confidence:
                direction = "sell"
            else:
                direction = None

            if direction:
                entry_price = row["close"]
                atr = row["atr"]

                # Адаптивні TP/SL на основі ATR та волатильності
                tp_mult = params.get("tp_mult", 1.5)
                sl_mult = params.get("sl_mult", 1.0)

                # Корекція для низької волатильності
                volatility_factor = max(1.0, atr / (entry_price * 0.005))

                if direction == "buy":
                    tp = entry_price + tp_mult * atr * volatility_factor
                    sl = entry_price - sl_mult * atr * volatility_factor
                else:
                    tp = entry_price - tp_mult * atr * volatility_factor
                    sl = entry_price + sl_mult * atr * volatility_factor

                position = {
                    "entry_time": current_time,
                    "entry_price": entry_price,
                    "direction": direction,
                    "tp": tp,
                    "sl": sl,
                    "confidence": row["confidence"],
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "factors": row["factors"],
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
