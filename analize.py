#!/usr/bin/env python3
"""
analize_summary.py

Генерує повну статистику за угодами з файлу summary_log.jsonl.
Використовує pandas для агрегації та matplotlib для візуалізацій.
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

def load_summary(path: str) -> pd.DataFrame:
    """Зчитує summary_log.jsonl у DataFrame, перевіряючи наявність."""
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise FileNotFoundError(f"{path} not found or is empty")
    return pd.read_json(path, lines=True)

def analyze_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Додає ключові колонки та виводить статистику."""
    # конвертація часу
    df["open_time"] = pd.to_datetime(df["open_time"])
    df["close_time"] = pd.to_datetime(df["close_time"])
    # додаткові колонки
    df["duration_min"] = (df["close_time"] - df["open_time"]).dt.total_seconds() / 60
    df["pred_error"]   = df["realized_profit"] - df["predicted_profit"]

    # Загальні метрики
    total = len(df)
    wins  = (df["realized_profit"] > 0).sum()
    losses= (df["realized_profit"] <= 0).sum()
    win_rate = wins / total * 100 if total else 0.0
    avg_rp   = df["realized_profit"].mean()
    avg_pp   = df["predicted_profit"].mean()
    profit_factor = (
        df[df["realized_profit"] > 0]["realized_profit"].sum()
        / -df[df["realized_profit"] < 0]["realized_profit"].sum()
        if (df["realized_profit"] < 0).any()
        else float("inf")
    )
    corr = df["predicted_profit"].corr(df["realized_profit"])

    print("\n=== OVERALL STATISTICS ===")
    print(f"Total trades: {total}")
    print(f"Wins: {wins}, Losses: {losses}, Win rate: {win_rate:.1f}%")
    print(f"Average Realized P/L: {avg_rp:.2f}%")
    print(f"Average Predicted P/L: {avg_pp:.2f}%")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Predicted vs Realized Corr: {corr:.2f}\n")

    # Описові статистики
    print("Duration (min) stats:")
    print(df["duration_min"].describe(), "\n")
    print("Prediction Error stats:")
    print(df["pred_error"].describe(), "\n")

    # Групування за символом
    symbol_stats = df.groupby("symbol")["realized_profit"].agg(
        trades="count", mean="mean", sum="sum", std="std"
    )
    print("By symbol:")
    print(symbol_stats, "\n")

    # Групування за стратегією
    strat_stats = df.groupby("strategy")["realized_profit"].agg(
        trades="count", mean="mean", sum="sum", std="std"
    )
    print("By strategy:")
    print(strat_stats, "\n")

    # Групування за годиною відкриття
    df["open_hour"] = df["open_time"].dt.hour
    hourly = df.groupby("open_hour")["realized_profit"].agg(
        trades="count", mean="mean"
    )
    print("By open hour:")
    print(hourly, "\n")

    return df

def main():
    path = "summary_log.jsonl"
    df = load_summary(path)
    analyze_summary(df)

if __name__ == "__main__":
    main()

