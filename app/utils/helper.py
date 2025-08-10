# utils/helpers.py
import pandas as pd
from typing import Optional, Dict

try:
    # якщо в тебе є мапа в конфігу — використовуємо її
    from app.config import TICK_SIZE_MAP  # optional
except Exception:
    TICK_SIZE_MAP = {}


def buffer_to_dataframe(RAMBuffer, symbol: str, limit: int = 500) -> pd.DataFrame:
    rows = RAMBuffer.get(symbol, "1m", limit)  # ← ПРАВИЛЬНО: RAMBuffer.get(...)
    if not rows:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)[["timestamp", "open", "high", "low", "close", "volume"]]
    df = df.dropna().reset_index(drop=True)
    df = df.rename(columns={"timestamp": "time"})
    return df


def resample_5m(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    Ресемпл 1m → 5m. Використовує '5min' (замість застарілого '5T').
    Повертає DF зі стовпцями: time(ms), open, high, low, close, volume.
    """
    if df_1m is None or df_1m.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    df = df_1m.copy()
    # time очікується у мілісекундах
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df = df.set_index("time").sort_index()

    o = df["open"].resample("5min").first()
    h = df["high"].resample("5min").max()
    l = df["low"].resample("5min").min()
    c = df["close"].resample("5min").last()
    v = df["volume"].resample("5min").sum()

    out = pd.concat([o, h, l, c, v], axis=1).dropna()
    out = out.reset_index()
    # повертаємо time знову у мілісекундах
    out["time"] = out["time"].astype("int64") // 10**6
    out.columns = ["time", "open", "high", "low", "close", "volume"]
    return out


def estimate_atr_pct(df_1m: pd.DataFrame) -> float:
    if df_1m is None or df_1m.empty:
        return 0.5
    tr = (
        (df_1m["high"] - df_1m["low"]).rolling(14).mean().iloc[-1]
        if len(df_1m) >= 14
        else (df_1m["high"] - df_1m["low"]).mean()
    )
    price = float(df_1m["close"].values[-1])
    return float(max(0.05, min(5.0, (tr / max(price, 1e-9)) * 100.0)))


def get_tick_size(
    symbol: str,
    price_hint: Optional[float] = None,
    overrides: Optional[Dict[str, float]] = None,
) -> float:
    """
    Повертає tick_size для символу.
    Пріоритет: overrides → TICK_SIZE_MAP → евристика за ціною.
    """
    sym = (symbol or "").lower()

    if overrides and sym in overrides:
        return float(overrides[sym])
    if isinstance(TICK_SIZE_MAP, dict) and sym in TICK_SIZE_MAP:
        return float(TICK_SIZE_MAP[sym])

    # Евристика, якщо немає довідника:
    # (Binance часто має дрібні кроки на дешевих активах і крупніші — на дорогих)
    if price_hint is not None:
        p = float(price_hint)
        if p < 0.01:
            return 1e-6
        if p < 0.1:
            return 1e-5
        if p < 1:
            return 1e-4
        if p < 10:
            return 1e-3
        if p < 100:
            return 1e-2
        if p < 1000:
            return 1e-1
        return 1.0

    # дефолт, якщо зовсім нічого не знаємо
    return 1e-3
