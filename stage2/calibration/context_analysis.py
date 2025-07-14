"""
context_analysis.py: аналіз ринкового контексту, кластеризація, профіль обсягу
"""

from typing import Dict, Any
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


def evaluate_context(symbol: str, indicators_df: pd.DataFrame) -> Dict[str, Any]:
    context = {
        "symbol": symbol,
        "timestamp": pd.Timestamp.utcnow().isoformat(),
        "timeframe_analysis": {},
        "key_levels": [],
        "volume_profile": {},
        "market_phase": "neutral",
        "volatility": 0.01,
        "cluster_indicators": [],
        "trend_strength": 0.0,
        "sentiment": 0.0,
        "rsi": 50.0,
    }
    try:
        avg_volatility = indicators_df["volatility"].mean()
        recent_volatility = indicators_df["volatility"].iloc[-5:].mean()
        if recent_volatility > avg_volatility * 1.2:
            context["market_phase"] = "трендова"
        else:
            context["market_phase"] = "бічна"
        close_prices = indicators_df["close"].values.reshape(-1, 1)
        kmeans = KMeans(n_clusters=3)
        kmeans.fit(close_prices)
        clusters = kmeans.cluster_centers_
        context["key_levels"] = sorted([c[0] for c in clusters])
        avg_volume = indicators_df["volume"].mean()
        recent_volume = indicators_df["volume"].iloc[-5:].mean()
        context["volume_profile"] = {
            "average": avg_volume,
            "recent": recent_volume,
            "trend": ("висхідний" if recent_volume > avg_volume * 1.2 else "низхідний"),
        }
        context["rsi"] = indicators_df["rsi"].iloc[-1]
    except Exception as e:
        context["error"] = str(e)
    return context
