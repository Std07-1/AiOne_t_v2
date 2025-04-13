"""
monitor/asset_selector/filters.py

Модуль для фільтрації активів за різними критеріями, включаючи кореляційний фільтр.
"""
import logging
from typing import Any, Dict, List

logger = logging.getLogger("asset_selector.filters")


async def correlation_filter(
    data_fetcher: Any,
    candidates: List[Dict[str, Any]],
    reference_symbol: str = "BTCUSDT",
    correlation_threshold: float = 0.6,
    period: int = 24
) -> List[Dict[str, Any]]:
    """
    Фільтрує активи за кореляцією з референтним символом.
    Актив залишається у виборі, якщо абсолютне значення кореляції менше порогу.
    """
    selected = []
    reference_df = await data_fetcher.get_data(
        symbol=reference_symbol, interval="1h", limit=period)
    if reference_df.empty:
        logger.error("[correlation_filter] Не вдалося отримати дані для референтного символу.")
        return selected

    reference_returns = reference_df["close"].pct_change().dropna()

    for asset in candidates:
        ticker = asset.get("ticker", "Unknown")
        df = await data_fetcher.get_data(symbol=ticker, interval="1h", limit=period)
        if df.empty:
            continue
        asset_returns = df["close"].pct_change().dropna()
        correlation = reference_returns.corr(asset_returns)
        asset["correlation_with_index"] = correlation
        logger.debug(f"[correlation_filter] Кореляція {ticker} з {reference_symbol}: {correlation:.2f}")
        if abs(correlation) < correlation_threshold:
            selected.append(asset)
            logger.info(f"[correlation_filter] {ticker} пройшов кореляційний фільтр.")
        else:
            logger.debug(f"[correlation_filter] {ticker} має високу кореляцію, виключено.")
    logger.info(f"[correlation_filter] Відібрано активів: {len(selected)}")
    return selected
