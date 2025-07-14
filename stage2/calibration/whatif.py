from typing import Dict, Any, Optional
from datetime import datetime
import pandas as pd
from .indicators import calculate_indicators
from .backtest import run_backtest, calculate_summary


async def run_whatif_analysis(
    engine,
    symbol: str,
    timeframe: str,
    new_params: Dict[str, Any],
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> Dict[str, Any]:
    base_result = await engine.get_calibration_result(f"calib:{symbol}:{timeframe}")
    if not base_result:
        return {"error": "Базові результати не знайдено"}
    if not date_from or not date_to:
        date_from = datetime.fromisoformat(base_result["data_range"]["start"])
        date_to = datetime.fromisoformat(base_result["data_range"]["end"])
    df = await engine.load_historical_data(symbol, timeframe, date_from, date_to)
    if df is None:
        return {"error": "Не вдалося завантажити дані"}
    df = calculate_indicators(df)
    new_backtest = run_backtest(df, new_params)
    new_summary = calculate_summary(new_backtest)
    base_summary = base_result["summary"]
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "new_params": new_params,
        "new_summary": new_summary,
        "comparison": {
            "win_rate": {
                "old": base_summary["win_rate"],
                "new": new_summary["win_rate"],
                "delta": new_summary["win_rate"] - base_summary["win_rate"],
            },
            "profit_factor": {
                "old": base_summary["profit_factor"],
                "new": new_summary["profit_factor"],
                "delta": new_summary["profit_factor"] - base_summary["profit_factor"],
            },
            "avg_profit": {
                "old": base_summary["avg_profit"],
                "new": new_summary["avg_profit"],
                "delta": new_summary["avg_profit"] - base_summary["avg_profit"],
            },
        },
        "backtest_results": new_backtest,
    }
