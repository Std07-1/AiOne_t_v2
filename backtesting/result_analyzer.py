from data.ram_buffer import RAMBuffer

from typing import List, Dict
from datetime import datetime, timedelta


from collections import defaultdict


class ResultAnalyzer:
    def __init__(self, results: List[Dict], buffer: RAMBuffer):
        self.results = results
        self.buffer = buffer
        self.analysis_report = defaultdict(list)

    def analyze_accuracy(self):
        """Аналіз точності рекомендацій Stage2"""
        for result in self.results:
            symbol = result["symbol"]
            timestamp = result["timestamp"]
            stage2 = result["stage2_result"]

            # Отримання майбутньої ціни (наступні 30 хв)
            future_price = self.get_future_price(symbol, timestamp, minutes=30)

            # Аналіз точності прогнозу
            accuracy = self.evaluate_recommendation(
                recommendation=stage2["recommendation"],
                entry_price=result["stage1_signal"]["stats"]["current_price"],
                future_price=future_price,
            )

            # Зберігання результатів аналізу
            self.analysis_report[symbol].append(
                {
                    "timestamp": timestamp,
                    "recommendation": stage2["recommendation"],
                    "narrative": stage2["narrative"],
                    "triggers": result["stage1_signal"]["trigger_reasons"],
                    "confidence": stage2["confidence_metrics"]["composite_confidence"],
                    "accuracy": accuracy,
                    "scenario": stage2["market_context"]["scenario"],
                }
            )

        return self.analysis_report

    def evaluate_recommendation(
        self, recommendation: str, entry_price: float, future_price: float
    ) -> str:
        """Оцінка точності рекомендації"""
        price_change = (future_price - entry_price) / entry_price * 100

        if "BUY" in recommendation and price_change > 0.5:
            return "correct"
        elif "SELL" in recommendation and price_change < -0.5:
            return "correct"
        elif "AVOID" in recommendation and abs(price_change) < 0.3:
            return "correct"
        else:
            return "incorrect"

    def get_future_price(self, symbol: str, timestamp: datetime, minutes: int) -> float:
        """Отримання ціни через вказаний проміжок часу"""
        future_time = timestamp + timedelta(minutes=minutes)
        bars = self.buffer.get(symbol, "1m", 1, future_time)
        return bars[0]["close"] if bars else 0.0
