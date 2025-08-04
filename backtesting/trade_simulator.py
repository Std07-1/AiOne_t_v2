# trade_simulator.py

"""Симулятор торгів для бектесту"""
import logging
from datetime import datetime
from typing import Dict, Any

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("calibration_objective")
logger.setLevel(logging.DEBUG)  # Рівень логування DEBUG для детального відстеження
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


# trade_simulator.py
class TradeSimulator:
    """Симулятор торгів для бектесту"""

    def __init__(self, initial_balance: float = 10000.0):
        self.balance = initial_balance
        self.positions = {}
        self.trade_history = []

    async def execute(self, signal: Dict[str, Any], price: float):
        """Симуляція виконання торгової рекомендації"""
        symbol = signal["symbol"]
        recommendation = signal["stage2_result"]["recommendation"]
        risk_params = signal["stage2_result"]["risk_parameters"]

        if "BUY" in recommendation:
            return await self._execute_buy(symbol, price, risk_params)
        elif "SELL" in recommendation:
            return await self._execute_sell(symbol, price, risk_params)

        return {"action": "HOLD"}

    async def _execute_buy(self, symbol: str, price: float, risk_params: Dict):
        """Симуляція покупки"""
        tp = risk_params["tp_targets"][0]
        sl = risk_params["sl_level"]

        # Розрахунок розміру позиції (5% від балансу)
        position_size = self.balance * 0.05
        quantity = position_size / price

        # Запис позиції
        self.positions[symbol] = {
            "entry_price": price,
            "quantity": quantity,
            "tp": tp,
            "sl": sl,
            "entry_time": datetime.utcnow(),
        }

        # Оновлення балансу
        self.balance -= position_size

        return {
            "action": "BUY",
            "symbol": symbol,
            "quantity": quantity,
            "entry_price": price,
        }

    async def _execute_sell(self, symbol: str, price: float, risk_params: Dict):
        """Симуляція продажу"""
        if symbol not in self.positions:
            return {"action": "NO_POSITION"}

        position = self.positions.pop(symbol)
        profit = (price - position["entry_price"]) * position["quantity"]

        # Оновлення балансу
        self.balance += position["quantity"] * price

        # Запис у історію
        trade_record = {
            "symbol": symbol,
            "entry_price": position["entry_price"],
            "exit_price": price,
            "quantity": position["quantity"],
            "profit": profit,
            "duration": (datetime.utcnow() - position["entry_time"]).total_seconds(),
        }
        self.trade_history.append(trade_record)

        return {"action": "SELL", "profit": profit, "trade_record": trade_record}
