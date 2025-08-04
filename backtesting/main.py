# main.py
"""
Основна асинхронна функція запуску бектестингу, аналізу результатів та генерації звітів.
"""

import logging
import asyncio
from datetime import datetime
from .backtest_runner import BacktestRunner, TradeSimulator
from .result_analyzer import ResultAnalyzer
from .report_generator import ReportGenerator


from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)  # Рівень логування DEBUG для детального відстеження
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def main():
    """
    Основна асинхронна функція запуску бектестингу, аналізу результатів та генерації звітів.
    """
    symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 5, 1)
    timeframe = "1m"

    logger.info("Початок бектестингу для символів: %s", symbols)

    # Ініціалізація бектестера
    runner = BacktestRunner(symbols, start_date, end_date, timeframe)
    logger.debug("BacktestRunner ініціалізовано")

    # Запуск основного циклу бектестингу
    logger.info("Запуск основного циклу бектестингу...")
    results = await runner.run()
    logger.info("Бектестинг завершено. Кількість результатів: %d", len(results))

    # Ініціалізація TradeSimulator для симуляції торгівлі
    simulator = TradeSimulator()
    for result in results:
        if result["stage2_result"]["recommendation"] != "AVOID":
            await simulator.execute(
                result, result["stage1_signal"]["stats"]["current_price"]
            )

    # Аналіз результатів
    logger.info("Аналіз результатів бектестингу...")
    analyzer = ResultAnalyzer(results, runner.data_loader)
    analysis_report = analyzer.analyze_accuracy()
    logger.debug("Звіт аналізу сформовано")

    # Додавання результатів симуляції торгів
    analysis_report["trading_results"] = {
        "final_balance": simulator.balance,
        "total_trades": len(simulator.trade_history),
        "profitability": simulator.balance / 10000 - 1,
    }

    # Генерація звітів
    logger.info("Генерація звітів...")
    reporter = ReportGenerator(analysis_report)
    reporter.generate_summary()
    reporter.generate_detailed_report("html")
    logger.info("Звіти збережено")

    # Вивід результатів
    print("\n=== Підсумок Backtesting ===")
    for symbol, data in reporter.items():
        print(f"\n{symbol}:")
        print(f"  Точність: {data['accuracy']:.2f}% ({data['total_signals']} сигналів)")
        print("  Точність по сценаріях:")
        for scenario, acc in data["scenario_accuracy"].items():
            print(f"    {scenario}: {acc:.2f}%")
        print("  Найефективніші тригери:")
        for trigger, count in list(data["common_triggers"].items())[:3]:
            print(f"    {trigger}: {count} разів")


if __name__ == "__main__":
    """
    Точка входу у програму. Запускає асинхронний main через asyncio.
    """
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Критична помилка виконання: {str(e)}")
