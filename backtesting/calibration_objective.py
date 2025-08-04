# calibration_objective.py
"""
Модуль містить цільову функцію для Optuna, яка використовує реальні компоненти системи для оцінки ефективності параметрів калібрування.
Використовується у процесі автоматичного підбору параметрів торгової стратегії.
"""

import logging
import optuna
import pandas as pd

from stage1.asset_monitoring import AssetMonitorStage1
from stage2.market_analysis import Stage2Processor
from data.cache_handler import SimpleCacheHandler
from app.thresholds import Thresholds

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("calibration_objective")
logger.setLevel(logging.DEBUG)  # Рівень логування DEBUG для детального відстеження
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


async def unified_objective(
    trial: optuna.Trial,
    data: pd.DataFrame,
    symbol: str,
    config: dict,
    min_trades: int = 5,
) -> float:
    """
    Асинхронна цільова функція для Optuna, яка використовує реальні компоненти системи для оцінки ефективності параметрів калібрування.
    Параметри підбираються для підвищення точності та прибутковості торгової стратегії.
    :param trial: Optuna trial для оптимізації
    :param data: DataFrame з історичними даними
    :param symbol: Торговий символ
    :param config: Конфігурація з діапазонами та вагами метрик
    :param min_trades: Мінімальна кількість сигналів для валідного результату
    :return: Композитний скор (float) для Optuna
    """
    logger.debug(f"[Optuna] Початок оцінки trial для {symbol}")

    # 1. Вибір параметрів для оптимізації
    params = {
        "vol_z_threshold": trial.suggest_float(
            "vol_z_threshold", *config["optuna_ranges"]["volume_z_threshold"]
        ),
        "atr_target": trial.suggest_float(
            "atr_target", *config["optuna_ranges"]["atr_target"]
        ),
        "low_gate": trial.suggest_float(
            "low_gate", *config["optuna_ranges"]["low_gate"]
        ),
        "high_gate": trial.suggest_float(
            "high_gate", *config["optuna_ranges"]["high_gate"]
        ),
        "rsi_oversold": trial.suggest_float(
            "rsi_oversold", *config["optuna_ranges"]["rsi_oversold"]
        ),
        "rsi_overbought": trial.suggest_float(
            "rsi_overbought", *config["optuna_ranges"]["rsi_overbought"]
        ),
    }
    logger.info(f"[Optuna] Параметри для trial: {params}")

    # 2. Ініціалізація реальних компонентів системи
    monitor = AssetMonitorStage1(SimpleCacheHandler())
    processor = Stage2Processor(Thresholds(**params), config["timeframe"])
    logger.debug("Компоненти AssetMonitorStage1 та Stage2Processor ініціалізовано")

    # 3. Імітація роботи системи на історичних даних
    correct_signals = 0
    total_signals = 0
    trade_results = []

    # Проходимо по всіх барах у даних
    for i in range(len(data)):
        # Беремо поточний бар та історичні дані для аналізу
        current_bar = data.iloc[i].to_dict()
        historical_data = data.iloc[max(0, i - 500) : i]  # Останні 500 барів

        # Stage1: Генерація сирого сигналу (як у реальній системі)
        try:
            stage1_signal = await monitor.check_anomalies(symbol, historical_data)
        except Exception as e:
            logger.error(
                f"[Optuna] Помилка Stage1 для {symbol} на індексі {i}: {str(e)}"
            )
            continue

        # Якщо є сигнал - обробляємо його через Stage2
        if stage1_signal and stage1_signal.get("trigger_reasons"):
            try:
                stage2_result = await processor.process(stage1_signal)
            except Exception as e:
                logger.error(
                    f"[Optuna] Помилка Stage2 для {symbol} на індексі {i}: {str(e)}"
                )
                continue

            # Симуляція "торгівлі" на основі рекомендації
            if stage2_result["recommendation"] != "AVOID":
                # Використовуємо реальну ціну входу з сигналу
                entry_price = stage1_signal["stats"]["current_price"]

                # Знаходимо ціну через 30 хвилин (емпірична перевірка точності)
                future_index = min(i + 30, len(data) - 1)
                future_price = data.iloc[future_index]["close"]

                # Оцінюємо точність прогнозу
                price_change = (future_price - entry_price) / entry_price * 100

                if (
                    "BUY" in stage2_result["recommendation"] and price_change > 0.5
                ) or (
                    "SELL" in stage2_result["recommendation"] and price_change < -0.5
                ):
                    correct_signals += 1
                    trade_profit = abs(price_change) * 0.01  # Спрощена модель прибутку
                    logger.debug(
                        f"[Optuna] Успішний сигнал {stage2_result['recommendation']} на {i}, зміна: {price_change:.2f}%"
                    )
                else:
                    trade_profit = -abs(price_change) * 0.01  # Спрощена модель збитку
                    logger.debug(
                        f"[Optuna] Неуспішний сигнал {stage2_result['recommendation']} на {i}, зміна: {price_change:.2f}%"
                    )

                trade_results.append(trade_profit)
                total_signals += 1

    # 4. Розрахунок метрик ефективності
    if total_signals < min_trades:
        logger.warning(
            f"[Optuna] Недостатньо сигналів ({total_signals}) для {symbol}, trial штрафується"
        )
        return -1000  # Штраф за недостатню кількість сигналів

    accuracy = correct_signals / total_signals
    total_profit = sum(trade_results)

    # Композитний показник, що враховує точність та прибутковість
    composite_score = (
        config["metric_weights"]["accuracy"] * accuracy * 100
        + config["metric_weights"]["profitability"] * total_profit
    )

    logger.info(
        f"[Optuna] Trial завершено для {symbol}: accuracy={accuracy:.2f}, profit={total_profit:.2f}, score={composite_score:.2f}"
    )
    return composite_score
