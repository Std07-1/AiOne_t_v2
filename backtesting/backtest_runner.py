# backtest_runner.py
import asyncio
import aiohttp
import pandas as pd
import optuna
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import asdict

from optuna.samplers import TPESampler
from stage2.config import STAGE2_CONFIG
from data.ram_buffer import RAMBuffer
from data.cache_handler import SimpleCacheHandler
from stage1.asset_monitoring import AssetMonitorStage1
from stage2.market_analysis import Stage2Processor
from app.thresholds import Thresholds, load_thresholds, save_thresholds
from .calibration_objective import unified_objective

from rich.console import Console
from rich.logging import RichHandler

# Імпорт оптимізованого завантажувача даних
from data.raw_data import OptimizedDataFetcher


# --- Налаштування логування ---
logger = logging.getLogger("backtest_runner")
logger.setLevel(logging.DEBUG)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


class BacktestStage2Adapter(Stage2Processor):
    """
    Адаптер для Stage2Processor у бектест-середовищі.
    Імітує поведінку черги калібрування, використовуючи прямий доступ до параметрів.
    """

    def __init__(self, calibrated_params: Dict[str, Any], timeframe: str):
        """
        :param calibrated_params: Калібровані параметри для Stage2
        :param timeframe: Таймфрейм даних
        """

        # Створюємо імітовану чергу калібрування
        class MockCalibQueue:
            def __init__(self, params):
                self.params_store = params

            async def get_cached(self, symbol: str, timeframe: str) -> Dict:
                return self.params_store.get(symbol, {})

        # Передаємо імітовану чергу у батьківський конструктор
        super().__init__(MockCalibQueue(calibrated_params), timeframe)

        # Додаткові налаштування для бектесту
        self.calibrated_params = calibrated_params
        self.timeframe = timeframe

    async def process(self, stage1_signal: Dict[str, Any]) -> Dict[str, Any]:
        """Обробка сигналу з додатковим логуванням для бектесту"""
        result = await super().process(stage1_signal)
        result["backtest_mode"] = True
        result["calibrated_params"] = self.calibrated_params.get(
            stage1_signal["symbol"], "default"
        )
        return result


class TradeSimulator:
    """Симулятор торгів для бектесту"""

    def __init__(self, initial_balance: float = 10000.0):
        self.balance = initial_balance
        self.positions = {}
        self.trade_history = []
        self.trade_id_counter = 0

    async def execute(self, signal: Dict[str, Any], current_price: float):
        """Симуляція виконання торгової рекомендації"""
        symbol = signal["symbol"]
        recommendation = signal["stage2_result"]["recommendation"]
        risk_params = signal["stage2_result"].get("risk_parameters", {})

        # Якщо немає ризик-параметрів, використовуємо дефолтні
        if not risk_params:
            risk_params = {
                "tp_targets": [current_price * 1.02],
                "sl_level": current_price * 0.98,
            }

        if "BUY" in recommendation and self.balance > 0:
            return await self._execute_buy(symbol, current_price, risk_params)
        elif "SELL" in recommendation and symbol in self.positions:
            return await self._execute_sell(symbol, current_price, risk_params)

        return {"action": "HOLD"}

    async def _execute_buy(self, symbol: str, price: float, risk_params: Dict):
        """Симуляція покупки"""
        tp = risk_params["tp_targets"][0]
        sl = risk_params["sl_level"]

        # Розрахунок розміру позиції (5% від балансу)
        position_size = self.balance * 0.05
        quantity = position_size / price

        # Запис позиції
        self.trade_id_counter += 1
        self.positions[symbol] = {
            "trade_id": self.trade_id_counter,
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
            "trade_id": position["trade_id"],
            "symbol": symbol,
            "entry_price": position["entry_price"],
            "exit_price": price,
            "quantity": position["quantity"],
            "profit": profit,
            "duration": (datetime.utcnow() - position["entry_time"]).total_seconds(),
        }
        self.trade_history.append(trade_record)

        return {"action": "SELL", "profit": profit, "trade_record": trade_record}


class BacktestRunner:
    """
    Оновлений клас для запуску бектестингу з уніфікованими компонентами,
    адаптерами для Stage2 та інтегрованим симулятором торгів.
    """

    def __init__(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: str = "1m",
        initial_balance: float = 10000.0,
    ):
        """
        Ініціалізація бектестера з основними параметрами.
        :param symbols: Список торгових символів для аналізу
        :param start_date: Дата початку бектесту
        :param end_date: Дата завершення бектесту
        :param timeframe: Таймфрейм для аналізу (за замовчуванням "1m")
        :param initial_balance: Початковий баланс для симулятора торгів
        """
        self.config = STAGE2_CONFIG
        self.cache = SimpleCacheHandler()
        self.session = aiohttp.ClientSession()
        self.fetcher = OptimizedDataFetcher(
            cache_handler=self.cache, session=self.session, compress_cache=True
        )

        # Ініціалізація буферів для різних таймфреймів
        self.ram_buffer = RAMBuffer(max_bars=500)  # Швидкий буфер для останніх даних
        self.historical_buffer = RAMBuffer(max_bars=1000)  # Буфер для історичних даних

        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.timeframe = timeframe
        self.results = []  # Зберігатиме результати бектестингу
        self.calibrated_params = (
            {}
        )  # Зберігатиме калібровані параметри для кожного символу
        self.trade_simulator = TradeSimulator(
            initial_balance
        )  # Ініціалізація симулятора торгів
        self.calib_cache = {}  # Кеш для калібрування параметрів

        # Ініціалізація компонентів системи
        self.monitor = AssetMonitorStage1(
            cache_handler=SimpleCacheHandler(),
            vol_z_threshold=2.5,  # Дефолтні значення
            rsi_overbought=70,
            rsi_oversold=30,
            min_reasons_for_alert=2,
        )
        self.stage2_processor = None

        logger.info(
            "Бектестер ініціалізовано. Символи: %s, Таймфрейм: %s, Діапазон: %s - %s",
            symbols,
            timeframe,
            start_date,
            end_date,
        )

    async def load_calibrated_params(
        self, symbols: List[str], timeframe: str
    ) -> Dict[str, Any]:
        """
        Завантаження каліброваних параметрів для Stage2 з кешу або файлу.
        :param symbols: Список торгових символів
        :param timeframe: Таймфрейм для аналізу
        :return: Словник з каліброваними параметрами
        """
        calibrated_params = {}
        for symbol in symbols:
            params = await self.get_calibration(symbol)
            if params:
                calibrated_params[symbol] = params
            else:
                logger.warning(f"Не знайдено каліброваних параметрів для {symbol}")

        return calibrated_params

    async def load_historical_data(
        self, symbol: str, timeframe: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        """
        Завантаження історичних даних для символу з кешу або API.
        Пріоритет: RAM буфер -> Кеш Redis -> Пряме завантаження з API.
        :param symbol: Торговий символ
        :param timeframe: Таймфрейм
        :param start: Дата початку
        :param end: Дата завершення
        :return: DataFrame з історичними даними
        """
        logger.debug(
            f"Спроба отримати історичні дані з RAM буфера для {symbol}/{timeframe}"
        )
        # Спроба отримати з RAM буфера
        if bars := self.historical_buffer.get(symbol, timeframe, 500):
            logger.debug(
                f"Використано RAM буфер для {symbol}/{timeframe} ({len(bars)} барів)"
            )
            return pd.DataFrame(bars)

        logger.info(
            f"Завантаження історичних даних для {symbol}/{timeframe} з {start} по {end}"
        )

        # Завантаження через OptimizedDataFetcher
        try:
            data = await self.fetcher.get_data_for_calibration(
                symbol=symbol,
                interval=timeframe,
                startTime=int(start.timestamp() * 1000),
                endTime=int(end.timestamp() * 1000),
            )

            # Захист від переповнення пам'яті
            if len(data) > 1_000_000:
                logger.warning(
                    f"Надто великий обсяг даних для {symbol} ({len(data)} рядків), обмежуємо до 100k"
                )
                data = data.iloc[-100000:]

            logger.debug(f"Отримано {len(data)} рядків історичних даних для {symbol}")
        except Exception as e:
            logger.error(
                f"Помилка при завантаженні історичних даних для {symbol}: {str(e)}"
            )
            return pd.DataFrame()

        # Зберігаємо дані в буфер для подальшого використання
        if not data.empty:
            for _, row in data.iterrows():
                self.historical_buffer.add(symbol, timeframe, row.to_dict())
            logger.debug(f"Дані збережено у historical_buffer для {symbol}")

        return data

    async def update_buffer(self, current_date: datetime):
        """
        Оновлення RAM буфера даними на поточну дату для всіх символів.
        :param current_date: Поточна дата для оновлення
        """
        logger.debug(f"Оновлення буфера на дату {current_date}")
        for symbol in self.symbols:
            # Для реального часу ми б використовували WebSocket, але в бектесті імітуємо
            # Завантаження даних для поточного періоду
            try:
                bar_df = await self.fetcher.get_data_for_calibration(
                    symbol,
                    self.timeframe,
                    startTime=int(
                        (current_date - self.get_timeframe_delta()).timestamp() * 1000
                    ),
                    endTime=int(current_date.timestamp() * 1000),
                    limit=1,
                )

                if not bar_df.empty:
                    bar = bar_df.iloc[0].to_dict()
                    # Додавання нового бару до буфера
                    self.ram_buffer.add(symbol, self.timeframe, bar)
                    logger.debug(f"Додано бар до RAM буфера {symbol}: {current_date}")
                else:
                    logger.warning(
                        f"Не знайдено нових барів для {symbol} на {current_date}"
                    )
            except Exception as e:
                logger.error(f"Помилка оновлення буфера для {symbol}: {str(e)}")

    def get_timeframe_delta(self) -> timedelta:
        """
        Повертає дельту часу для поточного таймфрейму.
        :return: timedelta відповідно до self.timeframe
        """
        timeframe_deltas = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }

        delta = timeframe_deltas.get(self.timeframe)
        if not delta:
            logger.warning(
                f"Невідомий таймфрейм {self.timeframe}, використовується 1 хвилина за замовчуванням"
            )
            delta = timedelta(minutes=1)

        return delta

    @staticmethod
    def validate_calibration(best_params: dict) -> bool:
        """
        Перевірка валідності результатів калібрування.
        :param best_params: Словник з параметрами калібрування
        :return: True, якщо параметри валідні, інакше False
        """
        required_keys = {
            "vol_z_threshold",
            "atr_target",
            "low_gate",
            "high_gate",
            "rsi_oversold",
            "rsi_overbought",
        }

        # Перевірка наявності всіх ключів
        if not required_keys.issubset(best_params.keys()):
            logger.warning("Відсутні необхідні параметри калібрування")
            return False

        # Додаємо перевірку нових параметрів
        if not (0.001 <= best_params["low_gate"] <= 0.01):
            logger.warning(f"Невірний low_gate: {best_params['low_gate']}")
            return False

        if not (0.005 <= best_params["high_gate"] <= 0.05):
            logger.warning(f"Невірний high_gate: {best_params['high_gate']}")
            return False

        if best_params["low_gate"] >= best_params["high_gate"]:
            logger.warning(
                f"low_gate >= high_gate: {best_params['low_gate']} >= {best_params['high_gate']}"
            )
            return False

        # Перевірка типів даних
        type_checks = [
            (best_params["vol_z_threshold"], (float, int)),
            (best_params["atr_target"], (float, int)),
            (best_params["rsi_oversold"], (float, int)),
            (best_params["rsi_overbought"], (float, int)),
        ]

        for value, valid_types in type_checks:
            if not isinstance(value, valid_types):
                logger.warning(f"Невірний тип параметра: {type(value)}")
                return False

        # Перевірка діапазонів значень
        if not (0 < best_params["vol_z_threshold"] < 5):
            logger.warning(
                f"Невірний vol_z_threshold: {best_params['vol_z_threshold']}"
            )
            return False

        if not (0.1 < best_params["atr_target"] < 2.0):
            logger.warning(f"Невірний atr_target: {best_params['atr_target']}")
            return False

        # Додаткові перевірки
        if not (0 < best_params["rsi_oversold"] < best_params["rsi_overbought"] < 100):
            logger.warning(
                f"Невірні RSI параметри: {best_params['rsi_oversold']}-{best_params['rsi_overbought']}"
            )
            return False

        logger.debug("Параметри калібрування пройшли валідацію")
        return True

    def create_unified_study(
        self, symbol: str, timeframe: str, seed: int = 42
    ) -> optuna.study.Study:
        """
        Створення Optuna study з уніфікованими параметрами для калібрування.
        :param symbol: Торговий символ
        :param timeframe: Таймфрейм
        :param seed: Випадковий seed для відтворюваності
        :return: Optuna Study
        """
        logger.debug(f"Створення Optuna study для {symbol} ({timeframe})")
        return optuna.create_study(
            direction="maximize",
            sampler=TPESampler(
                seed=seed,
                n_startup_trials=min(30, self.config["n_trials"] // 2),
                multivariate=True,
            ),
            pruner=optuna.pruners.MedianPruner(
                n_startup_trials=20,
                n_warmup_steps=20,
                interval_steps=5,
            ),
            study_name=f"{symbol}_{timeframe}_{seed}",
        )

    async def run_calibration(
        self, symbol: str, data: pd.DataFrame
    ) -> Optional[Dict[str, Any]]:
        """
        Виконання калібрування з уніфікованими параметрами для символу.
        :param symbol: Торговий символ
        :param data: DataFrame з історичними даними
        :return: Словник параметрів або None у разі помилки
        """
        try:
            logger.info(f"Запуск калібрування для {symbol} ({len(data)} барів)")

            # Перевірка достатності даних
            if len(data) < 500:
                logger.warning(
                    f"Недостатньо даних для калібрування {symbol} ({len(data)} рядків)"
                )
                return None

            # Створення study з уніфікованими параметрами
            study = self.create_unified_study(symbol, self.timeframe)

            logger.debug("Початок оптимізації Optuna")
            study.optimize(
                lambda trial: unified_objective(
                    trial,
                    data,
                    symbol,
                    self.config,
                    self.config["metric_weights"],
                    min_trades=5,
                ),
                n_trials=self.config["n_trials"],
                show_progress_bar=True,
            )

            # Збереження найкращих параметрів
            best_params = study.best_params
            logger.info(
                f"Калібрування завершено для {symbol}. Найкращі параметри: {best_params}"
            )

            if not best_params:
                logger.warning(f"Не знайдено кращих параметрів для {symbol}")
                return None

            # Перевірка валідності
            if self.validate_calibration(best_params):
                # Створюємо об'єкт Thresholds з усіма параметрами
                thresholds = Thresholds(**best_params)
                await save_thresholds(symbol, thresholds, self.cache)
                await self.publish_calibration_results(symbol, best_params)
                logger.info(
                    f"Параметри калібрування для {symbol} збережено та опубліковано"
                )
                return best_params
            else:
                logger.error(f"Невірні параметри калібрування для {symbol}")
                return None
        except Exception as e:
            logger.exception(f"Помилка калібрування {symbol}: {str(e)}")
            return None

    async def publish_calibration_results(self, symbol: str, params: Dict[str, Any]):
        """
        Публікація результатів калібрування у Redis (імітація реальної системи).
        :param symbol: Торговий символ
        :param params: Параметри калібрування
        """
        try:
            redis_key = f"calib:{symbol}:{self.timeframe}"
            await self.cache.store_in_cache(
                symbol,
                "",
                json.dumps(params),
                prefix="calib",
                ttl=timedelta(days=7),
                publish_channel="calibration_updates",
            )
            logger.info(f"Опубліковано результати калібрування для {symbol}")
        except Exception as e:
            logger.error(
                f"Помилка публікації результатів калібрування для {symbol}: {str(e)}"
            )

    async def get_calibration(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Отримання каліброваних параметрів для символу.
        :param symbol: Торговий символ
        :return: Словник параметрів або None, якщо не знайдено
        """
        try:
            # Перевірка в локальному кеші
            if symbol in self.calibrated_params:
                return self.calibrated_params[symbol]

            # Спроба отримати з кешу
            thresholds = await load_thresholds(symbol, self.cache)
            if thresholds:
                self.calibrated_params[symbol] = thresholds.__dict__
                return self.calibrated_params[symbol]

            # Конвертуємо об'єкт Thresholds у словник
            calibrated_params = {
                "vol_z_threshold": thresholds.vol_z_threshold,
                "atr_target": thresholds.atr_target,
                "rsi_oversold": thresholds.rsi_oversold,
                "rsi_overbought": thresholds.rsi_overbought,
            }
            self.calib_cache[symbol] = calibrated_params
            return calibrated_params
        except Exception as e:
            logger.error(f"Помилка отримання калібрування для {symbol}: {str(e)}")
            return None

    async def process_symbol(
        self, symbol: str, current_date: datetime
    ) -> Optional[Dict[str, Any]]:
        """
        Обробка одного символу в конкретний момент часу.
        :param symbol: Торговий символ
        :param current_date: Поточна дата
        :return: Словник з результатами аналізу або None у разі помилки
        """
        try:
            logger.debug(f"Обробка символу {symbol} на дату {current_date}")
            # Отримання даних з буфера
            lookback = 500  # Кількість барів для аналізу
            bars = self.ram_buffer.get(symbol, self.timeframe, lookback) or []

            # Якщо в буфері недостатньо даних, завантажуємо історичні
            if not bars or len(bars) < lookback:
                logger.warning(
                    f"Недостатньо даних у буфері для {symbol}, завантаження історичних..."
                )
                start_date = current_date - timedelta(days=30)
                bars = await self.load_historical_data(
                    symbol,
                    self.timeframe,
                    start_date,
                    current_date,
                )
                # Якщо отримали DataFrame, конвертуємо у список словників
                if isinstance(bars, pd.DataFrame) and not bars.empty:
                    bars = bars.to_dict("records")
                else:
                    bars = []

            # Перетворення у DataFrame для Stage1
            df = pd.DataFrame(bars) if bars else pd.DataFrame()

            # Stage1: Генерація сирого сигналу
            if not df.empty:
                logger.debug(f"Генерація Stage1 сигналу для {symbol} на {current_date}")
                stage1_signal = await self.monitor.check_anomalies(symbol, df)

                # Детальне логування сигналу Stage1
                if stage1_signal:
                    logger.info(
                        f"[Stage1] {symbol} | Сигнал: {stage1_signal.get('signal', 'N/A')} | "
                        f"Ціна: {stage1_signal.get('stats', {}).get('current_price', 0):.6f} | "
                        f"Тригери: {len(stage1_signal.get('trigger_reasons', []))}"
                    )

                    if stage1_signal.get("trigger_reasons"):
                        logger.debug(
                            f"Причини сигналу: {', '.join(stage1_signal['trigger_reasons'])}"
                        )

                    if stage1_signal.get("stats"):
                        stats = stage1_signal["stats"]
                        logger.debug(
                            f"Метрики: RSI={stats.get('rsi', 0):.1f}, "
                            f"VolZ={stats.get('volume_z', 0):.2f}, "
                            f"ATR={stats.get('atr', 0):.4f}"
                        )
            else:
                logger.warning(f"Немає даних для обробки {symbol}")
                return None

            # Отримання каліброваних параметрів
            calibrated_params = await self.get_calibration(symbol)

            # Якщо параметри не знайдені та налаштовано калібрування
            if calibrated_params is None and self.config.get("run_calibration", False):
                logger.info(
                    f"Параметри калібрування для {symbol} не знайдено, запускається калібрування"
                )
                calibrated_params = {}
                calibrated_params = await self.run_calibration(symbol, df)
                if calibrated_params:
                    self.calibrated_params[symbol] = calibrated_params

            else:
                logger.debug(
                    f"Отримано калібровані параметри для {symbol}: {calibrated_params}"
                )

            # Якщо параметри все ще відсутні, використовуємо дефолтні
            if calibrated_params is None:
                calibrated_params = Thresholds().__dict__
                logger.info(f"Використано дефолтні параметри для {symbol}")

            # Оновлення параметрів Stage1
            self.monitor.update_params(
                vol_z_threshold=calibrated_params.get("vol_z_threshold", 2.5),
                rsi_overbought=calibrated_params.get("rsi_overbought", 70),
                rsi_oversold=calibrated_params.get("rsi_oversold", 30),
            )

            # Ініціалізація Stage2 з адаптером
            self.stage2_processor = BacktestStage2Adapter(
                {symbol: calibrated_params}, self.timeframe
            )

            # Якщо Stage1 не дав сигналу, пропускаємо символ
            if (
                not stage1_signal.get("signal") == "NORMAL"
                or "signal" not in stage1_signal
            ):
                logger.info(f"Немає сигналу Stage1 для {symbol} на {current_date}")
                return None

            # Stage2: Аналіз ринкового контексту
            logger.debug(
                f"Аналіз ринкового контексту Stage2 для {symbol}"
                f" - Тригери Stage1: {', '.join(stage1_signal.get('trigger_reasons', []))}"
                f" - Ціна: {stage1_signal['stats']['current_price']:.6f}"
                f" - RSI: {stage1_signal['stats'].get('rsi', 0):.1f}"
                f" - VolZ: {stage1_signal['stats'].get('volume_z', 0):.2f}"
                f" - ATR: {stage1_signal['stats'].get('atr', 0):.4f}"
            )
            stage2_result = await self.stage2_processor.process(stage1_signal)

            # Оновлення параметрів Stage1 з каліброваними значеннями
            self.monitor.update_params(
                vol_z_threshold=calibrated_params.get("vol_z_threshold", 2.5),
                rsi_overbought=calibrated_params.get("rsi_overbought", 70),
                rsi_oversold=calibrated_params.get("rsi_oversold", 30),
            )

            # Симуляція торгів
            current_price = stage1_signal["stats"]["current_price"]
            trade_result = await self.trade_simulator.execute(
                {"symbol": symbol, "stage2_result": stage2_result}, current_price
            )

            logger.info(f"Обробка символу {symbol} завершена на {current_date}")
            return {
                "timestamp": current_date,
                "symbol": symbol,
                "stage1_signal": stage1_signal,
                "stage2_result": stage2_result,
                "trade_result": trade_result,
                "balance": self.trade_simulator.balance,
            }
        except Exception as e:
            logger.exception(f"Помилка обробки символу {symbol}: {str(e)}")
            return None

    async def run(self) -> List[Dict[str, Any]]:
        """
        Основний цикл бектестингу.
        Повертає список результатів обробки кожного періоду.
        :return: Список результатів по кожному символу та періоду
        """
        current_date = self.start_date
        logger.info(f"Початок бектестингу з {self.start_date} по {self.end_date}")

        # Основний цикл бектестингу
        while current_date <= self.end_date:
            logger.debug(f"Обробка дати: {current_date}")

            # Оновлення буфера даними на поточну дату
            await self.update_buffer(current_date)

            # Обробка кожного символу
            symbol_results = await asyncio.gather(
                *[self.process_symbol(symbol, current_date) for symbol in self.symbols]
            )

            for result in symbol_results:
                if result:
                    self.results.append(result)
                    logger.debug(
                        f"Отримано результат для {result['symbol']} на {current_date}"
                    )
                else:
                    logger.error(f"Не вдалося отримати результат на {current_date}")

            # Перехід до наступного періоду
            current_date += self.get_timeframe_delta()

        logger.info(f"Завершено бектестинг. Отримано {len(self.results)} результатів")
        logger.info(f"Кінцевий баланс: {self.trade_simulator.balance:.2f}")
        logger.info(f"Виконано угод: {len(self.trade_simulator.trade_history)}")

        # Розрахунок прибутковості
        initial_balance = self.trade_simulator.balance + sum(
            t["profit"] for t in self.trade_simulator.trade_history
        )
        profitability = (self.trade_simulator.balance / initial_balance - 1) * 100
        logger.info(f"Прибутковість: {profitability:.2f}%")

        return self.results

    async def close(self):
        """
        Коректне закриття ресурсів (сесія aiohttp).
        """
        await self.session.close()
        logger.info("Ресурси бектестера успішно закриті")
