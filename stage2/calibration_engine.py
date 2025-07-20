"""calibration_engine.py"""

import asyncio
import json
import logging
import os
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import optuna
from optuna.samplers import TPESampler
from .calibration.indicators import calculate_indicators
from .calibration.backtest import run_backtest, calculate_summary
from .calibration.calibration import (
    objective,
    calculate_sharpe,
    calculate_sortino,
    safe_metric_value,
)
from .calibration.data import load_data
from .calibration.utils import unify_stage2_params

from stage2.config import STAGE2_CONFIG

# Налаштування логування
from .calibration.core import logger  # Замість локального створення


class CalibrationEngine:
    """
    Асинхронний двигун для калібрування та бектестингу торгової системи.
    Оптимізує параметри сигналів для кожного активу та таймфрейму окремо.
    """

    def __init__(
        self,
        fetcher: Any,
        redis_client,
        ram_buffer,  # Об'єднаний буфер для даних
        interval: str = "1m",
        min_bars: int = 30,
        metric: str = "profit_factor",
        calib_queue: Optional[Any] = None,
    ):
        self.fetcher = fetcher
        self.interval = interval
        self.min_bars = 500 if interval == "1m" else min_bars
        self.redis = redis_client
        self.ram_buffer = ram_buffer
        self.metric = metric
        self.calibration_results = {}
        self.symbol_seeds = {}  # Унікальний seed для кожного символу
        self.calib_queue = calib_queue
        self.config = STAGE2_CONFIG

    async def run_calibration_system(
        self,
        symbols: List[str],
        timeframes: List[str],
        date_from: datetime,
        date_to: datetime,
        n_trials: int = 50,
        config_template: Optional[Dict] = None,
        override_old: bool = True,
        parallel: bool = True,
    ) -> Dict[str, Any]:
        if parallel:
            tasks = []
            for symbol in symbols:
                for tf in timeframes:
                    task = asyncio.create_task(
                        self.calibrate_symbol_timeframe(
                            symbol,
                            tf,
                            date_from,
                            date_to,
                            n_trials,
                            config_template,
                            override_old,
                        )
                    )
                    tasks.append(task)
            results = await asyncio.gather(*tasks)
        else:
            results = []
            for symbol in symbols:
                for tf in timeframes:
                    result = await self.calibrate_symbol_timeframe(
                        symbol,
                        tf,
                        date_from,
                        date_to,
                        n_trials,
                        config_template,
                        override_old,
                    )
                    results.append(result)

        self.calibration_results = {
            f"{res['symbol']}:{res['timeframe']}": res for res in results
        }

        # --- Друк підсумку оптимізації для всіх активів ---
        logger.info("\n" + "=" * 80)
        logger.info("🎯 ПІДСУМОК КАЛІБРУВАННЯ")
        for key, res in self.calibration_results.items():
            if "error" in res:
                logger.error(f"  {key}: ❌ {res['error']}")
            else:
                oos = res.get("oos_validation", {})
                logger.info(f"  {key}:")
                logger.info(f"    Score: {res.get('best_value', 0):.4f}")
                logger.info(f"    Параметри: {res.get('best_params', {})}")
                logger.info(
                    f"    OOS: Sharpe={oos.get('sharpe', '-'):.2f}, "
                    f"Sortino={oos.get('sortino', '-'):.2f}, "
                    f"Trades={oos.get('total_trades', '-')}"
                )
        logger.info("=" * 80 + "\n")
        return self.calibration_results

    def optimization_callback(
        self, study: optuna.Study, trial: optuna.trial.FrozenTrial
    ):
        if trial.state == optuna.trial.TrialState.COMPLETE:
            logger.debug(f"Trial {trial.number} finished with value: {trial.value}")

    async def calibrate_symbol_timeframe(
        self,
        symbol: str,
        timeframe: str,
        date_from: datetime,
        date_to: datetime,
        n_trials: int,
        config_template: Optional[Dict],
        override_old: bool,
    ) -> Dict[str, Any]:
        logger.info(f"🚀 Початок калібрування для {symbol} на {timeframe} таймфреймі")
        redis_key = f"calib:{symbol}:{timeframe}"
        if not override_old:
            cached_result = await self.get_calibration_result(redis_key)
            if cached_result:
                logger.debug(
                    f"♻️ Використано кешований результат для {symbol}:{timeframe}"
                )
                return cached_result

        # Генеруємо унікальний seed для кожного символу
        if symbol not in self.symbol_seeds:
            self.symbol_seeds[symbol] = int(datetime.now().timestamp() % 1000)
        seed = self.symbol_seeds[symbol]

        # Завантаження даних
        df = await load_data(
            self.fetcher,
            self.ram_buffer,
            symbol,
            timeframe,
            date_from,
            date_to,
            self.min_bars,
        )
        if df is None or len(df) < 100:
            logger.error(f"Проблема з даними: {len(df) if df is not None else 0} барів")
            logger.debug(f"Колонки: {df.columns.tolist() if df is not None else []}")
            logger.debug(
                f"Перші рядки: {df.head(2).to_dict() if df is not None else []}"
            )
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "error": "Недостатньо даних",
                "calibration_time": datetime.utcnow().isoformat(),
            }

        logger.debug(f"📊 Завантажено {len(df)} барів для {symbol}:{timeframe}")
        try:
            # Гнучкі параметри індикаторів для коротких таймфреймів
            df = calculate_indicators(
                df,
                custom_periods={
                    "rsi_period": 10,
                    "volume_window": 30,
                    "atr_period": 10,
                },
            )
        except Exception as e:
            logger.error(f"⚠️ Помилка обчислення індикаторів: {str(e)}")
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "error": f"Indicator error: {str(e)}",
                "calibration_time": datetime.utcnow().isoformat(),
            }

        # Фільтрація критичних колонок
        critical_cols = ["volume_z", "rsi", "vwap_deviation", "atr"]
        df = df.dropna(subset=critical_cols).reset_index(drop=True)

        if len(df) < 50:
            logger.error(f"⚠️ Недостатньо даних після обробки: {len(df)} барів")
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "error": "Insufficient data after processing",
                "calibration_time": datetime.utcnow().isoformat(),
                "data_points": len(df),
            }

        # --- Optuna study з динамічними параметрами ---
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(
                seed=seed,
                n_startup_trials=min(30, n_trials // 2),  # Більше початкових спроб
                multivariate=True,  # Дозволити взаємодію параметрів
            ),
            pruner=optuna.pruners.MedianPruner(
                n_startup_trials=20,  # Збільшити з 10
                n_warmup_steps=20,  # Збільшити з 10
                interval_steps=5,
            ),
            study_name=f"{symbol}_{timeframe}_{seed}",
        )

        # --- Запуск оптимізації з розширеними параметрами ---
        study.optimize(
            lambda trial: objective(
                trial,
                df,
                config_template,
                metric_weights=getattr(self, "metric_weights", None),
                run_backtest_fn=run_backtest,
                calculate_summary_fn=calculate_summary,
                metric="weighted_score",  # Використовуємо композитний показник
                min_trades=1,  # Дозволяємо trial з 1-2 трейдами
            ),
            n_trials=n_trials,
            callbacks=[self.optimization_callback],  # Закоментувати цей рядок
            show_progress_bar=True,
        )

        completed_trials = study.get_trials(
            deepcopy=False, states=[optuna.trial.TrialState.COMPLETE]
        )
        pruned_trials = study.get_trials(
            deepcopy=False, states=[optuna.trial.TrialState.PRUNED]
        )

        logger.debug(
            f"📈 Оптимізація завершена для {symbol}:{timeframe}. "
            f"Успішних: {len(completed_trials)}, Припинено: {len(pruned_trials)}"
        )

        # Аналіз причин припинення
        reasons = {}
        if pruned_trials:
            for trial in pruned_trials:
                reason = trial.user_attrs.get("reason", "Unknown")
                reasons[reason] = reasons.get(reason, 0) + 1
            logger.warning(f"📉 Причини припинення trial: {reasons}")

        # Обробка випадку без успішних trial
        if not completed_trials:
            logger.error(f"❌ Усі trial були припинені для {symbol}:{timeframe}")
            # Спроба знайти найменш поганий параметр
            best_pruned_trial = None
            for trial in study.trials:
                if trial.value is not None and (
                    best_pruned_trial is None or trial.value > best_pruned_trial.value
                ):
                    best_pruned_trial = trial
            if best_pruned_trial:
                logger.warning(f"⚡ Використання найкращого припиненого trial")
                best_params = best_pruned_trial.params
                best_value = best_pruned_trial.value
                completed_trials = [best_pruned_trial]
            else:
                return {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "error": "All trials were pruned - no valid parameters found",
                    "calibration_time": datetime.utcnow().isoformat(),
                    "trials": n_trials,
                    "pruned_reasons": reasons,
                }
        else:
            best_params = study.best_params
            best_value = study.best_value

        # --- Додатковий повторний запуск при малій кількості вдалих спроб ---
        if len(completed_trials) < 5:
            logger.warning("Мало вдалих спроб, пробуємо розширені параметри")
            expanded_params = {
                **best_params,
                "volume_z_threshold": max(
                    1.0, best_params.get("volume_z_threshold", 2.0) * 0.7
                ),
            }
            trades = run_backtest(df, expanded_params)
            logger.debug(
                f"Додатковий запуск: {len(trades)} трейдів при volume_z_threshold={expanded_params['volume_z_threshold']}"
            )

            # Оновлення найкращих параметрів на основі додаткового запуску
            if trades:
                avg_volume_z = np.mean([t["volume_z"] for t in trades])
                avg_rsi = np.mean([t["rsi"] for t in trades])
                avg_atr = np.mean([t["atr"] for t in trades])

                best_params.update(
                    {
                        "volume_z_threshold": avg_volume_z,
                        "rsi_overbought": avg_rsi,
                        "atr_multiplier": avg_atr,
                    }
                )
                logger.info(
                    f"🔄 Оновлено найкращі параметри на основі додаткового запуску: {best_params}"
                )

        # Підсумкова інформація для кожного символу/таймфрейму
        logger.info(f"\n🔍 Результати оптимізації для {symbol}/{timeframe}:")
        logger.info(f"   - Найкращий score: {best_value:.4f}")
        logger.info(f"   - Успішні trial: {len(completed_trials)}/{n_trials}")
        logger.info(f"   - Найкращі параметри:")
        for param, value in best_params.items():
            logger.info(f"      {param}: {value:.4f}")

        # Валідація на OOS даних
        oos_metrics = await self.run_oos_validation(
            symbol, timeframe, date_to, best_params
        )

        # Додайте oos_metrics до результату
        result = {
            "symbol": symbol,
            "timeframe": timeframe,
            "recommended_params": best_params,
            "best_value": best_value,
            "trials": n_trials,
            "calibration_time": datetime.utcnow().isoformat(),
            "data_points": len(df),
            "successful_trials": len(completed_trials),
            "pruned_trials": len(pruned_trials),
            "pruned_reasons": reasons,
            "oos_validation": oos_metrics,
            "seed": seed,
        }

        # Збереження результатів у Redis
        await self.save_calibration_result(redis_key, result)

        return result

    async def get_calibration_result(self, redis_key: str) -> Optional[Dict]:
        """
        Отримати результати калібрування з Redis.
        """
        try:
            cached_data = await self.redis.get(redis_key)
            if cached_data:
                result = json.loads(cached_data)
                logger.info(f"✅ Використано кешований результат для {redis_key}")
                return result
        except Exception as e:
            logger.error(f"Помилка отримання з кешу {redis_key}: {str(e)}")
        return None

    async def save_calibration_result(self, redis_key: str, result: Dict):
        """
        Зберегти результати калібрування в Redis.
        """
        try:
            # Додаємо TTL (час життя) для кешу - 1 година
            await self.redis.set(redis_key, json.dumps(result), ex=3600)
            logger.info(f"✅ Результати калібрування збережено в кеші для {redis_key}")
        except Exception as e:
            logger.error(f"Помилка збереження в кеш {redis_key}: {str(e)}")

    async def run_oos_validation(
        self, symbol: str, timeframe: str, date_to: datetime, params: Dict
    ) -> Dict:
        """Запуск валідації на позавибіркових даних"""
        try:
            oos_date_from = date_to - timedelta(days=30)
            oos_df = await load_data(
                self.fetcher,
                self.ram_buffer,
                symbol,
                timeframe,
                oos_date_from,
                date_to,
                max(100, self.min_bars // 3),
            )
            if oos_df is None or len(oos_df) < 50:
                return {"error": "Insufficient OOS data"}
            oos_df = calculate_indicators(
                oos_df,
                custom_periods={
                    "rsi_period": 10,
                    "volume_window": 30,
                    "atr_period": 10,
                },
            )
            # Перевірка критичних колонок
            critical_cols = ["volume_z", "rsi", "vwap_deviation", "atr"]
            oos_df = oos_df.dropna(subset=critical_cols).reset_index(drop=True)
            if len(oos_df) < 30:
                return {"error": "Insufficient data after processing"}
            oos_trades = run_backtest(oos_df, params)
            if not oos_trades:
                return {"error": "No trades in OOS validation"}
            oos_summary = calculate_summary(oos_trades)
            # Нормалізація OOS метрик
            return {
                "sharpe": safe_metric_value(calculate_sharpe(oos_trades)),
                "sortino": safe_metric_value(calculate_sortino(oos_trades)),
                "profit_factor": safe_metric_value(
                    oos_summary.get("profit_factor", 0.0)
                ),
                "win_rate": safe_metric_value(oos_summary.get("win_rate", 0.0)),
                "total_trades": len(oos_trades),
            }
        except Exception as e:
            logger.error(f"Помилка OOS валідації: {str(e)}")
            return {"error": f"Validation error: {str(e)}"}

    def set_metric_weights(self, weights: Dict[str, float]):
        """
        Встановити ваги для метрик при оптимізації.
        """
        self.metric_weights = weights

    def get_metric_weights(self) -> Dict[str, float]:
        """
        Отримати поточні ваги метрик.
        """
        return getattr(self, "metric_weights", {})

    def set_param_ranges(self, ranges: Dict[str, Tuple[float, float]]):
        """
        Встановити діапазони параметрів для калібрування.
        """
        self.param_ranges = ranges

    def get_param_ranges(self) -> Dict[str, Tuple[float, float]]:
        """
        Отримати поточні діапазони параметрів.
        """
        return getattr(self, "param_ranges", {})

    def set_additional_indicators(self, indicators: List[str]):
        """
        Встановити додаткові індикатори для обчислення під час калібрування.
        """
        self.additional_indicators = indicators

    def get_additional_indicators(self) -> List[str]:
        """
        Отримати список додаткових індикаторів.
        """
        return getattr(self, "additional_indicators", [])

    def set_logging_level(self, level: int):
        """
        Встановити рівень логування.
        """
        logger.setLevel(level)

    def get_logging_level(self) -> int:
        """
        Отримати поточний рівень логування.
        """
        return logger.level

    def set_calibration_mode(self, mode: str):
        """
        Встановити режим калібрування (наприклад, 'fast', 'full').
        """
        self.calibration_mode = mode

    def get_calibration_mode(self) -> str:
        """
        Отримати поточний режим калібрування.
        """
        return getattr(self, "calibration_mode", "full")

    def set_data_source(self, source: str):
        """
        Встановити джерело даних (наприклад, 'api', 'file').
        """
        self.data_source = source

    def get_data_source(self) -> str:
        """
        Отримати поточне джерело даних.
        """
        return getattr(self, "data_source", "api")

    def set_execution_mode(self, mode: str):
        """
        Встановити режим виконання (наприклад, 'live', 'backtest').
        """
        self.execution_mode = mode

    def get_execution_mode(self) -> str:
        """
        Отримати поточний режим виконання.
        """
        return getattr(self, "execution_mode", "backtest")

    def set_slippage_model(self, model: str):
        """
        Встановити модель сліппи (наприклад, 'none', 'fixed', 'variable').
        """
        self.slippage_model = model

    def get_slippage_model(self) -> str:
        """
        Отримати поточну модель сліппи.
        """
        return getattr(self, "slippage_model", "none")

    def set_commission_model(self, model: str):
        """
        Встановити модель комісії (наприклад, 'fixed', 'percentage').
        """
        self.commission_model = model

    def get_commission_model(self) -> str:
        """
        Отримати поточну модель комісії.
        """
        return getattr(self, "commission_model", "fixed")

    def set_order_type(self, order_type: str):
        """
        Встановити тип ордеру (наприклад, 'limit', 'market').
        """
        self.order_type = order_type

    def get_order_type(self) -> str:
        """
        Отримати поточний тип ордеру.
        """
        return getattr(self, "order_type", "limit")

    def set_timeframe_alignment(self, alignment: bool):
        """
        Встановити вирівнювання таймфреймів (True/False).
        """
        self.timeframe_alignment = alignment

    def get_timeframe_alignment(self) -> bool:
        """
        Отримати поточне налаштування вирівнювання таймфреймів.
        """
        return getattr(self, "timeframe_alignment", True)

    def set_max_drawdown(self, drawdown: float):
        """
        Встановити максимальний допустимий рівень просадки (drawdown) для стратегії.
        """
        self.max_drawdown = drawdown

    def get_max_drawdown(self) -> float:
        """
        Отримати поточний рівень максимального drawdown.
        """
        return getattr(self, "max_drawdown", 0.0)
