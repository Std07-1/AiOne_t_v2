# stage2/processor.py
# -*- coding: utf-8 -*-

"""
Stage2 — обробка сигналів з інтеграцією калібрування, QDE та LevelSystem v2.

Особливості:
- Чіткий порядок: corridor v2 -> (опційний) legacy fallback -> confidence -> narrative -> recommendation -> risk.
- Один-єдиний підсумковий лог-рішення [REC] на інструмент.
- Безпечне оновлення рівнів з тротлінгом (щоб не дублювати роботу продюсера).
- Акуратна інтеграція QDE як primary з керованим fallback.

PEP8 / type hints / Google-style docstrings.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Callable

from rich.console import Console
from rich.logging import RichHandler

from stage2.config import STAGE2_CONFIG

from .calibration_handler import (
    get_calibrated_params,
    default_calibration,
    apply_calibration,
)
from .validation import validate_input
from .anomaly_detector import detect_anomalies
from .context_analyzer import analyze_market_context
from .risk_manager import calculate_risk_parameters
from .confidence_calculator import calculate_confidence_metrics
from .narrative_generator import generate_trader_narrative
from .recommendation_engine import generate_recommendation
from .level_manager import LevelManager

# QDE (залишаю ваш шлях імпорту)
from QDE.quantum_decision_engine import QuantumDecisionEngine


# ──────────────────────────────────────────────────────────────────────────────
# ЛОГУВАННЯ
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("stage2.processor")
logger.setLevel(logging.INFO)
handler = RichHandler(console=Console(stderr=True), show_path=False)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False


def _safe(val: Any, default: float = 0.0) -> float:
    """Безпечне перетворення у float з фільтром NaN/Inf."""
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


class Stage2Processor:
    """
    Розширений аналіз ринкових сигналів із калібруванням, QDE, LevelSystem v2.

    Args:
        calib_queue: Черга калібрування.
        timeframe: Таймфрейм ('1m' за замовчуванням).
        state_manager: Менеджер стану (опційно).
        level_manager: Інстанс LevelManager (якщо None — створюється).
        bars_1m/bars_5m/bars_1d: Опційні словники {symbol: DataFrame} для прямої подачі барів.
        get_bars_1m/get_bars_5m/get_bars_1d: Опційні колбеки (symbol, n)->DataFrame.
        user_lang/user_style: Налаштування генератора наративу.
        levels_update_every: Мінімальний інтервал (сек) між оновленнями рівнів v2.

    Примітка:
        Якщо рівні вже централізовано оновлюються у producer — тротлінг практично завжди пропустить локальне оновлення.
    """

    def __init__(
        self,
        calib_queue: Any,
        timeframe: str = "1m",
        state_manager: Any = None,
        level_manager: Optional[LevelManager] = None,
        bars_1m: Optional[Dict[str, Any]] = None,
        bars_5m: Optional[Dict[str, Any]] = None,
        bars_1d: Optional[Dict[str, Any]] = None,
        get_bars_1m: Optional[Callable[[str, int], Any]] = None,
        get_bars_5m: Optional[Callable[[str, int], Any]] = None,
        get_bars_1d: Optional[Callable[[str], Any]] = None,
        user_lang: str = "UA",
        user_style: str = "explain",
        levels_update_every: int = 25,
    ):
        self.user_lang = user_lang
        self.user_style = user_style
        self.config: Dict[str, Any] = STAGE2_CONFIG

        self.calib_queue = calib_queue
        self.timeframe = timeframe
        self._state_manager = state_manager

        self.level_manager: LevelManager = level_manager or LevelManager()
        self.calib_cache: Dict[str, Dict[str, Any]] = {}
        self.default_calib_count = 0

        # QDE кеш
        self._qde_cache: Dict[Tuple[str, str], QuantumDecisionEngine] = {}

        # Перемикачі
        switches = self.config.get("switches") or {}
        self.switches = switches
        self.sw_analysis: Dict[str, bool] = switches.get("analysis", {})
        self.sw_qde: bool = switches.get("use_qde", True)
        self.sw_legacy_fallback: bool = switches.get("legacy_fallback", True)
        self.sw_fallback_when: Dict[str, Any] = switches.get(
            "fallback_when", {"qde_scenario_uncertain": True, "qde_conf_lt": 0.55}
        )
        self.sw_qde_levels: Dict[str, bool] = switches.get(
            "qde_levels", {"micro": True, "meso": True, "macro": True}
        )

        # Джерела барів
        self.bars_1m = bars_1m or {}
        self.bars_5m = bars_5m or {}
        self.bars_1d = bars_1d or {}
        self.get_bars_1m = get_bars_1m
        self.get_bars_5m = get_bars_5m
        self.get_bars_1d = get_bars_1d

        # Тротлінг оновлення рівнів
        self._levels_last_update: Dict[str, int] = {}
        self.levels_update_every = max(5, int(levels_update_every))  # сек

        logger.debug("Stage2Processor ініціалізовано, TF=%s", timeframe)

    # ────────────────────────────────────────────────────────────────────
    # ВНУТРІШНІ ДОПОМОЖНІ
    # ────────────────────────────────────────────────────────────────────
    def _get_qde(
        self, symbol: str, normal_state: Optional[Dict[str, float]] = None
    ) -> QuantumDecisionEngine:
        key = (symbol, self.timeframe)
        if key not in self._qde_cache:
            self._qde_cache[key] = QuantumDecisionEngine(
                symbol, normal_state=normal_state
            )
        return self._qde_cache[key]

    def _get_current_price(self, _: str) -> Optional[float]:
        """Заглушка — якщо потрібно, інжектуй зовнішній фід ціни."""
        return None

    def _maybe_fetch_bars(self, symbol: str) -> Tuple[Any, Any, Any]:
        """
        Повертає (df_1m, df_5m, df_1d) якщо доступні (словник або колбек), інакше (None,...).
        Колбеки мають пріоритет.
        """
        df_1m = (
            self.get_bars_1m(symbol, 500)
            if callable(self.get_bars_1m)
            else self.bars_1m.get(symbol)
        )
        df_5m = (
            self.get_bars_5m(symbol, 500)
            if callable(self.get_bars_5m)
            else self.bars_5m.get(symbol)
        )
        df_1d = (
            self.get_bars_1d(symbol)
            if callable(self.get_bars_1d)
            else self.bars_1d.get(symbol)
        )
        return df_1m, df_5m, df_1d

    def _update_levels_if_needed(self, symbol: str, stats: Dict[str, Any]) -> None:
        """
        Оновлює LevelSystem v2 з тротлінгом, щоб уникати зайвої роботи.
        Безпечний (обгорнуто try/except).
        """
        now_ts = int(time.time())
        last = self._levels_last_update.get(symbol, 0)
        if (now_ts - last) < self.levels_update_every:
            return

        try:
            price = float(stats.get("current_price") or 0.0)
            atr = float(stats.get("atr") or 0.0)
            if price > 0:
                atr_pct = (atr / price) * 100.0
            else:
                atr_pct = 0.5

            tick_size = stats.get("tick_size")

            self.level_manager.update_meta(symbol, atr_pct=atr_pct, tick_size=tick_size)

            df_1m, df_5m, df_1d = self._maybe_fetch_bars(symbol)
            # не обов'язково оновлювати всі TF кожного разу — але безпечно
            self.level_manager.update_from_bars(
                symbol, df_1m=df_1m, df_5m=df_5m, df_1d=df_1d
            )

            self._levels_last_update[symbol] = now_ts
        except Exception as e:
            logger.debug("Level update skipped for %s: %s", symbol, e)

    # ────────────────────────────────────────────────────────────────────
    # ОСНОВНИЙ ПАЙПЛАЙН
    # ────────────────────────────────────────────────────────────────────
    async def process(self, stage1_signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Обробка сигналу Stage1:
        validate -> calibration -> (optional) level-update -> anomalies ->
        QDE|legacy context -> corridor v2 injection -> (optional) legacy fallback ->
        confidence -> narrative -> recommendation -> risk -> single [REC] log.
        """
        logger.debug("Початок обробки сигналу Stage1")
        try:
            # ── INIT
            anomalies: Dict[str, Any] = {}
            confidence: Dict[str, float] = {"composite_confidence": 0.0}
            risk_params: Dict[str, Any] = {}
            narrative: str = ""
            recommendation: str = "AVOID"

            # ── VALIDATE
            validate_input(stage1_signal["stats"])

            # ── SYMBOL / STATS
            stats = dict(stage1_signal["stats"])
            stats.setdefault("symbol", stage1_signal.get("symbol", "UNKNOWN"))
            symbol: str = str(stats["symbol"])
            logger.debug("Обробка сигналу для символу: %s", symbol)

            # ── CALIBRATION
            calibrated_params = await get_calibrated_params(
                symbol=symbol,
                timeframe=self.timeframe,
                calib_cache=self.calib_cache,
                calib_queue=self.calib_queue,
                get_current_price_func=self._get_current_price,
                config=self.config,
            )
            if not calibrated_params:
                logger.warning("Використано дефолтні параметри калібрування")
                calibrated_params = default_calibration(self.config)
                calibrated_params["is_default"] = True
                self.default_calib_count += 1

            # ── APPLY CALIBRATION
            calibrated_signal = apply_calibration(stage1_signal, calibrated_params)
            stats = calibrated_signal["stats"]

            # ── LEVELS UPDATE (throttled)
            if self.level_manager is not None:
                self._update_levels_if_needed(symbol, stats)

            # ── ANOMALIES
            if self.sw_analysis.get("anomaly", True):
                anomalies = detect_anomalies(
                    stats, calibrated_signal["trigger_reasons"], calibrated_params
                )

            # ── CONTEXT: QDE primary (optional) + legacy context (optional for fallback)
            final_context: Dict[str, Any] = {}
            legacy_ctx: Optional[Dict[str, Any]] = None

            if self.sw_qde:
                normal_state = {
                    "volatility": max(
                        1e-6,
                        _safe(stats.get("atr"))
                        / max(1e-9, _safe(stats.get("current_price"), 1.0)),
                    ),
                    "spread": _safe(stats.get("bid_ask_spread"), 0.0008),
                    "correlation": _safe(stats.get("correlation"), 0.5),
                }
                qde = self._get_qde(symbol, normal_state=normal_state)
                qde_ctx = qde.quantum_decision(
                    stats, trigger_reasons=calibrated_signal["trigger_reasons"]
                )

                final_context = {
                    **qde_ctx,
                    "symbol": symbol,
                    "stats": stats,
                    "current_price": stats["current_price"],
                    # ключові рівні підкладемо після corridor v2
                }

                if self.sw_legacy_fallback and self.sw_analysis.get("context", True):
                    legacy_ctx = analyze_market_context(
                        stats=stats,
                        calibrated_params=calibrated_params,
                        level_manager=self.level_manager,
                        trigger_reasons=calibrated_signal["trigger_reasons"],
                        anomalies=anomalies,
                    )
            else:
                # legacy-only
                final_context = analyze_market_context(
                    stats=stats,
                    calibrated_params=calibrated_params,
                    level_manager=self.level_manager,
                    trigger_reasons=calibrated_signal["trigger_reasons"],
                    anomalies=anomalies,
                )
                final_context["symbol"] = symbol
                final_context["stats"] = stats

            # ── CORRIDOR V2 INJECTION (must be BEFORE confidence/reco/risk)
            corr = (
                self.level_manager.get_corridor(
                    symbol=symbol,
                    price=stats["current_price"],
                    daily_low=stats.get("daily_low"),
                    daily_high=stats.get("daily_high"),
                )
                if self.level_manager
                else {}
            )

            kl = final_context.get("key_levels") or {}
            final_context["key_levels"] = {
                "immediate_support": kl.get("immediate_support") or corr.get("support"),
                "immediate_resistance": kl.get("immediate_resistance")
                or corr.get("resistance"),
                "next_major_level": kl.get("next_major_level") or corr.get("mid"),
            }
            final_context["key_levels_meta"] = {
                "band_pct": corr.get("band_pct"),
                "confidence": corr.get("confidence"),
                "mid": corr.get("mid"),
                "dist_to_support_pct": corr.get("dist_to_support_pct"),
                "dist_to_resistance_pct": corr.get("dist_to_resistance_pct"),
            }

            # ── OPTIONAL LEGACY FALLBACK (after corridor injection)
            if self.sw_qde and self.sw_legacy_fallback and legacy_ctx:
                qde_scn = final_context.get("scenario", "UNCERTAIN")
                qde_conf = _safe(final_context.get("confidence"), 0.0)
                cond_unc = self.sw_fallback_when.get(
                    "qde_scenario_uncertain", True
                ) and (qde_scn == "UNCERTAIN")
                cond_lowc = qde_conf < float(
                    self.sw_fallback_when.get("qde_conf_lt", 0.55)
                )
                if cond_unc or cond_lowc:
                    # підміняємо весь контекст на legacy, але зберігаємо коридор v2
                    legacy_ctx = dict(legacy_ctx)
                    legacy_ctx["key_levels"] = final_context["key_levels"]
                    legacy_ctx["key_levels_meta"] = final_context["key_levels_meta"]
                    final_context = {
                        **legacy_ctx,
                        "symbol": symbol,
                        "stats": stats,
                        "qde_overlay": {
                            "scenario": qde_scn,
                            "confidence": qde_conf,
                            "weights": final_context.get("weights"),
                            "decision_matrix": final_context.get("decision_matrix"),
                        },
                    }

            # ── CONTEXT LOG (after corridor injection / possible fallback)
            sup = final_context["key_levels"].get("immediate_support")
            res = final_context["key_levels"].get("immediate_resistance")
            band = final_context.get("key_levels_meta", {}).get("band_pct")
            dsp = final_context.get("key_levels_meta", {}).get("dist_to_support_pct")
            drp = final_context.get("key_levels_meta", {}).get("dist_to_resistance_pct")
            logger.info(
                "[CTX.v2] %s support=%s resistance=%s band=%s%% distS=%s%% distR=%s%%",
                symbol,
                f"{float(sup):.6f}" if isinstance(sup, (int, float)) else "nan",
                f"{float(res):.6f}" if isinstance(res, (int, float)) else "nan",
                f"{float(band):.3f}" if isinstance(band, (int, float)) else "nan",
                f"{float(dsp):.2f}" if isinstance(dsp, (int, float)) else "nan",
                f"{float(drp):.2f}" if isinstance(drp, (int, float)) else "nan",
            )

            try:
                sup = final_context["key_levels"].get("immediate_support")
                res = final_context["key_levels"].get("immediate_resistance")
                s_ev = (
                    self.level_manager.evidence_around(symbol, sup, pct_window=0.12)
                    if isinstance(sup, (int, float))
                    else {}
                )
                r_ev = (
                    self.level_manager.evidence_around(symbol, res, pct_window=0.12)
                    if isinstance(res, (int, float))
                    else {}
                )
                final_context["level_evidence"] = {"support": s_ev, "resistance": r_ev}
            except Exception:
                final_context["level_evidence"] = {"support": {}, "resistance": {}}

            # Позначити джерело контексту
            if self.sw_qde:
                if (
                    self.sw_legacy_fallback
                    and legacy_ctx
                    and "qde_overlay" in final_context
                ):
                    final_context["context_source"] = "LEGACY_FALLBACK"
                else:
                    final_context["context_source"] = "QDE_PRIMARY"
            else:
                final_context["context_source"] = "LEGACY_ONLY"

            logger.info(
                "[SRC] %s source=%s scen=%s qde_conf=%.3f",
                symbol,
                final_context["context_source"],
                str(final_context.get("scenario")),
                _safe(
                    final_context.get("confidence"),
                    _safe(final_context.get("qde_overlay", {}).get("confidence")),
                ),
            )

            # ---- PROBABILITY NORMALIZATION (ensure keys for confidence_calculator) ----
            dm = final_context.get("decision_matrix") or {}
            km = final_context.get("key_levels_meta", {}) or {}
            scenario = final_context.get("scenario", "UNCERTAIN")
            band = km.get("band_pct")

            # 1) беремо, якщо дав QDE:
            bp = final_context.get("breakout_probability", dm.get("BULLISH_BREAKOUT"))
            pp = final_context.get("pullback_probability", dm.get("BEARISH_REVERSAL"))

            # 2) якщо ні — інферимо з сценарію + ширини коридора
            def _infer_bp_pp(
                scn: str, band_pct: Optional[float]
            ) -> tuple[float, float]:
                # помірні дефолти
                base_bp, base_pp = 0.30, 0.30
                if scn == "BULLISH_BREAKOUT":
                    return 0.75, 0.25
                if scn == "BEARISH_REVERSAL":
                    return 0.25, 0.75
                if scn == "RANGE_BOUND":
                    if isinstance(band_pct, (int, float)):
                        if band_pct < 0.08:  # мікро-флет
                            return 0.20, 0.30
                        if band_pct < 0.20:  # помірний флет
                            return 0.35, 0.35
                        return 0.45, 0.40  # широкий діапазон
                    return base_bp, base_pp
                # MANIPULATED/UNCERTAIN/інше
                return base_bp, base_pp

            if bp is None or pp is None:
                ibp, ipp = _infer_bp_pp(scenario, band)
                bp = ibp if bp is None else bp
                pp = ipp if pp is None else pp

            # 3) нормалізуємо й кладемо в контекст
            def _clamp01(x: float) -> float:
                try:
                    x = float(x)
                except Exception:
                    x = 0.0
                return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

            final_context["breakout_probability"] = _clamp01(bp)
            final_context["pullback_probability"] = _clamp01(pp)

            # ── CONFIDENCE → NARRATIVE → RECOMMENDATION → RISK (in that order)
            if self.sw_analysis.get("confidence", True):
                confidence = calculate_confidence_metrics(
                    final_context, calibrated_signal["trigger_reasons"]
                )

            if self.sw_analysis.get("narrative", True):
                narrative = generate_trader_narrative(
                    final_context,
                    anomalies,
                    calibrated_signal["trigger_reasons"],
                    stats=stats,
                    lang=self.user_lang,
                    style=self.user_style,
                )
                try:
                    preview = (narrative or "").replace("\n", " ")
                    logger.info(
                        "[NARR] %s %s", symbol, preview if preview else "(empty)"
                    )
                except Exception:
                    logger.debug("[NARR] %s (logging failed)", symbol)

            if self.sw_analysis.get("recommendation", True):
                recommendation = generate_recommendation(final_context, confidence)

            if self.sw_analysis.get("risk", True):
                risk_params = calculate_risk_parameters(
                    stats, final_context, calibrated_params
                )

            # ── SINGLE SUMMARY LOG
            tp_str = (
                ",".join(
                    f"{tp:.6f}" for tp in (risk_params.get("tp_targets") or [])[:3]
                )
                if risk_params
                else ""
            )
            sl_str = (
                f"{float(risk_params.get('sl_level')):.6f}"
                if risk_params and risk_params.get("sl_level") is not None
                else "nan"
            )
            rr_str = (
                f"{float(risk_params.get('risk_reward_ratio')):.2f}"
                if risk_params and risk_params.get("risk_reward_ratio") is not None
                else "nan"
            )
            logger.info(
                "[REC] %s scenario=%s composite=%.3f reco=%s tp=%s sl=%s rr=%s",
                symbol,
                final_context.get("scenario"),
                float(confidence.get("composite_confidence", 0.0)),
                recommendation,
                tp_str,
                sl_str,
                rr_str,
            )

            # ── QUALITY / RETURN
            quality_metrics = {
                "used_default_calib": calibrated_params.get("is_default", False),
                "default_calib_count": self.default_calib_count,
                "processing_time": datetime.utcnow().isoformat(),
            }

            return {
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "market_context": final_context,
                "risk_parameters": risk_params,
                "confidence_metrics": confidence,
                "anomaly_detection": anomalies,
                "narrative": narrative,
                "trader_narrative": narrative,
                "recommendation": recommendation,
                "calibration_params": calibrated_params,
                "quality_metrics": quality_metrics,
            }

        except ValueError as e:
            logger.error("Помилка валідації: %s", e)
            return {
                "error": str(e),
                "symbol": stage1_signal.get("symbol", "UNKNOWN"),
                "recommendation": "AVOID",
                "scenario": "INVALID_DATA",
                "narrative": "Помилка валідації вхідних даних",
            }
        except Exception as e:
            logger.exception("Критична помилка обробки: %s", e)
            return {
                "error": "SYSTEM_FAILURE",
                "symbol": stage1_signal.get("symbol", "UNKNOWN"),
                "recommendation": "AVOID",
                "scenario": "SYSTEM_FAILURE",
                "narrative": "Критична системна помилка",
            }
