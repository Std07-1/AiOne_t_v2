# asset_monitor

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from io import StringIO
from typing import Any, Dict, List, Optional

import aiohttp
import numpy as np
import pandas as pd
from scipy.signal import find_peaks

from .asset_selector.utils import (
    standardize_format,
    format_volume_usd,
    format_open_interest,
)
from .asset_monitor.config import TELEGRAM_TOKEN, ADMIN_ID
from rich.console import Console
from rich.logging import RichHandler

# Отримуємо консоль Rich
console = Console()

# Налаштування логування
logger = logging.getLogger("asset_monitor")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    rich_handler = RichHandler(console=console, level=logging.DEBUG, show_path=False)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    rich_handler.setFormatter(formatter)
    logger.addHandler(rich_handler)
logger.propagate = False


# ──────────────────── Технічні функції ────────────────────────────
def compute_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """
    Обчислює індекс відносної сили (RSI) з використанням експоненціального ковзного середнього (EMA).
    
    Аргументи:
        series (pd.Series): Серія цін.
        window (int): Розмір вікна для обчислення EMA (за замовчуванням 14).
        
    Повертає:
        pd.Series: RSI для кожного періоду.
    
    Примітка:
        Якщо середній приріст (avg_gain) і середня втрата (avg_loss) рівні 0, RSI встановлюється на 50,
        що вказує на відсутність тенденції. Якщо avg_loss дорівнює 0, а avg_gain > 0, RSI встановлюється на 100.
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    avg_gain = gain.ewm(alpha=1/window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/window, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    # Обробка випадків, коли обидва показники рівні 0: відсутність зміни
    both_zero = (avg_gain == 0) & (avg_loss == 0)
    rsi[both_zero] = 50
    # Якщо avg_loss == 0, але avg_gain > 0: перекупленість
    loss_zero = (avg_loss == 0) & (~both_zero)
    rsi[loss_zero] = 100

    return rsi


def compute_vwap(df: pd.DataFrame) -> float:
    """
    Розраховує Volume Weighted Average Price (VWAP) для заданого DataFrame.

    Аргументи:
        df (pd.DataFrame): Дані, які містять стовпці 'close' (ціни закриття) та 'volume' (обсяги).

    Повертає:
        float: Обчислений VWAP. Якщо сумарний обсяг дорівнює 0, повертає np.nan.
    """
    total_volume = df["volume"].sum()
    if total_volume == 0:
        return np.nan
    return (df["close"] * df["volume"]).sum() / total_volume


def compute_atr(df: pd.DataFrame, window: int = 14) -> float:
    """
    Розраховує середній справжній діапазон (ATR) для заданого DataFrame.

    Аргументи:
        df (pd.DataFrame): Дані з колонками "high", "low", "close".
        window (int): Кількість періодів для розрахунку ATR (за замовчуванням 14).

    Повертає:
        float: Значення ATR для останнього періоду. 
               Якщо недостатньо даних для розрахунку, повертається np.nan.
    """
    # Перевірка, чи є дані та чи достатньо їх для розрахунку
    if df.empty or len(df) < window:
        return np.nan

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()

    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=window, min_periods=window).mean().iloc[-1]
    return atr

def compute_sharpe_ratio(
    df: pd.DataFrame,
    risk_free_rate: float = 0.0,
    trading_days: int = 252,
    use_log_returns: bool = False
) -> float:
    """
    Розраховує коефіцієнт Шарпа за допомогою щоденних змін ціни.

    Аргументи:
        df (pd.DataFrame): Дані з колонкою "close", що містить ціни закриття.
        risk_free_rate (float): Безризикова ставка річного доходу (наприклад, 0.02 для 2%).
        trading_days (int): Кількість торгових днів на рік (за замовчуванням 252).
        use_log_returns (bool): Якщо True, використовуємо логарифмічні доходності замість процентних змін.

    Повертає:
        float: Розрахований коефіцієнт Шарпа. Якщо стандартне відхилення доходностей рівне 0, повертається np.nan.
    """
    # Обчислення доходностей: звичайні або логарифмічні
    if use_log_returns:
        returns = np.log(df["close"]).diff().dropna()
    else:
        returns = df["close"].pct_change().dropna()

    # Розрахунок надлишкових доходностей (за торговий день)
    excess_returns = returns - (risk_free_rate / trading_days)
    std_excess = excess_returns.std()
    
    if std_excess == 0:
        return np.nan

    # Перемножуємо на sqrt(trading_days) для річного коефіцієнту Шарпа
    sharpe_ratio = (excess_returns.mean() / std_excess) * np.sqrt(trading_days)
    return sharpe_ratio


def format_rsi(rsi: float, bar_length: int = 10) -> str:
    """
    Форматує значення RSI у вигляді прогрес-бару з фіксованою довжиною.
    
    Аргументи:
        rsi (float): Значення RSI від 0 до 100.
        bar_length (int): Загальна довжина прогрес-бару (кількість символів).
        
    Повертає:
        str: Рядок з прогрес-баром та відформатованим значенням RSI.
        
    Приклад:
        format_rsi(21.23) -> "[██░░░░░░░░] 21.2"
        format_rsi(50.91) -> "[█████░░░░░] 50.9"
        format_rsi(69.32) -> "[███████░░░] 69.3"
    """
    # Розрахунок кількості заповнених сегментів (ціле число)
    filled_length = int(round((rsi / 100) * bar_length))
    # Формування рядка з заповненими та незаповненими сегментами
    bar = "█" * filled_length + "░" * (bar_length - filled_length)
    return f"[{bar}] {rsi:.1f}"


def find_local_levels(
    prices: pd.Series, distance: int = 5, prominence: float = 1.0
) -> Dict[str, np.ndarray]:
    """
    Визначає локальні екстремуми (максимуми та мінімуми) у серії цін.

    Аргументи:
        prices (pd.Series): Серія цін.
        distance (int): Мінімальна відстань між піками (за замовчуванням 5).
        prominence (float): Мінімальна виразність піку (за замовчуванням 1.0).

    Повертає:
        Dict[str, np.ndarray]: Словник з двома ключами:
            - 'peaks': Індекси локальних максимумів.
            - 'troughs': Індекси локальних мінімумів.
    """
    # Якщо серія порожня, повертаємо порожні масиви
    if prices.empty:
        return {"peaks": np.array([]), "troughs": np.array([])}

    peaks, _ = find_peaks(prices, distance=distance, prominence=prominence)
    troughs, _ = find_peaks(-prices, distance=distance, prominence=prominence)
    return {"peaks": peaks, "troughs": troughs}


def compute_correlation(asset_df: pd.DataFrame, reference_df: pd.DataFrame) -> float:
    """
    Обчислює кореляцію доходностей активу з доходностями референтного символу.

    Аргументи:
        asset_df (pd.DataFrame): Дані активу з колонкою "close".
        reference_df (pd.DataFrame): Дані референтного символу з колонкою "close".

    Повертає:
        float: Кореляція доходностей. Якщо дані недоступні або кореляція не визначена, повертається 0.0.
    """
    if reference_df.empty or asset_df.empty:
        return 0.0
    ref_returns = reference_df["close"].pct_change().dropna()
    asset_returns = asset_df["close"].pct_change().dropna()
    correlation = ref_returns.corr(asset_returns)
    return correlation if correlation is not None else 0.0


# ──────────────────── Клас моніторингу ────────────────────────────
class AssetMonitor:
    """
    Моніторинг крипто‑активів у реальному часі.

    Функціональність
    ----------------
    • Багатофакторні тригери: volume‑spike, динамічні пороги RSI,
      підтримка / опір (із перевіркою обсягу), відхилення VWAP,
      ATR‑коридор (висока / низька волатильність).
    • Anti‑spam (два шари):
        1) cooldown‑per‑reason (30 хв за замовчуванням);
        2) min_price_delta_pct — повтор не надсилаємо, якщо
           ціна зрушила < 0 .5 %.
    • ATR‑gate: ринки з ATR/price < 1 % ігноруються.
    • Останні статистики кешуються у self.asset_stats.
    """

    # ──────────────────────── 1. Конструктор ────────────────────────
    def __init__(
        self,
        volume_z_threshold: float = 2.0,
        rsi_overbought: float | None = None,
        rsi_oversold: float | None = None,
        dynamic_rsi_multiplier: float = 1.1,
        cooldown_period: timedelta = timedelta(hours=1),     # загальний (необов’язк.)
        cooldown_reason: timedelta = timedelta(minutes=45),  # cooldown‑per‑reason
        min_price_delta_pct: float = 0.75                     # Δ ціни для повтору
    ):
        # — базові параметри —
        self.volume_z_threshold = volume_z_threshold
        self.dynamic_rsi_multiplier = dynamic_rsi_multiplier
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold

        # — сховища стану —
        self.asset_stats: Dict[str, Dict[str, Any]] = {}
        self.last_alert_time: Dict[str, datetime] = {}               # (загальний)
        self.last_reason_alert: Dict[tuple[str, str], datetime] = {} # cooldown map
        self.last_reason_price: Dict[tuple[str, str], float] = {}    # price map

        # — anti‑spam налаштування —
        self.cooldown_period = cooldown_period
        self.cooldown_reason = cooldown_reason
        self.min_price_delta_pct = min_price_delta_pct

    # ──────────────── 2. Helper‑методи (anti‑spam) ────────────────
    def _reason_allowed(self, symbol: str, reason: str) -> bool:
        """True, якщо cooldown для (symbol, reason) минув."""
        key = (symbol, reason)
        now = datetime.now(timezone.utc)
        last = self.last_reason_alert.get(key)
        if not last or now - last >= self.cooldown_reason:
            self.last_reason_alert[key] = now
            return True
        return False

    def _price_move_enough(self, symbol: str, reason: str, price: float) -> bool:
        """True, якщо ціна змінилась ≥ min_price_delta_pct % від останнього сигналу."""
        key = (symbol, reason)
        prev = self.last_reason_price.get(key)
        if prev is None:
            return True
        pct = abs(price - prev) / prev * 100
        return pct >= self.min_price_delta_pct

    # ──────────────── 3. Оновлення статистики ────────────────
    async def update_statistics(
        self,
        symbol: str,
        df: pd.DataFrame,
        reference_df: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """Розраховує всі потрібні метрики та кешує їх."""
        df = standardize_format(df)
        price = df["close"].iloc[-1]
        vol_mean = df["volume"].mean()
        vol_std = df["volume"].std(ddof=0) or 1.0
        vol_z = (df["volume"].iloc[-1] - vol_mean) / vol_std

        rsi_series = compute_rsi(df["close"])
        rsi_val = rsi_series.iloc[-1]
        rsi_bar = format_rsi(rsi_val)
        vwap_val = compute_vwap(df)
        atr_val = compute_atr(df)
        sharpe_val = compute_sharpe_ratio(df)
        corr_val = (
            compute_correlation(df, reference_df)
            if reference_df is not None else np.nan
        )

        avg_rsi = rsi_series.mean()
        dyn_over = self.rsi_overbought or min(avg_rsi * self.dynamic_rsi_multiplier, 90)
        dyn_under = self.rsi_oversold  or max(avg_rsi / self.dynamic_rsi_multiplier, 10)

        stats = {
            "current_price": price,
            "volume_mean": vol_mean,
            "volume_std": vol_std,
            "volume_z": vol_z,
            "rsi": rsi_val,
            "rsi_bar": rsi_bar, 
            "dynamic_overbought": dyn_over,
            "dynamic_oversold": dyn_under,
            "vwap": vwap_val,
            "atr": atr_val,
            "sharpe_ratio": sharpe_val,
            "correlation_with_index": corr_val,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        self.asset_stats[symbol] = stats
        return stats

    # ──────────────── 4. Головний аналіз аномалій ────────────────
    async def check_anomalies(
        self,
        symbol: str,
        df: pd.DataFrame,
        stats: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Повертає dict із ключами:
            symbol, current_price, anomalies[], signal, trigger_reasons[], stats
        """
        stats = stats or self.asset_stats.get(symbol) \
                or await self.update_statistics(symbol, df)
        price = stats["current_price"]

        anomalies: List[str] = []
        reasons:   List[str] = []

        # 1) ATR‑gate — «тихий» ринок
        if stats["atr"] / price < 0.01:
            return {"signal": "NORMAL ❌"}

        # локальний helper
        def _add(reason: str, text: str):
            if self._reason_allowed(symbol, reason) and \
               self._price_move_enough(symbol, reason, price):
                anomalies.append(text)
                reasons.append(text) # або reason
                self.last_reason_price[(symbol, reason)] = price

        # 2) volume spike
        if stats["volume_z"] >= self.volume_z_threshold:
            _add("volume_spike", f"🚨 Сплеск обсягу (Z={stats['volume_z']:.2f})")

        # 3) RSI
        if stats["rsi"] >= stats["dynamic_overbought"]:
            _add("rsi_overbought", f"🔺 RSI перекупленість ({stats['rsi']:.1f})")
        elif stats["rsi"] <= stats["dynamic_oversold"]:
            _add("rsi_oversold",  f"🔻 RSI перепроданість ({stats['rsi']:.1f})")

        # 4) Рівні підтримки / опору (з перевіркою обсягу)
        lvls = find_local_levels(df["close"])
        if lvls["peaks"].size:
            idx = (df["close"].iloc[lvls["peaks"]] - price).abs().idxmin()
            peak = df["close"].loc[idx]
            if abs(price - peak) / peak < 0.01 and df["volume"].loc[idx] > stats["volume_mean"]:
                _add("resistance", f"🔝 Ціна біля опору ({peak:.4f})")
        if lvls["troughs"].size:
            idx = (df["close"].iloc[lvls["troughs"]] - price).abs().idxmin()
            trough = df["close"].loc[idx]
            if abs(price - trough) / trough < 0.01 and df["volume"].loc[idx] > stats["volume_mean"]:
                _add("support", f"🔻 Ціна біля підтримки ({trough:.4f})")

        # 5) VWAP
        if abs(price - stats["vwap"]) / stats["vwap"] < 0.005:
            _add("vwap_near", f"⚖️ Ціна біля VWAP ({stats['vwap']:.4f})")

        # 6) ATR corridor
        if stats["atr"] / price > 0.02:
            _add("high_atr", "📊 Висока волатильність (ATR>2%)")
        else:
            _add("low_atr",  "📉 Низька волатильність (ATR<2%)")

        # 7) мультифактор
        signal_str = "ALERT ✅" if len(reasons) >= 2 else "NORMAL ❌"
        if signal_str == "NORMAL ❌":
            anomalies = []  # приховуємо непідтверджені

        # 8) лог + результат
        result = {
            "symbol": symbol,
            "current_price": price,
            "anomalies": anomalies,
            "signal": signal_str,
            "trigger_reasons": reasons,
            "stats": stats,
        }

        anomalies_str = (
            "\n".join(f"  ├─ {a}" for a in anomalies) if anomalies else "  └─ немає"
        )
        logger.debug(
            f"[{symbol}] Результати аналізу аномалій:\n"
            f"  ├─ Поточна ціна: {price:.4f} USD\n"
            f"  ├─ RSI: {stats['rsi']:.2f} {stats['rsi_bar']}\n"
            f"{anomalies_str}\n"
            f"  └─ Сигнал      : {signal_str}"
        )
        if anomalies:
            logger.info(
                "🔔 ALERT: %s @ %.4f USD | %s",
                symbol, price, "; ".join(reasons)
            )
        return result


    async def send_alert(self, symbol: str, message: str) -> None:
        """
        Надсилає повідомлення через Telegram, якщо загальний cooldown для активу завершено.
        """
        now = datetime.now(timezone.utc)
        last_alert = self.last_alert_time.get(symbol)
        if last_alert and now - last_alert < self.cooldown_period:
            logger.debug(f"[{symbol}] Alert не надіслано через загальний cooldown.")
            return
        await send_telegram_alert(message)
        self.last_alert_time[symbol] = now

    async def monitor_asset(self, symbol: str, data_fetcher: Any, interval_sec: int = 60) -> None:
        """
        Безперервно моніторить актив: отримує дані, оновлює статистику, перевіряє аномалії
        та надсилає сигнал при їх виявленні з урахуванням cooldown.
        """
        logger.info(f"🚀 Старт моніторингу {symbol}.")
        while True:
            try:
                df = await data_fetcher.get_data(symbol, interval="1h", limit=100, min_candles=24)
                if df is None or df.empty:
                    logger.warning(f"[{symbol}] Дані не отримано.")
                else:
                    stats = await self.update_statistics(symbol, df)
                    analysis = await self.check_anomalies(symbol, df, stats)
                    if analysis["signal"] == "ALERT ✅":
                        alert_message = (
                            f"🔔 ALERT: {symbol} @ {stats['current_price']:.4f} USD\n"
                            f"Причини: {', '.join(analysis['trigger_reasons'])}"
                        )
                        logger.info(alert_message)
                        await self.send_alert(symbol, alert_message)
                next_cycle = datetime.now(timezone.utc) + timedelta(seconds=interval_sec)
                logger.debug(f"[{symbol}] Наступний цикл о {next_cycle:%Y-%m-%d %H:%M:%S} UTC.")
                await asyncio.sleep(interval_sec)
            except Exception as e:
                logger.error(f"[{symbol}] Помилка: {e}", exc_info=True)
                await asyncio.sleep(interval_sec)


async def monitoring_loop(
    assets: List[Dict[str, Any]], data_fetcher: Any, monitor: AssetMonitor, interval_sec: int = 14600
) -> None:
    """
    Запускає паралельний моніторинг переліку активів.
    Кожен елемент `assets` може бути або словником з описом активу (має ключ "ticker"),
    або просто рядком-тікером (наприклад, "BTCUSDT").
    Якщо assets передано як словник (наприклад, {"final": [...], "base": [...]}), то значення об'єднуються в один список.
    """
    logger.info("Запуск моніторингу обраних активів...")

    # Якщо assets є словником, об'єднуємо всі списки активів в один
    if isinstance(assets, dict):
        combined_assets = []
        for key, value in assets.items():
            if isinstance(value, list):
                combined_assets.extend(value)
            else:
                combined_assets.append(value)
        assets = combined_assets

    total_assets = len(assets)
    logger.info(f"Загальна кількість активів, що передаються для моніторингу: {total_assets}")

    # Отримуємо список тікерів, перевіряючи наявність ключа "ticker" або "symbol"
    tickers = [
        asset.get("ticker") or asset.get("symbol")
        if isinstance(asset, dict) else asset
        for asset in assets
        if (isinstance(asset, dict) and (asset.get("ticker") or asset.get("symbol"))) or isinstance(asset, str)
    ]
    
    logger.info(f"Запущено моніторинг {len(tickers)} актив(ів): {', '.join(tickers)}")
    tasks = [monitor.monitor_asset(ticker, data_fetcher, interval_sec) for ticker in tickers]
    await asyncio.gather(*tasks)


async def send_telegram_alert(
    message: str,
    telegram_token: str = TELEGRAM_TOKEN,
    admin_id: int = ADMIN_ID
) -> None:
    """
    Асинхронно надсилає повідомлення в Telegram.

    Аргументи:
        message (str): Текст повідомлення.
        telegram_token (str): Токен Telegram бота. За замовчуванням імпортується з config.
        admin_id (int): ID користувача або групи для сповіщення. За замовчуванням імпортується з config.
    """
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"chat_id": admin_id, "text": message}
            async with session.post(url, json=payload) as response:
                response_json = await response.json()
                if response.status == 200 and response_json.get("ok"):
                    logger.debug("Повідомлення успішно надіслано.")
                else:
                    logger.error(
                        f"Помилка надсилання повідомлення: Статус {response.status}. Відповідь: {response_json}"
                    )
    except aiohttp.ClientError as e:
        logger.error(
            f"Помилка підключення до Telegram API: {e}",
            exc_info=True
        )
    except Exception as e:
        logger.error(
            f"Невідома помилка при надсиланні повідомлення: {e}",
            exc_info=True
        )
