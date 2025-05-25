# monitor/asset_monitor/ui_consumer.py

import sys
import logging
import asyncio
from typing import Any, Dict, List

from rich.console import Console
from rich.logging import RichHandler

from rich.live import Live
from rich.table import Table
from rich import box

from stage1.utils import format_volume_usd, format_open_interest

# Окрема консоль для Live-таблиці (stdout)
ui_console = Console(stderr=False)   

# Окремий логер для ui_consumer
ui_logger = logging.getLogger("ui_consumer")
ui_logger.setLevel(logging.WARNING)  # логуються тільки WARNING і вище
ui_logger.addHandler(
    RichHandler(console=Console(stderr=True), level="WARNING")
)
ui_logger.propagate = False          # щоб не дублювати логи у root-logger


class UI_Consumer:
    def __init__(
        self,
        vol_z_threshold: float = 2.0,
        # нижній поріг ATR у відсотках (ATR/price)
        low_atr_threshold: float = 0.005,  # 0.5% ціни
    ):
        """
        :param vol_z_threshold: Z-поріг для позначення аномального обсягу (volume_z)
        :param low_atr_threshold: мінімальний відносний ATR для активації сигналу (у частках ціни)
        """
        self.vol_z_threshold = vol_z_threshold
        self.low_atr_threshold = low_atr_threshold

        
    async def ui_consumer(
        self,
        queue: asyncio.Queue,       
        refresh_rate: float = 0.5,  
        loading_delay: float = 5.0,
        smooth_delay: float = 0.4,
    ):
        """
        Основний live-рендер сигналів для користувача:
            - Зачекає `loading_delay` перед стартом.
            - Читає результати з черги, будує та оновлює таблицю сигналів у Live‑режимі (stdout).
            - Логи з ui_consumer (наприклад, помилки парсингу, некоректні вхідні дані) виводяться у stderr через окремий RichHandler.
            - Параметри:
                :param queue: асинхронна черга з результатами моніторингу
                :param refresh_rate: частота оновлення Live‑таблиці (раз/сек)
                :param loading_delay: пауза перед стартом Live‑рендера
                :param smooth_delay: додатковий таймінг згладжування (для дебагу)
        """
        # затримка перед початком рендера (наповнення черги
        last_results: List[Dict[str, Any]] = []
        
        # затримка перед початком рендера
        await asyncio.sleep(loading_delay)  

        # Live-контекст: spinner + оновлення таблиці
        with Live(
            self._build_signal_table([], loading=True),
            console=ui_console,                          # Live‑таблиця тільки у stdout
            refresh_per_second=refresh_rate,
            screen=False,                                # затирає тільки попередній блок
            transient=False,                             # залишає останній стан таблиці
            redirect_stderr=False                        # stderr НЕ захоплюється
            
        ) as live:
            ui_logger.info("UI started")

            while True:
                # Перевіряємо, чи є нові дані у черзі
                try:
                    new = queue.get_nowait()
                    last_results = new

                except asyncio.QueueEmpty:
                    pass
                
                # Формуємо таблицю з даними або spinner, якщо ще немає результатів
                loading = not bool(last_results)
                table = self._build_signal_table(last_results, loading=loading)

                # Оновлюємо Live-зону — Rich сам очистить екран
                live.update(table)    # перезаписує попередній рендер
                
                # плавний інтервал між оновленнями
                await asyncio.sleep(smooth_delay)  

                # Чекаємо до наступного оновлення
                await asyncio.sleep(refresh_rate)
        

    def _build_signal_table(
        self,
        results: List[dict],
        loading: bool = False,
    ) -> Table:
        """
        Будує та повертає Rich.Table зі списку сигналів разом із статистикою
        в заголовку. Без зовнішніх console.print — вся візуалізація в межах самої таблиці.
        """
        # ─── Підрахунок статистики одразу ────────────────────────────────────
        anomaly_count = 0
        warning_count = 0
        total = len(results)

        # ─── Ініціалізуємо таблицю ───────────────────────────────────────────
        # Заголовок доповнюємо місцем для статистики, поки що без чисел
        title = "Сигнали по активам"
        table = Table(title=title, box=box.SIMPLE_HEAVY)

        # ─── Додаємо колонки ────────────────────────────────────────────────
        headers = [
            ("Символ", "cyan", "left"),
            ("Ціна",     None,   "right"),
            ("Обсяг",    None,   "right"),
            ("OI",       None,   "right"),
            ("RSI",      None,   "right"),
            ("ATR%",     None,   "right"),
            ("RS",       None,   "right"),
            ("Corr",     None,   "right"),
            ("Аном.",    None,   "center"),
            ("❗",       None,   "center"),
            ("Статус",   None,   "center"),
            ("Причини",  None,   "left"),
            ("S2",       None,   "center"),       # Stage2: long/short
            ("Сценарій", None,   "left"),
            ("TP",       None,   "right"),
            ("SL",       None,   "right"),
        ]
        for h, style, justify in headers:
            table.add_column(h, style=style or "", justify=justify or "left")

        # ─── Spinner поки завантажується ────────────────────────────────────
        if loading:
            table.add_row("[cyan]🔄 Аналізую…[/]", *[""] * (len(table.columns) - 1))
            return table

        # ─── Якщо немає результатів ────────────────────────────────────────
        if total == 0:
            table.add_row(*["—"] * (len(table.columns) - 1), "[green]Немає сигналів[/]")
            # Оновимо заголовок із статистикою (0/0)
            table.title = f"{title}  |  Аномалії: 0/0  Warnings: 0/0"
            return table

        # ─── Пріоритетне сортування ─────────────────────────────────────────
        def priority_key(r: dict) -> tuple:
            stats   = r["stats"]
            reasons = set(r["trigger_reasons"])
            is_alert= r["signal"] == "ALERT"
            anomaly = stats.get("volume_z", 0.0) >= self.vol_z_threshold
            warning = (not is_alert) and bool(reasons)
            # Категорії 0–4…
            if is_alert and "volume_spike" in reasons: cat = 0
            elif is_alert:                           cat = 1
            elif anomaly:                            cat = 2
            elif warning:                            cat = 3
            else:                                    cat = 4
            return (cat, -stats.get("volume_mean", 0.0))

        sorted_results = sorted(results, key=priority_key)

        # ─── Додаємо рядки та рахуємо статистику ────────────────────────────
        for r in sorted_results:
            s = r["stats"]
            is_alert    = r["signal"] == "ALERT"
            anomaly     = s.get("volume_z", 0.0) >= self.vol_z_threshold
            warning_flag= "❗" if (not is_alert and r["trigger_reasons"]) else ""
            anomaly_cell= "✅" if anomaly else ""

            # 1) числові значення
            price_val = s.get("current_price", 0.0)
            atr_val   = s.get("atr", 0.0)

            # 2) форматовані рядки
            price = f"{price_val:.4f}"
            # ATR% від ціни
            atr_pct = (atr_val / price_val) if price_val else 0.0
            atr_pct_str = f"{atr_pct*100:.2f}%"
            if atr_pct < self.low_atr_threshold:
                atr_pct_str = f"[bold red]{atr_pct_str}[/]"

            vol = format_volume_usd(s.get("volume_mean", 0.0))
            oi  = "-"
            if isinstance(s.get("open_interest"), (int, float)):
                oi = format_open_interest(s["open_interest"])

            rsi_str  = f"{s.get('rsi'):.1f}" if isinstance(s.get("rsi"), (int, float)) else "-"
            rs_str   = f"{s.get('rel_strength'):.4f}" if isinstance(s.get("rel_strength"), (int, float)) else "-"
            corr_str = f"{s.get('btc_dependency_score'):.2f}" if isinstance(s.get("btc_dependency_score"), (int, float)) else "-"

            sig_text  = "🔴 ALERT" if is_alert else "🟢 NORMAL"
            sig_style = "bold red" if is_alert else "bold green"

            tags = []
            for reason in r["trigger_reasons"]:
                if reason == "volume_spike":
                    tags.append("[magenta]Сплеск обсягу[/]")
                else:
                    tags.append(f"[yellow]{reason}[/]")
            reasons = "  ".join(tags) or "-"

            # Збільшуємо лічильники
            anomaly_count += int(anomaly)
            warning_count += int(bool(warning_flag))
            
            # отримуємо Stage2-сигнал і сценарій
            stage2 = r.get("stage2", "-")
            if stage2 == "long":
                stage2 = "[green]LONG[/]"
            elif stage2 == "short":
                stage2 = "[red]SHORT[/]"
            else:
                stage2 = "-"
            
            scenario = r.get("scenario", "-")
            scenario = f"[magenta]{scenario}[/]"
            
            # Форматуємо TP/SL, якщо є — інакше ставимо "-"
            tp = f"{r.get('tp'):.4f}" if r.get("tp") is not None else "-"
            sl = f"{r.get('sl'):.4f}" if r.get("sl") is not None else "-"

            table.add_row(
                r["symbol"],      # Символ
                price,            # Ціна
                vol,              # Обсяг
                oi,               # OI

                rsi_str,          # RSI
                atr_pct_str,      # ATR%

                rs_str,           # RS
                corr_str,         # Corr

                anomaly_cell,     # Аном.
                warning_flag,     # ❗

                f"[{sig_style}]{sig_text}[/]",  # Статус
                reasons,          # Причини

                stage2,           # S2
                scenario,         # Сценарій
                tp,               # TP
                sl                # SL
            )

        # ─── Оновлюємо заголовок із підрахованою статистикою ───────────────
        table.title = (
            f"{title}"
            f"  |  Аномалії: {anomaly_count}/{total}"
            f"  Warnings: {warning_count}/{total}"
        )

        return table
    

   