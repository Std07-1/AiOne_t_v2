# UI/ui_consumer.py
import redis.asyncio as redis
import json
import logging
import asyncio
import time
import math
from datetime import datetime
from typing import Any, Dict, List

from rich.console import Console
from rich.logging import RichHandler
from rich.live import Live
from rich.table import Table
from rich.box import ROUNDED

# Окрема консоль для Live-таблиці
ui_console = Console(stderr=False)

# Логування
ui_logger = logging.getLogger("ui_consumer")
ui_logger.setLevel(logging.INFO)
ui_logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
ui_logger.propagate = False


class AlertAnimator:
    def __init__(self):
        self.active_alerts = {}

    def add_alert(self, symbol: str):
        self.active_alerts[symbol] = time.time()

    def should_highlight(self, symbol: str) -> bool:
        if symbol in self.active_alerts:
            elapsed = time.time() - self.active_alerts[symbol]
            if elapsed < 8.0:
                return True
            else:
                del self.active_alerts[symbol]
        return False


class UI_Consumer:
    def __init__(
        self,
        vol_z_threshold: float = 2.5,  # Поріг Z-статистики для обсягу
        low_atr_threshold: float = 0.005,  # Поріг ATR% для низької волатильності
    ):
        self.vol_z_threshold = vol_z_threshold  # Поріг Z-статистики для обсягу
        self.low_atr_threshold = (
            low_atr_threshold  # Поріг ATR% для низької волатильності
        )
        self.alert_animator = AlertAnimator()  # Аніматор для алертів
        self.last_update_time = time.time()  # Час останнього оновлення

    def _format_price(self, price: float) -> str:
        """Форматування ціни з роздільником тисяч"""
        if price >= 1000:
            return f"{price:,.2f}"
        return f"{price:.4f}"

    def _get_rsi_color(self, rsi: float) -> str:
        """Колір для RSI на основі значення"""
        if rsi < 30:
            return "green"
        elif rsi < 50:
            return "light_green"
        elif rsi < 70:
            return "yellow"
        else:
            return "red"

    def _get_atr_color(self, atr_pct: float) -> str:
        """Колір для ATR% на основі волатильності"""
        if atr_pct < self.low_atr_threshold:
            return "red"
        elif atr_pct > 0.02:
            return "yellow"
        return ""

    def _get_calib_status_icon(self, status: str) -> str:
        """Іконки для статусу калібрування"""
        icons = {
            "completed": "✅",
            "in_progress": "🔄",
            "queued_urgent": "⚠️🔴",
            "queued_high": "⚠️🟡",
            "queued": "⚠️",
            "pending": "⏳",
            "error": "❌",
            "timeout": "⏱️",
        }
        return icons.get(status, "❓")

    def _get_signal_icon(self, signal: str) -> str:
        """Іконки для типу сигналу"""
        icons = {
            "ALERT": "🔴",
            "NORMAL": "🟢",
            "ALERT_BUY": "🟢↑",
            "ALERT_SELL": "🔴↓",
            "NONE": "⚪",
        }
        return icons.get(signal, "❓")

    def _get_recommendation_icon(self, recommendation: str) -> str:
        """Іконки для рекомендацій Stage2"""
        icons = {
            "STRONG_BUY": "🟢↑↑",
            "BUY_IN_DIPS": "🟢↑",
            "HOLD": "🟡",
            "SELL_ON_RALLIES": "🔴↓",
            "STRONG_SELL": "🔴↓↓",
            "AVOID": "⚫",
        }
        return icons.get(recommendation, "")

    async def redis_consumer(
        self,
        redis_url: str = "redis://localhost:6379/0",
        channel: str = "asset_state_update",
        refresh_rate: float = 0.7,  # Частота оновлення в секундах
        loading_delay: float = 2.0,  # Затримка перед початком рендера
        smooth_delay: float = 0.5,  # Затримка між оновленнями в циклі
    ):
        last_results: List[Dict[str, Any]] = []
        redis_client = redis.from_url(redis_url)
        pubsub = redis_client.pubsub()

        # Затримка перед початком рендера
        await asyncio.sleep(loading_delay)
        await pubsub.subscribe(channel)
        ui_logger.info(
            f"🔗 Підключено до Redis, очікування даних на каналі '{channel}'..."
        )

        with Live(
            self._build_signal_table([], loading=True),
            console=ui_console,
            refresh_per_second=refresh_rate,
            screen=False,
            transient=False,
        ) as live:
            while True:
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message:
                        data = json.loads(message["data"])
                        if isinstance(data, list):
                            last_results = data
                            ui_logger.debug(
                                f"Отримано оновлення для {len(data)} активів"
                            )

                    # Оновлюємо аніматори для алертів
                    for r in last_results:
                        if r.get("signal") == "ALERT":
                            self.alert_animator.add_alert(r["symbol"])

                    table = self._build_signal_table(last_results)
                    live.update(table)
                    await asyncio.sleep(smooth_delay)

                except (ConnectionError, TimeoutError) as e:
                    ui_logger.error(f"Помилка з'єднання: {str(e)}")
                    await asyncio.sleep(3)
                    try:
                        await pubsub.reset()
                        await pubsub.subscribe(channel)
                        ui_logger.info("✅ Перепідключено до Redis")
                    except Exception as reconnect_err:
                        ui_logger.error(
                            f"Помилка перепідключення: {str(reconnect_err)}"
                        )
                except Exception as e:
                    ui_logger.error(f"Невідома помилка: {str(e)}")
                    await asyncio.sleep(1)

    def _build_signal_table(
        self,
        results: List[dict],
        loading: bool = False,
    ) -> Table:
        """Побудова таблиці з сигналами та метриками системи"""

        # Статистика системи
        total_assets = len(results)
        alert_count = sum(1 for r in results if r.get("signal", "").startswith("ALERT"))
        calib_stats = {
            "completed": 0,
            "in_progress": 0,
            "queued": 0,
            "error": 0,
            "pending": 0,
        }

        for r in results:
            status = r.get("calib_status", "")
            if status in calib_stats:
                calib_stats[status] += 1

        last_update = datetime.fromtimestamp(self.last_update_time).strftime("%H:%M:%S")

        # Заголовок таблиці з статистикою
        title = (
            f"[bold]Система моніторингу AiOne_t[/bold] | "
            f"Активи: [green]{total_assets}[/green] | "
            f"ALERT: [red]{alert_count}[/red] | "
            f"Калібровано: [green]{calib_stats['completed']}[/green] | "
            f"В черзі: [yellow]{calib_stats['queued']+calib_stats['in_progress']}[/yellow] | "
            f"Оновлено: [cyan]{last_update}[/cyan]"
        )

        table = Table(
            title=title,
            box=ROUNDED,
            show_header=True,
            header_style="bold magenta",
            expand=True,
        )

        # Колонки таблиці
        columns = [
            ("Символ", "left"),
            ("Ціна", "right"),
            ("Обсяг", "right"),
            ("ATR%", "right"),
            ("RSI", "right"),
            ("Статус", "center"),
            ("Причини", "left"),
            ("Сигнал", "center"),
            ("Калібр.", "center"),
            ("Stage2", "center"),
            ("Рекомендація", "left"),
            ("TP/SL", "right"),
        ]

        for header, justify in columns:
            table.add_column(header, justify=justify)

        # Сповіщення про завантаження
        if loading or not results:
            table.add_row("[cyan]🔄 Очікування даних...[/]", *[""] * (len(columns) - 1))
            return table

        def priority_key(r: dict) -> tuple:
            stats = r.get("stats", {})
            reasons = set(r.get("trigger_reasons", []))
            is_alert = r.get("signal", "").startswith("ALERT")
            anomaly = stats.get("volume_z", 0.0) >= self.vol_z_threshold
            warning = (not is_alert) and bool(reasons)

            if is_alert and "volume_spike" in reasons:
                cat = 0
            elif is_alert:
                cat = 1
            elif anomaly:
                cat = 2
            elif warning:
                cat = 3
            else:
                cat = 4
            return (cat, -stats.get("volume_mean", 0.0))

        sorted_results = sorted(results, key=priority_key)

        # Додавання рядків для кожного активу
        for asset in sorted_results:
            symbol = asset["symbol"].upper()
            stats = asset.get("stats", {})
            stage2 = asset.get("stage2_result", {})

            # Ціна
            current_price = stats.get("current_price", 0)

            stage2_status = asset.get("stage2_result", {}).get("status", "")
            base_style = "dim"

            if stage2_status == "completed":
                base_style = "green"
            elif stage2_status == "pending":
                base_style = "yellow"

            # Підсвітка для alert, інакше — базовий стиль
            row_style = (
                "bold red"
                if self.alert_animator.should_highlight(symbol)
                else base_style
            )

            price_str = self._format_price(current_price)

            # Обсяг
            volume = stats.get("volume_mean", 0)
            volume_z = stats.get("volume_z", 0)
            volume_str = f"{volume:,.0f}"
            if volume_z > self.vol_z_threshold:
                volume_str = f"[bold magenta]{volume_str}[/]"

            # ATR%
            atr = stats.get("atr", 0)
            atr_pct = (atr / current_price * 100) if current_price else 0
            atr_color = self._get_atr_color(atr_pct)
            atr_str = f"[{atr_color}]{atr_pct:.2f}%[/]"

            # RSI
            rsi = stats.get("rsi", 0)
            rsi_color = self._get_rsi_color(rsi)
            rsi_str = f"[{rsi_color}]{rsi:.1f}[/]"

            # Статус
            status = asset.get("state", "normal")
            status_icon = "🟢" if status == "normal" else "🔴"
            status_str = f"{status_icon} {status}"

            # Сигнал
            signal = asset.get("signal", "NONE")
            signal_icon = self._get_signal_icon(signal)
            signal_str = f"{signal_icon} {signal}"

            # Калібрування
            calib_status = asset.get("calib_status", "pending")
            calib_icon = self._get_calib_status_icon(calib_status)
            calib_str = f"{calib_icon} {calib_status}"

            # Stage2
            stage2_status = asset.get("stage2_status", "pending")
            stage2_icon = "🟩" if stage2_status == "completed" else "🟨"
            stage2_str = f"{stage2_icon} {stage2_status}"

            # Рекомендація
            recommendation = stage2.get("recommendation", "-")
            rec_icon = self._get_recommendation_icon(recommendation)
            rec_str = f"{rec_icon} {recommendation}"

            # TP/SL
            tp = asset.get("tp", 0)
            sl = asset.get("sl", 0)
            tp_sl_str = (
                f"TP: {self._format_price(tp)}\nSL: {self._format_price(sl)}"
                if tp and sl
                else "-"
            )

            # Підсвітка рядка для нових алертів
            row_style = (
                "bold red" if self.alert_animator.should_highlight(symbol) else ""
            )

            tags = []
            for reason in asset.get("trigger_reasons", []):
                if reason == "volume_spike":
                    tags.append("[magenta]Сплеск обсягу[/]")
                else:
                    tags.append(f"[yellow]{reason}[/]")
            reasons = "  ".join(tags) or "-"

            table.add_row(
                symbol,
                price_str,
                volume_str,
                atr_str,
                rsi_str,
                status_str,
                reasons,  # Причини
                signal_str,
                calib_str,
                stage2_str,
                rec_str,
                tp_sl_str,
                style=row_style,
            )

        return table


# Головна функція запуску
async def main():
    consumer = UI_Consumer()
    await consumer.redis_consumer()


if __name__ == "__main__":
    asyncio.run(main())
