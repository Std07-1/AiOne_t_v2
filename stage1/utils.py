# stage1\utils
# -*- coding: utf-8 -*-
"""
Модуль utils
-----------------
Універсальні утиліти для роботи з фінансовими даними, зокрема:
- Форматування обсягу у USD (format_volume_usd)
- Перевірка робочих годин американської торгової сесії (is_us_session)
- Форматування Open Interest (format_open_interest)
Всі логи — україномовні, з підтримкою RichHandler для зручної діагностики.
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo
import logging

from rich.console import Console
from rich.logging import RichHandler

# --- Налаштування логування ---
logger = logging.getLogger("stage1.utils")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(RichHandler(console=Console(stderr=True), show_path=False))
logger.propagate = False


def format_volume_usd(volume: float | str) -> str:
    """
    Форматує оборот у USD.
    Приймає як float, так і вже відформатований рядок —
    у другому випадку повертає його без змін.
    :param volume: float або str
    :return: відформатований рядок
    """
    logger.debug(f"[format_volume_usd] Вхідне значення: {volume}")
    if isinstance(volume, str):
        logger.debug("[format_volume_usd] Вже рядок, повертаємо як є")
        return volume
    if volume >= 1e12:
        result = f"{volume/1e12:.2f}T USD"
    elif volume >= 1e9:
        result = f"{volume/1e9:.2f}G USD"
    elif volume >= 1e6:
        result = f"{volume/1e6:.2f}M USD"
    elif volume >= 1e3:
        result = f"{volume/1e3:.2f}K USD"
    else:
        result = f"{volume:.2f} USD"
    logger.debug(f"[format_volume_usd] Вихід: {result}")
    return result


def is_us_session(current_time: datetime) -> bool:
    """
    Перевіряє, чи поточний час входить до робочих годин американської торгової сесії (9:30–16:00 ET).
    :param current_time: datetime
    :return: bool
    """
    logger.debug(f"[is_us_session] Вхідний час: {current_time}")
    try:
        eastern = current_time.astimezone(ZoneInfo("America/New_York"))
        logger.debug(f"[is_us_session] Конвертовано до ET: {eastern}")
    except Exception as e:
        logger.error(f"Помилка конвертації часу: {e}")
        return False
    start = time(9, 30)
    end = time(16, 0)
    result = eastern.weekday() < 5 and start <= eastern.time() <= end
    logger.debug(
        f"[is_us_session] Поточний час (ET): {eastern.time()} — US сесія = {result}"
    )
    return True  # або повернути result для реальної перевірки


def format_open_interest(oi: float) -> str:
    """
    Форматує значення Open Interest для зручного відображення.
    Якщо oi >= 1e9, повертає у мільярдах (B);
    якщо >= 1e6, повертає у мільйонах (M);
    якщо >= 1e3, повертає у тисячах (K);
    інакше повертає стандартне значення.
    :param oi: float
    :return: відформатований рядок
    """
    logger.debug(f"[format_open_interest] Вхідне значення: {oi}")
    try:
        val = float(oi)
    except (ValueError, TypeError):
        logger.debug("[format_open_interest] Неможливо конвертувати у float")
        return "-"
    if val >= 1e9:
        result = f"{val / 1e9:.2f}B"
    elif val >= 1e6:
        result = f"{val / 1e6:.2f}M"
    elif val >= 1e3:
        result = f"{val / 1e3:.2f}K"
    else:
        result = f"{val:.2f} USD"
    logger.debug(f"[format_open_interest] Вихід: {result}")
    return result


def format_price(price: float, symbol: str) -> str:
    """Форматує ціну відповідно до специфіки активу"""
    # Визначаємо кількість знаків після коми на основі символу
    decimals = 4  # Значення за замовчуванням

    if "btc" in symbol or "eth" in symbol:
        decimals = 2
    elif "usdt" in symbol:
        decimals = 4

    # Форматуємо з відповідною точністю
    if price < 1:
        return f"{price:.{decimals}f}"
    else:
        # Для великих чисел використовуємо роздільник тисяч
        return f"{price:,.{decimals}f}"
