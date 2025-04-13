"""
monitor/asset_monitor/event_detector.py

Модуль для виявлення подій та генерації сигналів.
Містить функції для формування повідомлень-оповіщень та інтеграції з Telegram.
"""
import logging
import aiohttp
from .config import TELEGRAM_TOKEN, ADMIN_ID

logger = logging.getLogger("asset_monitor.event_detector")


async def send_telegram_alert(message: str, telegram_token: str = TELEGRAM_TOKEN, admin_id: int = ADMIN_ID) -> None:
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"chat_id": admin_id, "text": message}
            async with session.post(url, json=payload) as response:
                response_json = {}
                try:
                    response_json = await response.json()
                except Exception as parse_error:
                    logger.error(f"[send_telegram_alert] Помилка розбору відповіді: {parse_error}", exc_info=True)
                if response.status == 200 and response_json.get("ok"):
                    logger.info("Повідомлення успішно надіслано.")
                else:
                    logger.error(f"Помилка надсилання повідомлення: Статус {response.status}. Відповідь: {response_json}")
    except aiohttp.ClientError as e:
        logger.error(f"Помилка підключення до Telegram API: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Невідома помилка при надсиланні повідомлення: {e}", exc_info=True)



def generate_alert(symbol: str, price: float, level: float, level_type: str, approach: bool = False, forecast_period: str = None) -> str:
    """
    Генерує текстове повідомлення-оповіщення про пробиття або наближення ключового рівня.
    """
    base_msg = f"[{symbol}] Ціна: {price:.2f} "
    if approach:
        base_msg += f"наближається до {level_type} {level:.2f}."
    else:
        base_msg += f"пробила {level_type} {level:.2f}."
    if forecast_period:
        base_msg += f" Прогноз: {forecast_period}."
    alert = f"ℹ️ {base_msg}" if approach else f"⚠️ {base_msg}"
    logger.debug(f"Згенеровано оповіщення: {alert}")
    return alert


def generate_combined_signal(df: 'pd.DataFrame', symbol: str, current_price: float) -> dict:
    """
    Генерує комбінований сигнал на основі VWAP та волатильності.
    """
    from .analyzers import calculate_vwap, calculate_volatility
    vwap = calculate_vwap(df.tail(20))
    volatility = calculate_volatility(df, window=14)
    signal = "HOLD"
    signal_type = "без явного сигналу"
    if current_price < vwap and volatility < 0.05:
        signal = "BUY"
        signal_type = "відскок від VWAP, висхідний тренд"
    elif current_price > vwap and volatility < 0.05:
        signal = "SELL"
        signal_type = "корекція від VWAP, низхідний тренд"
    alert_msg = (f"[{symbol}] Поточна ціна: {current_price:.2f}, VWAP: {vwap:.2f}, "
                 f"волатильність: {volatility*100:.2f}%. Сигнал: {signal} ({signal_type}).")
    logger.debug(f"Згенеровано комбінований сигнал: {alert_msg}")
    return {"signal": signal, "alert": alert_msg, "vwap": vwap, "volatility": volatility}
