"""
monitor/asset_monitor/config.py

Конфігураційний модуль для моніторингу активів.
Містить налаштування логування, параметри оновлення даних,
конфігурацію Telegram та інші глобальні константи.
"""
import logging
import os
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

# Конфігурація Telegram
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "1501232696"))

# Налаштування логування
LOG_FILENAME = "system.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILENAME, mode="a", encoding="utf-8")
    ]
)
logger = logging.getLogger("asset_monitor")

# Параметри оновлення даних
HOURLY_UPDATE_PERIOD = 10          # секунд для циклічного оновлення 1h даних
DAILY_UPDATE_PERIOD_INITIAL = 14400  # початковий інтервал (сек) для денного оновлення
DAILY_UPDATE_PERIOD_MAX = 3600       # максимальний інтервал (сек) при відсутності нових даних
