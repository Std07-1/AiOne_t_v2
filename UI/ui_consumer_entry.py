# UI/ui_consumer_entry.py

import asyncio
import logging
from UI.ui_consumer import UI_Consumer

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ui_consumer")


async def main():
    # Додаємо low_atr_threshold як у конструкторі UI_Consumer
    ui = UI_Consumer(
        vol_z_threshold=2.5, low_atr_threshold=0.005  # Додано новий параметр
    )

    logger.info("🚀 Запуск UI Consumer з оптимізованим інтерфейсом...")

    # Використовуємо оптимізовані параметри оновлення
    await ui.redis_consumer(
        redis_url="redis://localhost:6379/0",
        channel="asset_state_update",
        refresh_rate=0.8,  # Оптимальна частота оновлення
        loading_delay=1.5,  # Скорочений час завантаження
        smooth_delay=0.05,  # Мінімальна затримка для плавності
    )


if __name__ == "__main__":
    asyncio.run(main())
