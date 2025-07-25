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
    ui = UI_Consumer(vol_z_threshold=2.5)
    logger.info("🚀 Запуск UI Consumer...")
    await ui.redis_consumer(
        redis_url="redis://localhost:6379/0",
        channel="asset_state_update",
        refresh_rate=1.0,
        loading_delay=2.0,
        smooth_delay=0.1,
    )


if __name__ == "__main__":
    asyncio.run(main())
