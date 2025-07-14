import redis.asyncio as redis
import asyncio
import json
import logging
from UI.ui_consumer import UI_Consumer

async def ui_redis_live():
    ui = UI_Consumer(vol_z_threshold=2.5)
    last_results = []

    while True:
        try:
            client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            pubsub = client.pubsub()
            await pubsub.subscribe("signals")
            logging.info("🔗 UI: Підключено до Redis PubSub.")
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                await ui.ui_consumer(
                    redis_url="redis://localhost:6379/0",
                    channel="signals",
                    refresh_rate=1.0,
                    loading_delay=2.0,
                    smooth_delay=0.1
                )
        except Exception as e:
            logging.error("⛔️ Втрата зв'язку з Redis: %s", e)
            await asyncio.sleep(3)   # auto-reconnect

if __name__ == "__main__":
    asyncio.run(ui_redis_live())
