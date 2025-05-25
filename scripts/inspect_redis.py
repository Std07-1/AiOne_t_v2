#!/usr/bin/env python3
import sys, pathlib

# Додаємо корінь tr/ у sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import os
import json
import asyncio
from dotenv import load_dotenv
from data.cache_handler import SimpleCacheHandler

load_dotenv()

def get_cache() -> SimpleCacheHandler:
    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        return SimpleCacheHandler.from_url(redis_url)
    return SimpleCacheHandler(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", 6379))
    )

async def inspect():
    cache = get_cache()
    for key in ("top10", "asset_stats"):
        raw = await cache.fetch_from_cache(key)
        if not raw:
            print(f"{key}: (nil)")
            continue
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            print(f"{key}: (invalid JSON) {raw!r}")
        else:
            print(f"{key}:")
            print(json.dumps(data, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    asyncio.run(inspect())

