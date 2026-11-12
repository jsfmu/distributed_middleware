import threading
import time
import requests

import asyncio
import urllib.request

HEARTBEAT_URL = "../middleware/middleware.py"
INTERVAL_SECONDS = 20

async def heartbeat_loop():
    while True:
        try:
            await asyncio.to_thread(urllib.request.urlopen, HEARTBEAT_URL, timeout=5)
            print("Async heartbeat sent")
        except Exception as e:
            print(f"Async heartbeat failed: {e}")

    await asyncio.sleep(INTERVAL_SECONDS)