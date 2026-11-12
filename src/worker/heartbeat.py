import threading
import time
import requests

import asyncio
import urllib.request

HEARTBEAT_URL = "http://localhost:8000/heartbeat"
WORKER_ID = "worker-1"
INTERVAL_SECONDS = 20

async def heartbeat_loop():
    while True:
        try:
            response = await asyncio.to_thread(requests.post, HEARTBEAT_URL, json={"worker_id":WORKER_ID}, timeout=5)
            print(f"Async heartbeat sent (status: {response.status_code})")
        except Exception as e:
            print(f"Async heartbeat failed: {e}")
        await asyncio.sleep(INTERVAL_SECONDS)

async def main():
    heartbeat_task = asyncio.create_task(heartbeat_loop())
    try:
        print("Main worker running...")
        await asyncio.sleep(100)
    finally:
        heartbeat_task.cancel()

asyncio.run(main())