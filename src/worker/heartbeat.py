import threading
import time
import requests

import asyncio
import urllib.request

HEARTBEAT_URL = "http://localhost:8000/heartbeat"
WORKER_ID = "worker-1"
INTERVAL_SECONDS = 20

async def heartbeat_loop(worker_id: str, heartbeat_url: str, interval: int):
    while True:
        payload = {
            "worker_id": worker_id, "timestamp": time.time()}
        print(f"Generated payload: {payload}")
        
        try:
            response = await asyncio.to_thread(requests.post, HEARTBEAT_URL, json={"worker_id":WORKER_ID}, timeout=5)
            print(f"Async heartbeat sent (status: {response.status_code})")
        except Exception as e:
            print(f"Async heartbeat failed: {e}")
        await asyncio.sleep(INTERVAL_SECONDS)

def start_heartbeat_thread(worker_id: str, heartbeat_url: str, interval: int = 20):
    def _run_event_loop():
        asyncio.run(heartbeat_loop(worker_id, heartbeat_url, interval))

    thread = threading.Thread(target=_run_event_loop, daemon=True)
    thread.start()
    return thread

if __name__ == "__main__":
    asyncio.run(heartbeat_loop("worker-1", "http://localhost:8000/heartbeat", 20))