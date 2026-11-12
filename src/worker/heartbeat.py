import threading
import requests

import asyncio

async def heartbeat_loop(worker_id: str, heartbeat_url: str, interval: int):
    while True:
        payload = {
            "worker_id": worker_id
        }

        try:
            response = await asyncio.to_thread(requests.post, heartbeat_url, json=payload, timeout=5)
            print(f"[HEARTBEAT] worker={worker_id} status={response.status_code}")
        except requests.RequestException as exc:
            print(f"[HEARTBEAT ERROR] worker={worker_id} error={exc}")

        await asyncio.sleep(interval)

def start_heartbeat_thread(worker_id: str, heartbeat_url: str, interval: int = 20):
    def _run_event_loop():
        asyncio.run(heartbeat_loop(worker_id, heartbeat_url, interval))

    thread = threading.Thread(target=_run_event_loop, daemon=True)
    thread.start()
    return thread