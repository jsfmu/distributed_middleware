import threading
from typing import Callable, Optional

import requests

import asyncio


PayloadFn = Optional[Callable[[], dict]]


def build_payload(worker_id: str, payload_fn: PayloadFn = None) -> dict:
    payload = {
        "worker_id": worker_id
    }

    if payload_fn is not None:
        payload.update(payload_fn())

    return payload


async def heartbeat_loop(worker_id: str, heartbeat_url: str, interval: float, payload_fn: PayloadFn = None):
    while True:
        # Rebuilt on every beat so the load is current, not the startup value
        payload = build_payload(worker_id, payload_fn)
        print(f"[HEARTBEAT PAYLOAD] {payload}")

        try:
            response = await asyncio.to_thread(requests.post, heartbeat_url, json=payload, timeout=5)
            print(f"[HEARTBEAT] worker={worker_id} status={response.status_code}")
        except requests.RequestException as exc:
            print(f"[HEARTBEAT ERROR] worker={worker_id} error={exc}")

        await asyncio.sleep(interval)

def start_heartbeat_thread(worker_id: str, heartbeat_url: str, interval: float = 20, payload_fn: PayloadFn = None):
    def _run_event_loop():
        asyncio.run(heartbeat_loop(worker_id, heartbeat_url, interval, payload_fn))

    thread = threading.Thread(target=_run_event_loop, daemon=True)
    thread.start()
    return thread
