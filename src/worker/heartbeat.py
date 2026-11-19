import threading
from typing import Callable, Optional, Union

import requests

import asyncio


PayloadFn = Optional[Callable[[], dict]]
UrlSource = Union[str, Callable[[], str]]


def build_payload(worker_id: str, payload_fn: PayloadFn = None) -> dict:
    payload = {
        "worker_id": worker_id
    }

    if payload_fn is not None:
        payload.update(payload_fn())

    return payload


async def heartbeat_loop(
    worker_id: str,
    heartbeat_url: UrlSource,
    interval: float,
    payload_fn: PayloadFn = None,
    on_failure: Optional[Callable[[], None]] = None,
    max_failures: int = 2
):
    failures = 0

    while True:
        # Ask for the URL every beat, so a failover to another middleware is picked up
        url = heartbeat_url() if callable(heartbeat_url) else heartbeat_url
        # Rebuilt on every beat so the load is current, not the startup value
        payload = build_payload(worker_id, payload_fn)
        print(f"[HEARTBEAT PAYLOAD] {payload}")

        ok = False
        try:
            response = await asyncio.to_thread(requests.post, url, json=payload, timeout=5)
            ok = response.status_code == 200
            print(f"[HEARTBEAT] worker={worker_id} url={url} status={response.status_code}")
        except requests.RequestException as exc:
            print(f"[HEARTBEAT ERROR] worker={worker_id} url={url} error={exc}")

        if ok:
            failures = 0
        else:
            failures += 1
            if on_failure is not None and failures >= max_failures:
                print(f"[HEARTBEAT] worker={worker_id} {failures} failed beats in a row, reconnecting")
                await asyncio.to_thread(on_failure)
                failures = 0

        await asyncio.sleep(interval)

def start_heartbeat_thread(
    worker_id: str,
    heartbeat_url: UrlSource,
    interval: float = 20,
    payload_fn: PayloadFn = None,
    on_failure: Optional[Callable[[], None]] = None,
    max_failures: int = 2
):
    def _run_event_loop():
        asyncio.run(heartbeat_loop(worker_id, heartbeat_url, interval, payload_fn, on_failure, max_failures))

    thread = threading.Thread(target=_run_event_loop, daemon=True)
    thread.start()
    return thread
