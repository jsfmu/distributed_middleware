import random
import threading
import time
import requests

GOSSIP_INTERVAL = 2
GOSSIP_FANOUT = 1
GOSSIP_TIMEOUT = 2
GOSSIP_RETRIES = 1
GOSSIP_RETRY_DELAY = 0.2

REACHABLE_STATUSES = {"healthy", "unknown"}

# to select peers
def select_gossip_peers(peers, fanout=GOSSIP_FANOUT, rng=random):
    if not peers or fanout <= 0:
        return []

    preferred = [peer for peer in peers if peer.get("status") in REACHABLE_STATUSES]

    candidates = preferred or list(peers)

    return rng.sample(candidates, min(fanout, len(candidates)))

# to format the message
def build_gossip_meesage(node_id, state):
    workers = []
    if isinstance(state, dict) and isinstance(state.get("workers"), list):
        workers = list(state["workers"])

    return {
        "sender_id": node_id,
        "workers": workers
    }

def send_gossip(peer, message, timeout=GOSSIP_TIMEOUT, retries=GOSSIP_RETRIES, retry_delay=GOSSIP_RETRY_DELAY):
    url = f"http://{peer['host']}:{peer['port']}/gossip"
    attempts = 1+  max(0, retries)
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(url, json=message, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(retry_delay)
            continue

        if response.ok:
            return "ok", None
        return "rejected", f"HTTP {response.state_code}"
    return "unreachable", str(last_error)