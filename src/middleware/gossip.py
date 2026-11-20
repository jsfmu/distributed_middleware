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