
import copy
import threading
import time

import requests
from urllib3.exceptions import NewConnectionError


def worker_key(worker):
    return worker["owner"], worker["worker_id"]


def _eligible(worker, model):
    """Check whether a worker can serve the requested model."""

    if not isinstance(worker, dict):
        return False

    return (
        isinstance(worker.get("worker_id"), str)
        and bool(worker["worker_id"].strip())
        and isinstance(worker.get("host"), str)
        and bool(worker["host"].strip())
        and isinstance(worker.get("port"), int)
        and not isinstance(worker["port"], bool)
        and 1 <= worker["port"] <= 65535
        and isinstance(worker.get("models"), list)
        and model in worker["models"]
        and worker.get("status") == "healthy"
    )


def build_routing_candidates(
    local_workers,
    replica_snapshot,
    node_id,
    model,
    include_remote=False,
    trusted_owners=None,
    allowed_remote_hosts=None,
    remote_port_range=(9100, 9199)
):
    """
    Build a safe, deduplicated worker pool.

    Local ownership takes priority.
    Remote workers must be fresh, healthy and permitted.
    Conflicting remote ownership claims are excluded.
    """

    candidates = []
    local_ids = set()

    # Step 1: Include eligible local workers.
    for worker in local_workers:
        if not isinstance(worker, dict):
            continue

        worker_id = worker.get("worker_id")

        if isinstance(worker_id, str) and worker_id.strip():
            local_ids.add(worker_id)

        if _eligible(worker, model):
            record = copy.deepcopy(worker)
            record["owner"] = node_id
            record["route_scope"] = "local"
            candidates.append(record)

    # Preserve the original local-only behavior when disabled.
    if (
        not include_remote
        or not trusted_owners
        or not allowed_remote_hosts
    ):
        return sorted(candidates, key=worker_key)

    replica_snapshot = replica_snapshot or {}

    # Step 2: Find conflicting remote ownership claims.
    owners_by_id = {}

    for worker in replica_snapshot.get("remote_workers", []):
        if not isinstance(worker, dict):
            continue

        if worker.get("deleted", False):
            continue

        worker_id = worker.get("worker_id")
        owner = worker.get("owner")

        if not isinstance(worker_id, str):
            continue

        if not isinstance(owner, str):
            continue

        if (
            not worker_id.strip()
            or not owner.strip()
            or owner == node_id
        ):
            continue

        owners_by_id.setdefault(worker_id, set()).add(owner)

    conflicts = {
        worker_id
        for worker_id, owners in owners_by_id.items()
        if len(owners) > 1
    }

    seen = {worker_key(record) for record in candidates}

    # Step 3: Include only accepted, fresh remote records.
    for worker in replica_snapshot.get("fresh_remote_workers", []):
        if not _eligible(worker, model):
            continue

        if worker.get("deleted", False):
            continue

        worker_id = worker["worker_id"]
        owner = worker.get("owner")

        if not isinstance(owner, str) or not owner.strip():
            continue

        if (
            owner == node_id
            or worker_id in local_ids
            or worker_id in conflicts
        ):
            continue

        # Restrict destinations to configured worker endpoints.
        if (
            owner not in trusted_owners
            or worker["host"] not in allowed_remote_hosts
        ):
            continue

        if not (
            remote_port_range[0]
            <= worker["port"]
            <= remote_port_range[1]
        ):
            continue

        if owner not in owners_by_id.get(worker_id, set()):
            continue

        key = (owner, worker_id)

        if key in seen:
            continue

        record = copy.deepcopy(worker)
        record["route_scope"] = "remote"

        candidates.append(record)
        seen.add(key)

    return sorted(candidates, key=worker_key)


class WorkerQuarantine:
    """
    Temporarily exclude workers after connection failures.

    Quarantine is local to this middleware process.
    """

    def __init__(self, cooldown_seconds=10):
        self.cooldown_seconds = cooldown_seconds
        self._until = {}
        self._lock = threading.RLock()

    def mark_failed(self, worker):
        with self._lock:
            self._until[worker_key(worker)] = (
                time.monotonic() + self.cooldown_seconds
            )

    def is_quarantined(self, worker):
        key = worker_key(worker)

        with self._lock:
            deadline = self._until.get(key, 0)

            if deadline <= time.monotonic():
                self._until.pop(key, None)
                return False

            return True

    def clear(self):
        with self._lock:
            self._until.clear()


def safe_to_retry(exc):
    """
    Retry only when connection establishment failed.

    Do not automatically retry ambiguous failures because
    the worker might already have executed the inference.
    """

    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return True

    if not isinstance(exc, requests.exceptions.ConnectionError):
        return False

    to_check = [exc]
    seen = set()

    while to_check:
        node = to_check.pop()

        if id(node) in seen:
            continue

        seen.add(id(node))

        if isinstance(
            node,
            (NewConnectionError, ConnectionRefusedError)
        ):
            return True

        for next_node in (
            getattr(node, "reason", None),
            getattr(node, "__cause__", None),
            getattr(node, "__context__", None)
        ):
            if isinstance(next_node, BaseException):
                to_check.append(next_node)

        for arg in getattr(node, "args", ()):
            if isinstance(arg, BaseException):
                to_check.append(arg)

    return False
