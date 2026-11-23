import argparse
import json
import threading
import time
import os
from pathlib import Path

import requests
from flask import Flask, jsonify, request

from failure_detector import start_failure_detector
from peers import PeerRegistry
from registry import VersionedWorkerRegistry
from routing import choose_routing_policy, select_worker
from distributed_routing import (
    WorkerQuarantine,
    build_routing_candidates,
    safe_to_retry,
    worker_key
)

from gossip import start_gossip

HEARTBEAT_TIMEOUT = 30
HEALTH_CHECK_INTERVAL = 1

# Middleware peer discovery settings
PEER_DISCOVERY_INTERVAL = 2
PEER_REQUEST_TIMEOUT = 2

# Maximum age of received gossip used for worker consolidation
GOSSIP_SNAPSHOT_TTL = 30

# Choose the routing policy
ROUTING_POLICY = "adaptive"

# Disabled by default to preserve previous routing behavior.
ENABLE_DISTRIBUTED_ROUTING = False

# Maximum worker attempts per inference request.
MAX_INFER_ATTEMPTS = 3

# Only explicitly configured remote worker destinations are eligible.
# The defaults support the local multi-node test environment.
REMOTE_WORKER_ALLOWED_HOSTS = {
    host.strip()
    for host in os.environ.get(
        "REMOTE_WORKER_ALLOWED_HOSTS",
        "127.0.0.1"
    ).split(",")
    if host.strip()
}

REMOTE_WORKER_MIN_PORT = int(
    os.environ.get("REMOTE_WORKER_MIN_PORT", "9100")
)

REMOTE_WORKER_MAX_PORT = int(
    os.environ.get("REMOTE_WORKER_MAX_PORT", "9199")
)

# Temporarily exclude unreachable workers.
routing_quarantine = WorkerQuarantine(
    cooldown_seconds=10
)

# Middleware node identity
NODE_ID = "middleware-8000"

# Known middleware peers (separate from the worker registry)
peer_registry = PeerRegistry()

# Versioned worker state used for distributed replication
replicated_registry = VersionedWorkerRegistry(NODE_ID)

app = Flask(__name__)

# ----------------------------------------------------------------------
# Middleware Health
# ----------------------------------------------------------------------

worker_registry = {}
registry_lock = threading.Lock()

# Gossip received from other middleware nodes
# Kept separate from the local worker registry
received_gossip = {}
received_gossip_lock = threading.Lock()

@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "middleware",
        "node_id": NODE_ID,
        "routing_policy": ROUTING_POLICY,
        "peer_count": peer_registry.get_peer_count()
    })

# ----------------------------------------------------------------------
# Worker Registry
# ----------------------------------------------------------------------

@app.post("/register")
def register_worker():
    data = request.get_json(silent=True) or {}

    required_fields = [
        "worker_id",
        "host",
        "port",
        "models"
    ]

    missing_fields = [
        field
        for field in required_fields
        if field not in data
    ]

    if missing_fields:
        return jsonify({
            "error": "missing required fields",
            "missing": missing_fields
        }), 400

    worker = {
        "worker_id": data["worker_id"],
        "host": data["host"],
        "port": int(data["port"]),
        "models": data["models"],
        "status": "healthy",
        "last_heartbeat": time.time(),
        "active_requests": 0,
        "capacity": data.get("capacity"),
        "hardware": data.get("hardware", {})
    }

    with registry_lock:
        worker_registry[worker["worker_id"]] = worker

    # Sync local worker state into the versioned replicated registry
    sync_local_replication()

    print(
        f"[REGISTER] worker={worker['worker_id']} "
        f"models={worker['models']} "
        f"endpoint={worker['host']}:{worker['port']}"
    )

    return jsonify({
        "message": "worker registered",
        "worker": worker
    }), 201


@app.get("/workers")
def list_workers():
    with registry_lock:
        workers = list(worker_registry.values())

    return jsonify({
        "count": len(workers),
        "workers": workers
    })

# ----------------------------------------------------------------------
# Middleware Peer Communication
# ----------------------------------------------------------------------

@app.get("/peers")
def list_peers():
    """
    Return all known middleware peers.

    The local node is excluded from the peer registry.
    Peer status reflects the last known state, not necessarily
    current network reachability.
    """

    peers = peer_registry.get_all_peers()

    return jsonify({
        "node_id": NODE_ID,
        "count": len(peers),
        "peers": peers
    }), 200


@app.post("/peer/join")
def join_peer():
    """
    Register a middleware node as a known peer.

    Registration does not automatically establish peer health
    or synchronize worker registries.
    """

    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return jsonify({
            "error": "request body must be a JSON object"
        }), 400

    required_fields = ["node_id", "host", "port"]

    missing_fields = [
        field for field in required_fields
        if field not in data
    ]

    if missing_fields:
        return jsonify({
            "error": "missing required fields",
            "missing": missing_fields
        }), 400

    if data["node_id"] == NODE_ID:
        return jsonify({
            "error": "middleware cannot register itself as a peer"
        }), 400

    try:
        peer = peer_registry.add_or_update_peer(
            node_id=data["node_id"],
            host=data["host"],
            port=data["port"]
        )
    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400

    print(
        f"[PEER JOIN] node={NODE_ID} "
        f"peer={peer['node_id']} "
        f"endpoint={peer['host']}:{peer['port']}"
    )

    return jsonify({
        "message": "peer registered",
        "node_id": NODE_ID,
        "peer": peer,
        "peer_count": peer_registry.get_peer_count()
    }), 200


# ----------------------------------------------------------------------
# Middleware Gossip Reception
# ----------------------------------------------------------------------

def validate_gossip_message(data):
    """
    Validate incoming gossip before modifying stored state.

    Supports legacy worker snapshots, versioned worker
    records, and deletion tombstones.
    """

    if not isinstance(data, dict):
        raise ValueError(
            "gossip payload must be a JSON object"
        )

    sender_id = data.get("sender_id")

    if not isinstance(sender_id, str) or not sender_id.strip():
        raise ValueError(
            "sender_id must be a non-empty string"
        )

    sender_id = sender_id.strip()

    if sender_id == NODE_ID:
        raise ValueError(
            "middleware cannot receive its own gossip"
        )

    workers = data.get("workers")

    if not isinstance(workers, list):
        raise ValueError("workers must be a list")

    validated_workers = []
    seen = set()

    for index, worker in enumerate(workers):
        if not isinstance(worker, dict):
            raise ValueError(
                f"worker entry {index} must be a JSON object"
            )

        worker_id = worker.get("worker_id")
        owner = worker.get("owner")

        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError(
                f"worker entry {index} has invalid worker_id"
            )

        if not isinstance(owner, str) or not owner.strip():
            raise ValueError(
                f"worker entry {index} has invalid owner"
            )

        worker_id = worker_id.strip()
        owner = owner.strip()

        key = (owner, worker_id)

        if key in seen:
            raise ValueError(
                f"duplicate worker entry: {owner}/{worker_id}"
            )

        seen.add(key)

        deleted = worker.get("deleted", False)

        if not isinstance(deleted, bool):
            raise ValueError(
                f"worker entry {index} has invalid deleted flag"
            )

        has_version = "version" in worker

        if has_version:
            VersionedWorkerRegistry.validate_version(
                worker["version"]
            )

        # Deleted records need only identity and version.
        if deleted:
            if not has_version:
                raise ValueError(
                    "deleted worker records require a version"
                )

            validated_workers.append({
                "worker_id": worker_id,
                "owner": owner,
                "deleted": True,
                "version": dict(worker["version"])
            })

            continue

        host = worker.get("host")
        port = worker.get("port")
        models = worker.get("models")
        status = worker.get("status")
        active_requests = worker.get("active_requests", 0)
        capacity = worker.get("capacity")

        if not isinstance(host, str) or not host.strip():
            raise ValueError(
                f"worker entry {index} has invalid host"
            )

        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError(
                f"worker entry {index} has invalid port"
            )

        if (
            not isinstance(models, list)
            or any(
                not isinstance(model, str) or not model.strip()
                for model in models
            )
        ):
            raise ValueError(
                f"worker entry {index} has invalid models"
            )

        if status not in {"healthy", "unavailable"}:
            raise ValueError(
                f"worker entry {index} has invalid status"
            )

        if (
            not isinstance(active_requests, int)
            or isinstance(active_requests, bool)
            or active_requests < 0
        ):
            raise ValueError(
                f"worker entry {index} has invalid active_requests"
            )

        if capacity is not None and (
            not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or capacity <= 0
        ):
            raise ValueError(
                f"worker entry {index} has invalid capacity"
            )

        record = {
            "worker_id": worker_id,
            "host": host.strip(),
            "port": port,
            "models": list(models),
            "status": status,
            "owner": owner,
            "active_requests": active_requests,
            "capacity": capacity
        }

        if has_version:
            record["version"] = dict(worker["version"])
            record["deleted"] = False

        validated_workers.append(record)

    return sender_id, validated_workers



@app.post("/gossip")
def receive_gossip():
    """
    Receive worker snapshots from another middleware node.

    Received workers are not added to the local routing registry.
    """

    data = request.get_json(silent=True)

    try:
        sender_id, workers = validate_gossip_message(data)
    except ValueError as exc:
        return jsonify({
            "error": str(exc)
        }), 400

    with received_gossip_lock:
        received_gossip[sender_id] = {
            "sender_id": sender_id,
            "workers": workers,
            "received_at": time.time()
        }

    # Apply versioned updates to the replicated registry.
    # Legacy messages without versions remain snapshot-only.
    outcomes = replicated_registry.apply_remote_batch(workers)

    if outcomes:
        print(
            f"[REPLICATION MERGE] node={NODE_ID} "
            f"results={outcomes}"
        )

    print(
        f"[GOSSIP RECEIVED] node={NODE_ID} "
        f"sender={sender_id} workers={len(workers)}"
    )

    return jsonify({
        "message": "gossip received",
        "node_id": NODE_ID,
        "sender_id": sender_id,
        "worker_count": len(workers)
    }), 200


@app.get("/gossip/received")
def list_received_gossip():
    """
    Inspect received gossip snapshots for testing.
    """

    with received_gossip_lock:
        sources = [
            {
                "sender_id": entry["sender_id"],
                "received_at": entry["received_at"],
                "worker_count": len(entry["workers"]),
                "workers": [
                    {
                        **worker,
                        "models": list(worker.get("models", []))
                    }
                    for worker in entry["workers"]
                ]
            }
            for _, entry in sorted(received_gossip.items())
        ]

    return jsonify({
        "node_id": NODE_ID,
        "source_count": len(sources),
        "sources": sources
    }), 200



# ----------------------------------------------------------------------
# Consolidated Worker View
# ----------------------------------------------------------------------

def build_consolidated_worker_view():
    """
    Combine local workers with fresh, directly owned gossip records.

    Local workers take priority.
    Conflicting remote ownership claims are excluded.
    Does not modify the active worker_registry.
    """

    now = time.time()

    # Copy local worker state under its existing lock.
    with registry_lock:
        local_workers = {}

        for worker in worker_registry.values():
            worker_id = worker["worker_id"]

            local_workers[worker_id] = {
                "worker_id": worker_id,
                "host": worker["host"],
                "port": worker["port"],
                "models": list(worker.get("models", [])),
                "status": worker["status"],
                "owner": NODE_ID,
                "active_requests": worker.get("active_requests", 0),
                "capacity": worker.get("capacity"),
                "source": "local",
                "last_gossip_at": None
            }

    # Copy received snapshots under the gossip lock.
    with received_gossip_lock:
        snapshots = [
            {
                "sender_id": entry["sender_id"],
                "received_at": entry["received_at"],
                "workers": [
                    {
                        **worker,
                        "models": list(worker.get("models", []))
                    }
                    for worker in entry["workers"]
                ]
            }
            for entry in received_gossip.values()
        ]

    # Group remote ownership claims by worker ID.
    claims = {}

    for snapshot in snapshots:
        sender_id = snapshot["sender_id"]
        received_at = snapshot["received_at"]

        # Ignore stale snapshots.
        if now - received_at > GOSSIP_SNAPSHOT_TTL:
            continue

        for worker in snapshot["workers"]:
            # Tombstones are not active worker records.
            if worker.get("deleted", False):
                continue

            worker_id = worker["worker_id"]

            # Only accept directly owned worker information.
            # Forwarded gossip needs versioning in a later phase.
            if worker["owner"] != sender_id:
                continue

            # Local registrations are authoritative on this node.
            if worker_id in local_workers:
                continue

            claims.setdefault(worker_id, []).append({
                "sender_id": sender_id,
                "received_at": received_at,
                "worker": worker
            })

    merged = dict(local_workers)
    conflicts = []

    for worker_id, worker_claims in claims.items():
        owners = {
            claim["sender_id"]
            for claim in worker_claims
        }

        # Without ownership epochs, conflicting claims cannot
        # be resolved safely using arrival time alone.
        if len(owners) > 1:
            conflicts.append(worker_id)
            continue

        claim = worker_claims[0]
        worker = claim["worker"]

        merged[worker_id] = {
            **worker,
            "models": list(worker.get("models", [])),
            "source": "gossip",
            "last_gossip_at": claim["received_at"]
        }

    workers = [
        merged[worker_id]
        for worker_id in sorted(merged)
    ]

    return {
        "count": len(workers),
        "local_count": sum(
            worker["source"] == "local" for worker in workers
        ),
        "remote_count": sum(
            worker["source"] == "gossip" for worker in workers
        ),
        "conflict_count": len(conflicts),
        "conflicts": sorted(conflicts),
        "workers": workers
    }


@app.get("/gossip/workers")
def list_consolidated_gossip_workers():
    """
    Inspect the consolidated worker view.

    Remote workers remain excluded from active inference routing.
    """

    result = build_consolidated_worker_view()

    return jsonify({
        "node_id": NODE_ID,
        **result
    }), 200


# ----------------------------------------------------------------------
# Versioned Worker Registry Synchronization
# ----------------------------------------------------------------------

def sync_local_replication():
    """
    Synchronize authoritative local worker state
    with the versioned replicated registry.
    """

    with registry_lock:
        workers = [
            {
                **worker,
                "models": list(worker.get("models", []))
            }
            for worker in worker_registry.values()
        ]

        # Keep both operations serialized so an older
        # snapshot cannot overwrite a newer local update.
        replicated_registry.sync_local_workers(workers)


@app.get("/replication/workers")
def list_replicated_workers():
    """
    Inspect versioned local and remote worker state.
    """

    sync_local_replication()

    return jsonify(
        replicated_registry.replication_snapshot()
    ), 200


# ----------------------------------------------------------------------
# Distributed Inference Routing
# ----------------------------------------------------------------------

def get_routing_candidates(model):
    """
    Combine local workers with accepted, fresh,
    trusted remote worker records.
    """

    with registry_lock:
        local_workers = [
            dict(worker)
            for worker in worker_registry.values()
        ]

    snapshot = (
        replicated_registry.replication_snapshot()
        if ENABLE_DISTRIBUTED_ROUTING
        else {}
    )

    trusted_owners = {
        peer["node_id"]
        for peer in peer_registry.get_all_peers()
    }

    return build_routing_candidates(
        local_workers=local_workers,
        replica_snapshot=snapshot,
        node_id=NODE_ID,
        model=model,
        include_remote=ENABLE_DISTRIBUTED_ROUTING,
        trusted_owners=trusted_owners,
        allowed_remote_hosts=REMOTE_WORKER_ALLOWED_HOSTS,
        remote_port_range=(
            REMOTE_WORKER_MIN_PORT,
            REMOTE_WORKER_MAX_PORT
        )
    )


@app.get("/routing/candidates")
def list_routing_candidates():
    """
    Inspect eligible local and remote routing candidates.
    """

    model = request.args.get("model")

    if not model:
        return jsonify({
            "error": "model is required"
        }), 400

    candidates = get_routing_candidates(model)

    return jsonify({
        "node_id": NODE_ID,
        "distributed_routing_enabled": ENABLE_DISTRIBUTED_ROUTING,
        "model": model,
        "count": len(candidates),
        "local_count": sum(
            w["route_scope"] == "local"
            for w in candidates
        ),
        "remote_count": sum(
            w["route_scope"] == "remote"
            for w in candidates
        ),
        "workers": candidates
    }), 200


@app.post("/infer")
def infer():
    data = request.get_json(silent=True) or {}

    model = data.get("model")
    input_data = data.get("input")

    if model is None:
        return jsonify({
            "error": "model is required"
        }), 400

    if input_data is None:
        return jsonify({
            "error": "input is required"
        }), 400

    # Obtain eligible local and remote workers.
    candidates = get_routing_candidates(model)

    if not candidates:
        return jsonify({
            "error": "no worker supports requested model",
            "model": model
        }), 404

    attempted = set()
    last_error = None
    last_failed_worker = None

    max_attempts = min(
        MAX_INFER_ATTEMPTS,
        len(candidates)
    )

    for attempt in range(1, max_attempts + 1):

        # Exclude workers already attempted or quarantined.
        available = [
            worker
            for worker in candidates
            if worker_key(worker) not in attempted
            and not routing_quarantine.is_quarantined(worker)
        ]

        if not available:
            break

        # Preserve the five existing routing strategies.
        selected_policy = ROUTING_POLICY
        policy_reason = "manual_override"

        if ROUTING_POLICY == "adaptive":
            selected_policy, policy_reason = choose_routing_policy(
                available
            )

        selected_worker = select_worker(
            selected_policy,
            model,
            available
        )

        attempted.add(worker_key(selected_worker))

        worker_url = (
            f"http://{selected_worker['host']}:"
            f"{selected_worker['port']}/infer"
        )

        print(
            f"[DISTRIBUTED ROUTE] node={NODE_ID} "
            f"model={model} "
            f"worker={selected_worker['worker_id']} "
            f"owner={selected_worker['owner']} "
            f"scope={selected_worker['route_scope']} "
            f"policy={selected_policy} "
            f"attempt={attempt}"
        )

        try:
            response = requests.post(
                worker_url,
                json={
                    "model": model,
                    "input": input_data
                },
                timeout=5
            )

        except requests.RequestException as exc:

            # Retry only when the connection was not established.
            if safe_to_retry(exc):
                routing_quarantine.mark_failed(
                    selected_worker
                )

                last_error = str(exc)
                last_failed_worker = selected_worker["worker_id"]
                continue

            # Do not retry ambiguous errors: execution may
            # already have occurred on the selected worker.
            return jsonify({
                "error": (
                    "failed to contact worker; "
                    "request was not retried"
                ),
                "worker_id": selected_worker["worker_id"],
                "details": str(exc)
            }), 502

        # Forward the worker's response without automatically
        # retrying HTTP errors.
        try:
            payload = response.json()

        except ValueError:
            payload = {
                "error": "worker returned non-JSON response",
                "worker_id": selected_worker["worker_id"]
            }

        result = jsonify(payload)

        result.headers["X-Routing-Policy"] = selected_policy
        result.headers["X-Routing-Reason"] = policy_reason
        result.headers["X-Worker-Owner"] = (
            selected_worker["owner"]
        )
        result.headers["X-Worker-Scope"] = (
            selected_worker["route_scope"]
        )
        result.headers["X-Routing-Attempts"] = str(attempt)

        return result, response.status_code

    # All allowed attempts failed before connecting.
    if last_error is not None:
        return jsonify({
            "error": "no reachable worker could be selected",
            "worker_id": last_failed_worker,
            "details": last_error,
            "attempted": len(attempted)
        }), 502

    return jsonify({
        "error": "all matching workers are temporarily quarantined",
        "model": model
    }), 503


# ----------------------------------------------------------------------
# Worker Heartbeats
# ----------------------------------------------------------------------

@app.post("/heartbeat")
def receive_heartbeat():
    data = request.get_json(silent=True) or {}
    worker_id = data.get("worker_id")

    if not worker_id:
        return jsonify({"error": "worker_id is required"}), 400

    with registry_lock:
        if worker_id not in worker_registry:
            return jsonify({
                "error": "worker not registered",
                "worker_id": worker_id
            }), 404

        worker_registry[worker_id]["last_heartbeat"] = time.time()
        worker_registry[worker_id]["status"] = "healthy"
        worker_registry[worker_id]["active_requests"] = data.get("active_requests", 0)
        worker_registry[worker_id]["capacity"] = data.get("capacity")
        worker_registry[worker_id]["cpu_load_1m"] = data.get("cpu_load_1m")

    # Sync local worker state into the versioned replicated registry
    sync_local_replication()

    print(
        f"[HEARTBEAT] worker={worker_id} "
        f"load={data.get('active_requests', 0)}"
    )


    return jsonify({
        "message": "heartbeat received",
        "worker_id": worker_id
    }), 200

# function to find workers that support a specific model
def find_workers_by_model(model):
    with registry_lock:
        matching_workers = [
            worker.copy()
            for worker in worker_registry.values()
            if (
                model in worker["models"]
                and worker["status"] == "healthy"
            )
        ]

    return matching_workers

# function to load seed peers from a JSON file
def load_seed_peers(peers_file):
    """
    Load known middleware peers from a JSON configuration file.

    The local middleware node is excluded from its own peer registry.
    All loaded peers initially have unknown health status.
    """

    try:
        entries = json.loads(
            Path(peers_file).read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Failed to load peers file '{peers_file}': {exc}"
        ) from exc

    if not isinstance(entries, list):
        raise ValueError(
            "Peers configuration must be a JSON array"
        )

    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(
                f"Peer entry {index} must be a JSON object"
            )

        node_id = entry.get("node_id")

        # Do not register this middleware as its own peer.
        if node_id == NODE_ID:
            continue

        try:
            peer_registry.add_or_update_peer(
                node_id=node_id,
                host=entry.get("host"),
                port=entry.get("port")
            )
        except ValueError as exc:
            raise ValueError(
                f"Invalid peer entry {index}: {exc}"
            ) from exc

    print(
        f"[PEERS] node={NODE_ID} "
        f"loaded={peer_registry.get_peer_count()} "
        f"file={peers_file}"
    )

    return peer_registry.get_peer_count()

# ----------------------------------------------------------------------
# Automatic Middleware Peer Discovery
# ----------------------------------------------------------------------

# function to discover peers once by contacting known peers
def discover_peers_once(local_host, local_port):
    """
    Contact each known peer, announce this middleware, and
    retrieve additional peer information.

    Unreachable peers remain in the registry for future retries.
    """

    announcement = {
        "node_id": NODE_ID,
        "host": local_host,
        "port": local_port
    }

    # Use a snapshot so newly discovered peers are contacted
    # during the next discovery cycle.
    known_peers = peer_registry.get_all_peers()

    for peer in known_peers:
        peer_id = peer["node_id"]
        peer_url = f"http://{peer['host']}:{peer['port']}"

        try:
            # Announce ourselves to the remote middleware.
            response = requests.post(
                f"{peer_url}/peer/join",
                json=announcement,
                timeout=PEER_REQUEST_TIMEOUT
            )
            response.raise_for_status()

            # Retrieve the remote middleware's known peers.
            response = requests.get(
                f"{peer_url}/peers",
                timeout=PEER_REQUEST_TIMEOUT
            )
            response.raise_for_status()

            data = response.json()

            if (
                not isinstance(data, dict)
                or data.get("node_id") != peer_id
                or not isinstance(data.get("peers"), list)
            ):
                raise ValueError("Invalid peer discovery response")

        except (requests.RequestException, ValueError) as exc:
            peer_registry.set_peer_status(
                peer_id,
                "unavailable"
            )

            if peer["status"] != "unavailable":
                print(
                    f"[PEER UNAVAILABLE] node={NODE_ID} "
                    f"peer={peer_id} error={exc}"
                )

            continue

        # Successful HTTP communication confirms peer availability.
        peer_registry.set_peer_status(
            peer_id,
            "healthy"
        )

        if peer["status"] != "healthy":
            print(
                f"[PEER HEALTHY] node={NODE_ID} "
                f"peer={peer_id}"
            )

        # Learn about middleware nodes known by this peer.
        for discovered in data["peers"]:
            if not isinstance(discovered, dict):
                continue

            discovered_id = discovered.get("node_id")

            # Never register the local node as its own peer.
            if discovered_id == NODE_ID:
                continue

            existing = peer_registry.get_peer(discovered_id)

            try:
                peer_registry.add_or_update_peer(
                    node_id=discovered_id,
                    host=discovered.get("host"),
                    port=discovered.get("port")
                )
            except ValueError:
                continue

            if existing is None:
                print(
                    f"[PEER DISCOVERED] node={NODE_ID} "
                    f"peer={discovered_id}"
                )

# function to start a background thread for periodic peer discovery
def start_peer_discovery(local_host, local_port):
    """
    Run peer discovery periodically in a daemon thread.

    The thread does not block middleware HTTP request handling.
    """

    def discovery_loop():
        while True:
            try:
                discover_peers_once(local_host, local_port)
            except Exception as exc:
                print(
                    f"[PEER DISCOVERY ERROR] "
                    f"node={NODE_ID} error={exc}"
                )

            time.sleep(PEER_DISCOVERY_INTERVAL)

    thread = threading.Thread(
        target=discovery_loop,
        name="peer-discovery",
        daemon=True
    )

    thread.start()

    return thread

# gossip state export
def export_gossip_state():
    """
    Export versioned local workers, replicated remote
    workers, and deletion tombstones.
    """

    sync_local_replication()

    return {
        "workers": replicated_registry.export_gossip_records()
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Distributed AI Middleware"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000
    )

    parser.add_argument(
        "--routing-policy",
        choices=[
            "round_robin",
            "least_loaded",
            "capacity_aware",
            "power_of_two",
            "adaptive"
        ],
        default="adaptive"
    )

    parser.add_argument(
        "--node-id",
        default=None,
        help="Unique identifier for this middleware node"
    )

    parser.add_argument(
        "--peers-file",
        default=None,
        help="JSON file containing known middleware peers"
    )

    parser.add_argument(
        "--distributed-routing",
        action="store_true",
        help="Enable routing to eligible remote workers"
    )

    args = parser.parse_args()

    ROUTING_POLICY = args.routing_policy
    ENABLE_DISTRIBUTED_ROUTING = args.distributed_routing

    NODE_ID = args.node_id or f"middleware-{args.port}"

    # Initialize replication with the final middleware node ID.
    replicated_registry = VersionedWorkerRegistry(NODE_ID)

    if args.peers_file:
        try:
            load_seed_peers(args.peers_file)
        except ValueError as exc:
            parser.error(str(exc))

    start_failure_detector(
        worker_registry,
        registry_lock,
        HEARTBEAT_TIMEOUT,
        HEALTH_CHECK_INTERVAL
    )

    start_peer_discovery(
        local_host=args.host,
        local_port=args.port
    )

    start_gossip(
        node_id=NODE_ID,
        peer_registry=peer_registry,
        get_state_fn=export_gossip_state
    )


    print(
        f"[MIDDLEWARE] node={NODE_ID} "
        f"running at http://{args.host}:{args.port} "
        f"routing_policy={ROUTING_POLICY} "
        f"known_peers={peer_registry.get_peer_count()}"
    )

    app.run(
        host=args.host,
        port=args.port
    )