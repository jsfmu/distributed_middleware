import argparse
import json
import threading
import time
from pathlib import Path

import requests
from flask import Flask, jsonify, request

from failure_detector import start_failure_detector
from peers import PeerRegistry
from routing import choose_routing_policy, select_worker

from gossip import start_gossip

HEARTBEAT_TIMEOUT = 30
HEALTH_CHECK_INTERVAL = 1

# Middleware peer discovery settings
PEER_DISCOVERY_INTERVAL = 2
PEER_REQUEST_TIMEOUT = 2

# Choose the routing policy
ROUTING_POLICY = "adaptive"

# Middleware node identity
NODE_ID = "middleware-8000"

# Known middleware peers (separate from the worker registry)
peer_registry = PeerRegistry()

app = Flask(__name__)

# ----------------------------------------------------------------------
# Middleware Health
# ----------------------------------------------------------------------

worker_registry = {}
registry_lock = threading.Lock()

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
# Inference Routing
# ----------------------------------------------------------------------

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

    matching_workers = find_workers_by_model(model)

    if not matching_workers:
        return jsonify({
            "error": "no worker supports requested model",
            "model": model
        }), 404

    # Determine which routing policy to use
    selected_policy = ROUTING_POLICY
    policy_reason = "manual_override"

    if ROUTING_POLICY == "adaptive":
        selected_policy, policy_reason = choose_routing_policy(
            matching_workers
        )

    # Select a worker using the chosen policy
    selected_worker = select_worker(
        selected_policy,
        model,
        matching_workers
    )

    worker_url = (
        f"http://{selected_worker['host']}:"
        f"{selected_worker['port']}/infer"
    )

    print(
        f"[ROUTE] model={model} "
        f"worker={selected_worker['worker_id']} "
        f"load={selected_worker.get('active_requests', 0)} "
        f"capacity={selected_worker.get('capacity')} "
        f"mode={ROUTING_POLICY} "
        f"policy={selected_policy} "
        f"reason={policy_reason}"
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

        result = jsonify(response.json())

        result.headers["X-Routing-Policy"] = selected_policy
        result.headers["X-Routing-Reason"] = policy_reason

        return result, response.status_code

    except requests.RequestException as exc:
        return jsonify({
            "error": "failed to contact worker",
            "worker_id": selected_worker["worker_id"],
            "details": str(exc)
        }), 502

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
    with registry_lock:
        workers = [{
            "worker_id": worker["worker_id"],
            "host": worker["host"],
            "port": worker["port"],
            "models": worker["models"],
            "status": worker["status"],
            "owner": NODE_ID,
            "active_requests": worker.get("active_requests", 0),
            "capacity": worker.get("capacity")
        }
        for worker in worker_registry.values()]
    return {"workers": workers}

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

    args = parser.parse_args()

    ROUTING_POLICY = args.routing_policy

    NODE_ID = args.node_id or f"middleware-{args.port}"

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