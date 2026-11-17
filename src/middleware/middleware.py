import argparse
import threading
import time

import requests
from flask import Flask, jsonify, request

from failure_detector import start_failure_detector
from routing import select_worker


HEARTBEAT_TIMEOUT = 30
HEALTH_CHECK_INTERVAL = 1

# Choose the routing policy 
ROUTING_POLICY = "least_loaded"

app = Flask(__name__)

# ----------------------------------------------------------------------
# Worker Registry
# ----------------------------------------------------------------------

worker_registry = {}
registry_lock = threading.Lock()

@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "middleware"
    })


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

    # Select a worker based on the routing policy
    selected_worker = select_worker(
        ROUTING_POLICY,
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
        f"policy={ROUTING_POLICY}"
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

        return jsonify(
            response.json()
        ), response.status_code

    except requests.RequestException as exc:
        return jsonify({
            "error": "failed to contact worker",
            "worker_id": selected_worker["worker_id"],
            "details": str(exc)
        }), 502

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
            "capacity_aware"
        ],
        default="least_loaded"
    )

    args = parser.parse_args()

    ROUTING_POLICY = args.routing_policy

    start_failure_detector(
        worker_registry,
        registry_lock,
        HEARTBEAT_TIMEOUT,
        HEALTH_CHECK_INTERVAL
    )

    print(
        f"[MIDDLEWARE] running at "
        f"http://{args.host}:{args.port} "
        f"routing_policy={ROUTING_POLICY}"
    )

    app.run(
        host=args.host,
        port=args.port
    )