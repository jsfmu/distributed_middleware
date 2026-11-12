import argparse
import threading
import time

import requests
import asyncio
import urllib
from flask import Flask, jsonify, request
from collections import defaultdict


HEARTBEAT_URL = "http://127.0.0.1:8000"
PING_COOLDOWN = 30


app = Flask(__name__)

# ----------------------------------------------------------------------
# Worker Registry
# ----------------------------------------------------------------------

worker_registry = {}
registry_lock = threading.Lock()

round_robin_index = defaultdict(int)


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
        "models": data["models"]
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

    # Select a worker using round-robin scheduling
    selected_worker = select_worker_round_robin(
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
        f"policy=round_robin"
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
def recieve_heartbeat():
    data = request.get_json(silent=True) or {}
    worker_id = data.get("worker_id")

    if not worker_id:
        return jsonify({"error": "worker_id is required"}), 400

    with registry_lock:
        if worker_id not in worker_registry:
            return jsonify({"error": "worker_id is required"}), 404
        worker_registry[worker_id]["last_heartbeat"] = time.time()

    print(f"[HEARTBEAT] worker={worker_id}")

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
            if model in worker["models"]
        ]

    return matching_workers

# function to select a worker using round-robin scheduling
def select_worker_round_robin(model, workers):
    if not workers:
        return None

    workers = sorted(
        workers,
        key=lambda worker: worker["worker_id"]
    )

    index = round_robin_index[model] % len(workers)

    selected_worker = workers[index]

    round_robin_index[model] = (
        round_robin_index[model] + 1
    ) % len(workers)

    return selected_worker

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

    args = parser.parse_args()

    print(
        f"[MIDDLEWARE] running at "
        f"http://{args.host}:{args.port}"
    )

    app.run(
        host=args.host,
        port=args.port
    )