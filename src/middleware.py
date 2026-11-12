import argparse
import threading

import requests
from flask import Flask, jsonify, request


app = Flask(__name__)


# Inference still goes to one static worker.
# Dynamic routing will be implemented later.
WORKER_URL = "http://127.0.0.1:9001"


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

    input_data = data.get("input")

    if input_data is None:
        return jsonify({
            "error": "input is required"
        }), 400

    try:
        response = requests.post(
            f"{WORKER_URL}/infer",
            json={
                "input": input_data
            },
            timeout=5
        )

        return jsonify(response.json()), response.status_code

    except requests.RequestException as exc:
        return jsonify({
            "error": "failed to contact worker",
            "details": str(exc)
        }), 502


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