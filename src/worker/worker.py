import argparse
import time

import requests
from flask import Flask, jsonify, request

from heartbeat import start_heartbeat_thread


app = Flask(__name__)


WORKER_ID = None
WORKER_HOST = None
WORKER_PORT = None
MODELS = []

MIDDLEWARE_HOST = None
MIDDLEWARE_PORT = None

HEARTBEAT_INTERVAL = 10


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "worker",
        "worker_id": WORKER_ID
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

    if model not in MODELS:
        return jsonify({
            "error": "model not supported",
            "worker_id": WORKER_ID,
            "requested_model": model,
            "supported_models": MODELS
        }), 400

    time.sleep(0.5)

    result = (
        f"Simulated inference result from "
        f"{WORKER_ID} using {model}: {input_data}"
    )

    return jsonify({
        "worker_id": WORKER_ID,
        "model": model,
        "result": result
    })


def register_with_middleware():
    url = (
        f"http://{MIDDLEWARE_HOST}:"
        f"{MIDDLEWARE_PORT}/register"
    )

    payload = {
        "worker_id": WORKER_ID,
        "host": WORKER_HOST,
        "port": WORKER_PORT,
        "models": MODELS
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=5
        )

        response.raise_for_status()

        print(
            f"[REGISTERED] worker={WORKER_ID} "
            f"with middleware"
        )

    except requests.RequestException as exc:
        print(
            f"[ERROR] registration failed: {exc}"
        )

        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AI Inference Worker"
    )

    parser.add_argument(
        "--id",
        required=True
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True
    )

    parser.add_argument(
        "--models",
        required=True,
        help="Comma-separated model names"
    )

    parser.add_argument(
        "--middleware-host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--middleware-port",
        type=int,
        default=8000
    )

    args = parser.parse_args()

    WORKER_ID = args.id
    WORKER_HOST = args.host
    WORKER_PORT = args.port

    MODELS = [
        model.strip()
        for model in args.models.split(",")
        if model.strip()
    ]

    MIDDLEWARE_HOST = args.middleware_host
    MIDDLEWARE_PORT = args.middleware_port

    register_with_middleware()

    heartbeat_url = (
        f"http://{MIDDLEWARE_HOST}:"
        f"{MIDDLEWARE_PORT}/heartbeat"
    )

    start_heartbeat_thread(
        WORKER_ID,
        heartbeat_url,
        HEARTBEAT_INTERVAL
    )

    print(
        f"[WORKER] {WORKER_ID} running at "
        f"http://{WORKER_HOST}:{WORKER_PORT}"
    )

    app.run(
        host=WORKER_HOST,
        port=WORKER_PORT
    )