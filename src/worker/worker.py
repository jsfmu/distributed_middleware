import argparse
import time
import random

import requests
from flask import Flask, jsonify, request

from heartbeat import start_heartbeat_thread


app = Flask(__name__)
from metrics import worker_metrics

WORKER_ID = None
WORKER_HOST = None
WORKER_PORT = None
MODELS = []

MIDDLEWARE_HOST = None
MIDDLEWARE_PORT = None

INFERENCE_DELAY = 0.5
DELAY_JITTER = 0.0
LOAD_PENALTY = 0.0

HEARTBEAT_INTERVAL = 10
HEARTBEAT_INTERVAL = args.heartbeat_interval


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "worker",
        "worker_id": WORKER_ID
    })

@app.get("/metrics")
def get_metrics():
    return jsonify({
        "worker_id": WORKER_ID,
        **worker_metrics.snapshot()
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

    with worker_metrics.track_request():
        time.sleep(compute_delay())

        result = (
            f"Simulated inference result from "
            f"{WORKER_ID} using {model}: {input_data}"
        )

    return jsonify({
        "worker_id": WORKER_ID,
        "model": model,
        "result": result
    })

def compute_delay():
    delay = INFERENCE_DELAY

    if DELAY_JITTER > 0:
        delay += random.uniform(-DELAY_JITTER, DELAY_JITTER)

    if LOAD_PENALTY > 0:
        # get_load() includes this request, so subtract 1 to count the others
        other_requests = worker_metrics.get_load() - 1
        delay *= 1 + LOAD_PENALTY * other_requests

    return max(delay, 0.0)

def heartbeat_payload():
    return worker_metrics.snapshot()

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
        "--delay",
        type=float,
        default=INFERENCE_DELAY,
        help="Base seconds each inference takes"
    )

    parser.add_argument(
        "--jitter",
        type=float,
        default=0.0,
        help="Random +/- seconds added to each delay"
    )

    parser.add_argument(
        "--load-penalty",
        type=float,
        default=0.0,
        help="Extra delay fraction per other active request"
    )

    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=HEARTBEAT_INTERVAL,
        help="Seconds between heartbeats"
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
        help="Comma-separated model names",
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
    INFERENCE_DELAY = args.delay,
    DELAY_JITTER = args.jitter,
    LOAD_PENALTY = args.load_penalty,
    HEARTBEAT_INTERVAL = args.heartbeat_interval

    register_with_middleware()

    heartbeat_url = (
        f"http://{MIDDLEWARE_HOST}:"
        f"{MIDDLEWARE_PORT}/heartbeat"
    )

    start_heartbeat_thread(
        WORKER_ID,
        heartbeat_url,
        HEARTBEAT_INTERVAL,
        payload_fn=heartbeat_payload
    )

    print(
        f"[WORKER] {WORKER_ID} running at "
        f"http://{WORKER_HOST}:{WORKER_PORT}"
    )

    app.run(
        host=WORKER_HOST,
        port=WORKER_PORT
    )