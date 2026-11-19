import argparse
import time
import random

import requests
from flask import Flask, jsonify, request

from heartbeat import start_heartbeat_thread
from metrics import cpu_load_avg, hardware_info, worker_metrics

app = Flask(__name__)

WORKER_ID = None
WORKER_HOST = None
WORKER_PORT = None
MODELS = []

MIDDLEWARE_NODES = []
current_middleware = 0

INFERENCE_DELAY = 0.5
DELAY_JITTER = 0.0
LOAD_PENALTY = 0.0

HEARTBEAT_INTERVAL = 10

HARDWARE = {}



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
    payload = worker_metrics.snapshot()
    payload["cpu_load_1m"] = cpu_load_avg()
    return payload


def parse_middleware_list(text):
    nodes = []

    for item in text.split(","):
        item = item.strip()
        if not item:
            continue

        host, _, port = item.rpartition(":")
        nodes.append((host or "127.0.0.1", int(port)))

    return nodes


def current_heartbeat_url():
    host, port = MIDDLEWARE_NODES[current_middleware]
    return f"http://{host}:{port}/heartbeat"


def register_with_middleware():
    global current_middleware

    payload = {
        "worker_id": WORKER_ID,
        "host": WORKER_HOST,
        "port": WORKER_PORT,
        "models": MODELS,
        "capacity": worker_metrics.capacity,
        "hardware": HARDWARE
    }

    for offset in range(len(MIDDLEWARE_NODES)):
        index = (current_middleware + offset) % len(MIDDLEWARE_NODES)
        host, port = MIDDLEWARE_NODES[index]

        try:
            response = requests.post(
                f"http://{host}:{port}/register",
                json=payload,
                timeout=5
            )
            response.raise_for_status()

        except requests.RequestException as exc:
            print(
                f"[REGISTER FAILED] worker={WORKER_ID} "
                f"middleware={host}:{port} error={exc}"
            )
            continue

        current_middleware = index
        print(
            f"[REGISTERED] worker={WORKER_ID} "
            f"middleware={host}:{port}"
        )
        return True

    return False


def reconnect():
    if not register_with_middleware():
        print(
            f"[RECONNECT FAILED] worker={WORKER_ID} "
            f"no middleware reachable, will retry"
        )


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
        "--gpu",
        default="none",
        help="GPU label to report, e.g. none or A100"
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

    parser.add_argument(
        "--middleware",
        default=None,
        help="Comma-separated middleware nodes in preference order, "
             "e.g. 127.0.0.1:8000,127.0.0.1:8001 "
             "(overrides --middleware-host/--middleware-port)"
    )

    parser.add_argument(
        "--standalone",
        action="store_true",
        help="Skip middleware registration (test the worker alone)"
    )

    parser.add_argument(
        "--capacity",
        type=int,
        default=None,
        help="Advertised concurrent request capacity used for routing"
    )

    args = parser.parse_args()

    if args.capacity is not None and args.capacity <= 0:
        parser.error("--capacity must be greater than 0")

    worker_metrics.capacity = args.capacity

    WORKER_ID = args.id
    WORKER_HOST = args.host
    WORKER_PORT = args.port

    MODELS = [
        model.strip()
        for model in args.models.split(",")
        if model.strip()
    ]

    if args.middleware:
        MIDDLEWARE_NODES = parse_middleware_list(args.middleware)
    else:
        MIDDLEWARE_NODES = [(args.middleware_host, args.middleware_port)]
    INFERENCE_DELAY = args.delay
    DELAY_JITTER = args.jitter
    LOAD_PENALTY = args.load_penalty
    HEARTBEAT_INTERVAL = args.heartbeat_interval
    HARDWARE = hardware_info(args.gpu)

    if args.standalone:
        print(
            f"[STANDALONE] worker={WORKER_ID} "
            f"skipping middleware registration"
        )
    else:
        if not register_with_middleware():
            print(f"[ERROR] worker={WORKER_ID} could not reach any middleware")
            raise SystemExit(1)

        start_heartbeat_thread(
            WORKER_ID,
            current_heartbeat_url,
            HEARTBEAT_INTERVAL,
            payload_fn=heartbeat_payload,
            on_failure=reconnect
        )

    print(
        f"[WORKER] {WORKER_ID} running at "
        f"http://{WORKER_HOST}:{WORKER_PORT}"
    )

    app.run(
        host=WORKER_HOST,
        port=WORKER_PORT
    )