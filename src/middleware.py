import argparse

import requests
from flask import Flask, jsonify, request


app = Flask(__name__)


# For the first prototype, the worker address is static.
# Dynamic registration and discovery will be added later.
WORKER_URL = "http://127.0.0.1:9001"


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "middleware"
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