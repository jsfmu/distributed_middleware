import argparse
import time

from flask import Flask, jsonify, request


app = Flask(__name__)


@app.get("/health")
def health():
    return jsonify({
        "status": "healthy",
        "service": "worker"
    })


@app.post("/infer")
def infer():
    data = request.get_json(silent=True) or {}

    input_data = data.get("input")

    if input_data is None:
        return jsonify({
            "error": "input is required"
        }), 400

    # Simulate inference work.
    time.sleep(0.5)

    result = f"Simulated inference result for: {input_data}"

    return jsonify({
        "result": result
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="AI Inference Worker"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9001
    )

    args = parser.parse_args()

    print(
        f"[WORKER] running at "
        f"http://{args.host}:{args.port}"
    )

    app.run(
        host=args.host,
        port=args.port
    )