#!/usr/bin/env bash

source "$(dirname "$0")/common.sh"

COUNT="${1:-2}"
MIDDLEWARE_COUNT="${2:-2}"
MODELS="${MODELS:-llama}"

for ((i = 1; i <= COUNT; i++)); do
    primary=$(( (i - 1) % MIDDLEWARE_COUNT + 1 ))
    port=$(worker_port "$i")

    start_process "worker-$i" "$port" \
        "$PYTHON" -u "$ROOT/src/worker/worker.py" \
        --id "worker-$i" --port "$port" --models "$MODELS" \
        --middleware "$(middleware_list "$primary" "$MIDDLEWARE_COUNT")" \
        --heartbeat-interval 2 $WORKER_ARGS
done

for ((i = 1; i <= COUNT; i++)); do
    wait_for_http "http://$HOST:$(worker_port "$i")/health" || exit 1
done

echo "[READY] worker-1 .. worker-$COUNT"
