#!/usr/bin/env bash

source "$(dirname "$0")/common.sh"

COUNT="${1:-2}"
FIRST="${2:-1}"
LAST=$(( FIRST + COUNT - 1 ))

# The first cluster writes the peer list. A node joining later reuses it as its seed list.
if [ "$FIRST" -eq 1 ]; then
    write_peers_file "$COUNT"
fi

PEERS_SUPPORTED=no
if middleware_supports_peers; then
    PEERS_SUPPORTED=yes
fi

for ((i = FIRST; i <= LAST; i++)); do
    port=$(middleware_port "$i")

    if [ "$PEERS_SUPPORTED" = yes ]; then
        start_process "middleware-$i" "$port" \
            "$PYTHON" -u "$ROOT/src/middleware/middleware.py" --port "$port" \
            --node-id "middleware-$i" --peers-file "$RUN_DIR/peers.json" $MIDDLEWARE_ARGS
    else
        start_process "middleware-$i" "$port" \
            "$PYTHON" -u "$ROOT/src/middleware/middleware.py" --port "$port" $MIDDLEWARE_ARGS
    fi
done

for ((i = FIRST; i <= LAST; i++)); do
    wait_for_http "http://$HOST:$(middleware_port "$i")/health" || exit 1
done

echo "[READY] middleware-$FIRST .. middleware-$LAST (peer flags: $PEERS_SUPPORTED)"
