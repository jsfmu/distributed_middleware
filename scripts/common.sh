#!/usr/bin/env bash
# Shared settings and helpers. The other scripts load this with "source".

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT/logs"
RUN_DIR="$ROOT/run"
mkdir -p "$LOG_DIR" "$RUN_DIR"

PYTHON="${PYTHON:-python3}"
HOST="127.0.0.1"
MIDDLEWARE_BASE_PORT="${MIDDLEWARE_BASE_PORT:-8000}"   # middleware-1 -> 8000, middleware-2 -> 8001, ...
WORKER_BASE_PORT="${WORKER_BASE_PORT:-9101}"           # worker-1 -> 9101 (9001 is taken by Jupyter)

middleware_port() { echo $(( MIDDLEWARE_BASE_PORT + $1 - 1 )); }
worker_port()     { echo $(( WORKER_BASE_PORT + $1 - 1 )); }

# True if something is already listening on this port
port_in_use() {
    "$PYTHON" -c '
import socket
import sys

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.5)
    result = sock.connect_ex(("127.0.0.1", int(sys.argv[1])))

sys.exit(0 if result == 0 else 1)
' "$1"
}

# wait_for_http URL [SECONDS]: wait until the URL answers
wait_for_http() {
    local url="$1" tries=$(( ${2:-10} * 5 )) i
    for ((i = 0; i < tries; i++)); do
        curl -s -o /dev/null "$url" && return 0
        sleep 0.2
    done
    echo "[TIMEOUT] $url did not respond" >&2
    return 1
}

# start_process NAME PORT COMMAND...: run COMMAND in the background,
# log to logs/NAME.log and remember its PID in run/NAME.pid
start_process() {
    local name="$1" port="$2"
    shift 2

    if port_in_use "$port"; then
        echo "[SKIP] port $port is already in use, not starting $name" >&2
        return 1
    fi

    "$@" > "$LOG_DIR/$name.log" 2>&1 &
    echo $! > "$RUN_DIR/$name.pid"
    echo "[STARTED] $name on port $port (pid $!, log logs/$name.log)"
}

# stop_process NAME [SIGNAL]: stop a process started by start_process
stop_process() {
    local pidfile="$RUN_DIR/$1.pid"
    [ -f "$pidfile" ] || return 0
    kill "-${2:-TERM}" "$(cat "$pidfile")" 2>/dev/null
    rm -f "$pidfile"
    echo "[STOPPED] $1"
}

# middleware_list PRIMARY COUNT: "host:port,..." with node PRIMARY first,
# so a worker registers there but can fail over to the others
middleware_list() {
    local primary="$1" count="$2" i
    local list="$HOST:$(middleware_port "$primary")"
    for ((i = 1; i <= count; i++)); do
        [ "$i" -eq "$primary" ] || list="$list,$HOST:$(middleware_port "$i")"
    done
    echo "$list"
}

# write_peers_file COUNT: run/peers.json in the shared peer format
write_peers_file() {
    local count="$1" i line
    echo "[" > "$RUN_DIR/peers.json"
    for ((i = 1; i <= count; i++)); do
        line="  {\"node_id\": \"middleware-$i\", \"host\": \"$HOST\", \"port\": $(middleware_port "$i")}"
        [ "$i" -lt "$count" ] && line="$line,"
        echo "$line" >> "$RUN_DIR/peers.json"
    done
    echo "]" >> "$RUN_DIR/peers.json"
}

# True once middleware.py accepts --node-id (i.e. after peers.py is merged)
middleware_supports_peers() {
    "$PYTHON" "$ROOT/src/middleware/middleware.py" --help 2>/dev/null | grep -q -- "--node-id"
}

# worker_count_on PORT: how many workers that middleware knows about
worker_count_on() {
    curl -s "http://$HOST:$1/workers" |
        "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["count"])' 2>/dev/null || echo 0
}
