#!/usr/bin/env bash

source "$(dirname "$0")/common.sh"

for pidfile in "$RUN_DIR"/*.pid; do
    [ -e "$pidfile" ] || continue     # no pid files means nothing is running
    stop_process "$(basename "$pidfile" .pid)"
done

sleep 1   # give the ports a moment to free up
