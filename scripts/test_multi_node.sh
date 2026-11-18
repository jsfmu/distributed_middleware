source "$(dirname "$0")/common.sh"

FAILURES=0

# check "description" COMMAND...: run COMMAND, print PASS or FAIL
check() {
    local description="$1"
    shift
    if "$@"; then
        echo "  PASS  $description"
    else
        echo "  FAIL  $description"
        FAILURES=$(( FAILURES + 1 ))
    fi
}

equals()    { [ "$1" = "$2" ]; }
not_empty() { [ -n "$1" ]; }
not()       { ! "$@"; }
http_ok()   { curl -sf -o /dev/null "$1"; }

# infer_worker_id PORT: send one request through that middleware, print which worker answered
infer_worker_id() {
    curl -s -X POST "http://$HOST:$1/infer" \
        -H 'Content-Type: application/json' \
        -d '{"model":"llama","input":"test"}' |
        "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin).get("worker_id", ""))' 2>/dev/null
}

# wait_for_count PORT EXPECTED SECONDS: wait until that middleware knows EXPECTED workers
wait_for_count() {
    local i
    for ((i = 0; i < $3; i++)); do
        [ "$(worker_count_on "$1")" = "$2" ] && return 0
        sleep 1
    done
    return 1
}

cleanup() {
    "$ROOT/scripts/stop_all.sh" > /dev/null
}
trap cleanup EXIT

# start_cluster MIDDLEWARE_COUNT WORKER_COUNT
start_cluster() {
    "$ROOT/scripts/start_middleware_nodes.sh" "$1" > /dev/null || exit 1
    "$ROOT/scripts/start_workers.sh" "$2" "$1" > /dev/null || exit 1
}

scenario_independent() {
    echo "=== Scenario: independent middleware nodes"
    start_cluster 2 4

    local expected=2
    [ "$EXPECT_GOSSIP" = 1 ] && expected=4

    check "middleware-1 listens on port 8000" port_in_use "$(middleware_port 1)"
    check "middleware-2 listens on port 8001" port_in_use "$(middleware_port 2)"
    check "middleware-1 knows $expected workers" equals "$(worker_count_on "$(middleware_port 1)")" "$expected"
    check "middleware-2 knows $expected workers" equals "$(worker_count_on "$(middleware_port 2)")" "$expected"

    local w1 w2
    w1=$(infer_worker_id "$(middleware_port 1)")
    w2=$(infer_worker_id "$(middleware_port 2)")
    check "request via middleware-1 served (by ${w1:-nobody})" not_empty "$w1"
    check "request via middleware-2 served (by ${w2:-nobody})" not_empty "$w2"

    cleanup
}

scenario_join() {
    echo "=== Scenario: middleware-3 joins a running cluster"
    start_cluster 2 2
    "$ROOT/scripts/start_middleware_nodes.sh" 1 3 > /dev/null || exit 1

    check "middleware-3 listens on port 8002" port_in_use "$(middleware_port 3)"
    check "middleware-1 still healthy" http_ok "http://$HOST:$(middleware_port 1)/health"
    check "middleware-2 still healthy" http_ok "http://$HOST:$(middleware_port 2)/health"

    if [ "$EXPECT_GOSSIP" = 1 ]; then
        check "middleware-3 learns both workers via gossip" wait_for_count "$(middleware_port 3)" 2 15
    else
        echo "  INFO  middleware-3 knows $(worker_count_on "$(middleware_port 3)") workers" \
             "(should become 2 once gossip exists; rerun with EXPECT_GOSSIP=1)"
    fi

    cleanup
}

scenario_crash() {
    echo "=== Scenario: middleware-1 crashes"
    start_cluster 2 4

    stop_process middleware-1 KILL > /dev/null   # simulate a crash
    sleep 1

    check "middleware-1 is down" not port_in_use "$(middleware_port 1)"
    check "middleware-2 still serves requests" not_empty "$(infer_worker_id "$(middleware_port 2)")"
    check "all 4 workers fail over to middleware-2" wait_for_count "$(middleware_port 2)" 4 20

    cleanup
}

case "${1:-all}" in
    independent) scenario_independent ;;
    join)        scenario_join ;;
    crash)       scenario_crash ;;
    all)         scenario_independent; scenario_join; scenario_crash ;;
    *)           echo "usage: $0 [independent|join|crash|all]"; exit 2 ;;
esac

echo
if [ "$FAILURES" -eq 0 ]; then
    echo "ALL CHECKS PASSED"
else
    echo "$FAILURES CHECK(S) FAILED - look in logs/ for details"
    exit 1
fi
