import threading
import time


def monitor_worker_health(
    worker_registry,
    registry_lock,
    heartbeat_timeout,
    check_interval=1
):
    while True:
        current_time = time.time()

        with registry_lock:
            for worker in worker_registry.values():
                last_heartbeat = worker.get("last_heartbeat")

                if last_heartbeat is None:
                    continue

                elapsed = current_time - last_heartbeat

                if elapsed > heartbeat_timeout:
                    if worker["status"] != "unavailable":
                        worker["status"] = "unavailable"

                        print(
                            f"[FAILURE] worker={worker['worker_id']} "
                            f"marked unavailable"
                        )

        time.sleep(check_interval)


def start_failure_detector(
    worker_registry,
    registry_lock,
    heartbeat_timeout,
    check_interval=1
):
    thread = threading.Thread(
        target=monitor_worker_health,
        args=(
            worker_registry,
            registry_lock,
            heartbeat_timeout,
            check_interval
        ),
        daemon=True
    )

    thread.start()

    return thread