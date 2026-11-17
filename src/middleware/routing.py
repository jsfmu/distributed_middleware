
import math
import random
import threading
from collections import defaultdict


round_robin_index = defaultdict(int)
round_robin_lock = threading.Lock()


# Round-robin scheduling
def select_worker_round_robin(model, workers):
    if not workers:
        return None

    workers = sorted(
        workers,
        key=lambda worker: worker["worker_id"]
    )

    with round_robin_lock:
        index = round_robin_index[model] % len(workers)

        selected_worker = workers[index]

        round_robin_index[model] = (
            index + 1
        ) % len(workers)

    return selected_worker


# Least-loaded scheduling
def select_worker_least_loaded(workers):
    if not workers:
        return None

    workers = sorted(
        workers,
        key=lambda worker: (
            worker.get("active_requests", 0),
            worker["worker_id"]
        )
    )

    return workers[0]


# Check whether a worker advertises valid capacity
def has_valid_capacity(worker):
    capacity = worker.get("capacity")

    return (
        type(capacity) in (int, float)
        and math.isfinite(capacity)
        and capacity > 0
    )


# Capacity-aware scheduling
def select_worker_capacity_aware(workers):
    if not workers:
        return None

    valid_workers = [
        worker
        for worker in workers
        if has_valid_capacity(worker)
    ]

    if not valid_workers:
        return None

    return min(
        valid_workers,
        key=lambda worker: (
            worker.get("active_requests", 0) / worker["capacity"],
            worker["worker_id"]
        )
    )


# Power of Two Choices scheduling
def select_worker_power_of_two(workers):
    if not workers:
        return None

    if len(workers) == 1:
        return workers[0]

    first, second = random.sample(workers, 2)

    first_load = first.get("active_requests", 0)
    second_load = second.get("active_requests", 0)

    if first_load < second_load:
        return first

    if second_load < first_load:
        return second

    return random.choice([first, second])


# Adaptive routing policy selection
def choose_routing_policy(workers):
    if len(workers) <= 1:
        return "round_robin", "single_worker"

    loads = [
        worker.get("active_requests")
        for worker in workers
    ]

    # Fall back to round robin if load information is invalid
    if any(
        not isinstance(load, (int, float)) or load < 0
        for load in loads
    ):
        return "round_robin", "missing_load_information"

    # No active requests: use simple round-robin distribution
    if max(loads) == 0:
        return "round_robin", "all_workers_idle"

    capacities = [
        worker.get("capacity")
        for worker in workers
    ]

    valid_capacities = all(
        has_valid_capacity(worker)
        for worker in workers
    )

    # Prefer relative utilization for heterogeneous workers
    if valid_capacities and len(set(capacities)) > 1:
        return "capacity_aware", "different_worker_capacities"

    # React to significant load imbalance
    if max(loads) - min(loads) >= 2:
        return "least_loaded", "load_imbalance"

    # Sample two candidates in a larger worker pool
    if len(workers) >= 4:
        return "power_of_two", "large_worker_pool"

    return "round_robin", "similar_worker_loads"


# Function to select a worker based on the routing policy
def select_worker(policy, model, workers):
    if policy == "round_robin":
        return select_worker_round_robin(
            model,
            workers
        )

    if policy == "least_loaded":
        return select_worker_least_loaded(
            workers
        )

    if policy == "capacity_aware":
        selected_worker = select_worker_capacity_aware(
            workers
        )

        if selected_worker is not None:
            return selected_worker

        # No valid capacities: use round robin
        return select_worker_round_robin(
            model,
            workers
        )

    if policy == "power_of_two":
        return select_worker_power_of_two(
            workers
        )

    raise ValueError(
        f"unsupported routing policy: {policy}"
    )
