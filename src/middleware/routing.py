
import random
from collections import defaultdict


round_robin_index = defaultdict(int)


# Round-robin scheduling
def select_worker_round_robin(model, workers):
    if not workers:
        return None

    workers = sorted(
        workers,
        key=lambda worker: worker["worker_id"]
    )

    index = round_robin_index[model] % len(workers)

    selected_worker = workers[index]

    round_robin_index[model] = (
        round_robin_index[model] + 1
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


# Capacity-aware scheduling
def select_worker_capacity_aware(workers):
    if not workers:
        return None

    def utilization(worker):
        active_requests = worker.get("active_requests", 0)
        capacity = worker.get("capacity")

        if capacity is None or capacity <= 0:
            return float("inf")

        return active_requests / capacity

    return min(
        workers,
        key=lambda worker: (
            utilization(worker),
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
        return select_worker_capacity_aware(
            workers
        )

    if policy == "power_of_two":
        return select_worker_power_of_two(
            workers
        )

    raise ValueError(
        f"unsupported routing policy: {policy}"
    )
