from collections import defaultdict


round_robin_index = defaultdict(int)


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

    raise ValueError(
        f"unsupported routing policy: {policy}"
    )