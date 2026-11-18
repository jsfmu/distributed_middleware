
import argparse
import csv
import math
import statistics
import time

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests


def percentile(values, percent):
    sorted_values = sorted(values)
    index = math.ceil(percent / 100 * len(sorted_values)) - 1
    return sorted_values[max(0, index)]


def format_counts(counts):
    return ", ".join(
        f"{name}={count}"
        for name, count in sorted(counts.items())
    )


def send_request(request_id, url, model, timeout):
    start = time.perf_counter()

    try:
        response = requests.post(
            f"{url}/infer",
            json={
                "model": model,
                "input": f"benchmark request {request_id}"
            },
            timeout=timeout
        )

        latency_ms = (time.perf_counter() - start) * 1000

        try:
            data = response.json()
        except ValueError:
            data = {}

        if not isinstance(data, dict):
            data = {}

        worker_id = data.get("worker_id", "")
        success = response.ok and bool(worker_id)

        return {
            "request_id": request_id,
            "success": int(success),
            "status": response.status_code,
            "worker_id": worker_id,
            "latency_ms": round(latency_ms, 2),
            "selected_policy": response.headers.get(
                "X-Routing-Policy", ""
            ),
            "policy_reason": response.headers.get(
                "X-Routing-Reason", ""
            ),
            "error": "" if success else str(
                data.get("error", "request failed")
            )
        }

    except requests.RequestException as exc:
        latency_ms = (time.perf_counter() - start) * 1000

        return {
            "request_id": request_id,
            "success": 0,
            "status": 0,
            "worker_id": "",
            "latency_ms": round(latency_ms, 2),
            "selected_policy": "",
            "policy_reason": "",
            "error": str(exc)
        }


def save_csv(path, rows, append=False):
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)

    write_header = (
        not append
        or not path.exists()
        or path.stat().st_size == 0
    )

    with path.open(
        "a" if append else "w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=list(rows[0].keys())
        )

        if write_header:
            writer.writeheader()

        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Compare middleware routing policies"
    )

    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000"
    )

    parser.add_argument(
        "--model",
        default="model-a"
    )

    parser.add_argument(
        "--policy",
        required=True,
        choices=[
            "round_robin",
            "least_loaded",
            "capacity_aware",
            "power_of_two",
            "adaptive"
        ]
    )

    parser.add_argument(
        "--scenario",
        default="heterogeneous"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=60
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=6
    )

    parser.add_argument(
        "--trial",
        type=int,
        default=1
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=8
    )

    parser.add_argument(
        "--output-dir",
        default="results/routing"
    )

    args = parser.parse_args()
    url = args.url.rstrip("/")

    if args.count <= 0 or args.concurrency <= 0:
        parser.error("count and concurrency must be positive")

    # Verify middleware is using the intended policy
    try:
        response = requests.get(
            f"{url}/health",
            timeout=3
        )
        response.raise_for_status()
        health = response.json()
    except (requests.RequestException, ValueError) as exc:
        parser.error(f"middleware health check failed: {exc}")

    actual_policy = health.get("routing_policy")

    if actual_policy != args.policy:
        parser.error(
            f"expected policy={args.policy}, "
            f"middleware reports policy={actual_policy}"
        )

    print(
        f"[BENCHMARK] policy={args.policy} "
        f"scenario={args.scenario} "
        f"trial={args.trial}"
    )

    print(
        f"[WORKLOAD] requests={args.count} "
        f"concurrency={args.concurrency}"
    )

    # Execute concurrent requests
    start = time.perf_counter()

    with ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        futures = [
            executor.submit(
                send_request,
                i,
                url,
                args.model,
                args.timeout
            )
            for i in range(args.count)
        ]

        results = [
            future.result()
            for future in as_completed(futures)
        ]

    duration = time.perf_counter() - start

    results.sort(
        key=lambda result: result["request_id"]
    )

    successful = [
        result
        for result in results
        if result["success"]
    ]

    failed = len(results) - len(successful)

    latencies = [
        result["latency_ms"]
        for result in successful
    ]

    worker_counts = Counter(
        result["worker_id"]
        for result in successful
    )

    policy_counts = Counter(
        result["selected_policy"]
        for result in successful
        if result["selected_policy"]
    )

    mean_ms = (
        round(statistics.mean(latencies), 2)
        if latencies else None
    )

    p95_ms = (
        round(percentile(latencies, 95), 2)
        if latencies else None
    )

    throughput = (
        len(successful) / duration
        if duration > 0 else 0
    )

    summary = {
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(timespec="seconds"),
        "scenario": args.scenario,
        "policy": args.policy,
        "trial": args.trial,
        "requests": args.count,
        "concurrency": args.concurrency,
        "successful": len(successful),
        "failed": failed,
        "duration_s": round(duration, 2),
        "throughput_rps": round(throughput, 2),
        "mean_latency_ms": mean_ms,
        "p95_latency_ms": p95_ms,
        "worker_distribution": format_counts(worker_counts),
        "selected_policies": format_counts(policy_counts)
    }

    output_dir = Path(args.output_dir)

    raw_path = output_dir / (
        f"{args.scenario}_{args.policy}_"
        f"trial{args.trial}.csv"
    )

    summary_path = output_dir / "summary.csv"

    save_csv(raw_path, results)
    save_csv(summary_path, [summary], append=True)

    print()
    print("[RESULTS]")
    print(f"Successful: {len(successful)}/{args.count}")
    print(f"Failed: {failed}")
    print(f"Duration: {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} req/s")
    print(f"Mean latency: {mean_ms} ms")
    print(f"P95 latency: {p95_ms} ms")
    print(f"Workers: {format_counts(worker_counts)}")
    print(f"Selected policies: {format_counts(policy_counts)}")

    print()
    print(f"[SAVED] {raw_path}")
    print(f"[SAVED] {summary_path}")


if __name__ == "__main__":
    main()
