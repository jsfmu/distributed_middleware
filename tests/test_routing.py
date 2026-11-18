
import sys
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import middleware
import routing


class TestRouting(unittest.TestCase):
    def setUp(self):
        routing.round_robin_index.clear()

    def worker(self, worker_id, load=0, capacity=None):
        return {
            "worker_id": worker_id,
            "active_requests": load,
            "capacity": capacity
        }

    def test_round_robin_rotation(self):
        workers = [
            self.worker("worker-2"),
            self.worker("worker-1")
        ]

        selected = [
            routing.select_worker(
                "round_robin", "model-a", workers
            )["worker_id"]
            for _ in range(4)
        ]

        self.assertEqual(
            selected,
            ["worker-1", "worker-2", "worker-1", "worker-2"]
        )

    def test_round_robin_concurrent(self):
        self.assertTrue(
            hasattr(routing, "round_robin_lock")
        )

        workers = [
            self.worker("worker-1"),
            self.worker("worker-2")
        ]

        with ThreadPoolExecutor(max_workers=20) as executor:
            selected = list(
                executor.map(
                    lambda _: routing.select_worker(
                        "round_robin", "model-a", workers
                    )["worker_id"],
                    range(1000)
                )
            )

        self.assertEqual(
            Counter(selected),
            {"worker-1": 500, "worker-2": 500}
        )

    def test_least_loaded(self):
        workers = [
            self.worker("worker-1", 3),
            self.worker("worker-2", 1)
        ]

        selected = routing.select_worker(
            "least_loaded", "model-a", workers
        )

        self.assertEqual(
            selected["worker_id"],
            "worker-2"
        )

    def test_capacity_aware_prefers_lower_utilization(self):
        workers = [
            self.worker("worker-1", 2, 8),
            self.worker("worker-2", 1, 2)
        ]

        selected = routing.select_worker(
            "capacity_aware", "model-a", workers
        )

        self.assertEqual(
            selected["worker_id"],
            "worker-1"
        )

    def test_capacity_aware_ignores_unknown_capacity(self):
        workers = [
            self.worker("worker-1", 0, None),
            self.worker("worker-2", 1, 4)
        ]

        selected = routing.select_worker(
            "capacity_aware", "model-a", workers
        )

        self.assertEqual(
            selected["worker_id"],
            "worker-2"
        )

    def test_capacity_aware_falls_back_to_round_robin(self):
        workers = [
            self.worker("worker-1"),
            self.worker("worker-2")
        ]

        selected = [
            routing.select_worker(
                "capacity_aware", "model-a", workers
            )["worker_id"]
            for _ in range(6)
        ]

        self.assertEqual(
            Counter(selected),
            {"worker-1": 3, "worker-2": 3}
        )

    def test_power_of_two_uses_sampled_candidates_only(self):
        workers = [
            self.worker("worker-1", 5),
            self.worker("worker-2", 1),
            self.worker("worker-3", 0)
        ]

        with patch.object(
            routing.random,
            "sample",
            return_value=[workers[0], workers[1]]
        ) as sample:
            selected = routing.select_worker(
                "power_of_two", "model-a", workers
            )

        sample.assert_called_once_with(workers, 2)

        self.assertEqual(
            selected["worker_id"],
            "worker-2"
        )

    def test_power_of_two_handles_equal_load(self):
        workers = [
            self.worker("worker-1", 1),
            self.worker("worker-2", 1)
        ]

        with patch.object(
            routing.random,
            "sample",
            return_value=workers
        ):
            with patch.object(
                routing.random,
                "choice",
                return_value=workers[1]
            ) as choice:
                selected = routing.select_worker(
                    "power_of_two", "model-a", workers
                )

        choice.assert_called_once()

        self.assertEqual(
            selected["worker_id"],
            "worker-2"
        )

    def test_adaptive_decisions(self):
        cases = [
            (
                "single",
                [self.worker("w1")],
                "round_robin",
                "single_worker"
            ),
            (
                "missing",
                [
                    self.worker("w1", None),
                    self.worker("w2", 1)
                ],
                "round_robin",
                "missing_load_information"
            ),
            (
                "idle",
                [
                    self.worker("w1", 0, 8),
                    self.worker("w2", 0, 2)
                ],
                "round_robin",
                "all_workers_idle"
            ),
            (
                "heterogeneous",
                [
                    self.worker("w1", 2, 8),
                    self.worker("w2", 1, 2)
                ],
                "capacity_aware",
                "different_worker_capacities"
            ),
            (
                "imbalance",
                [
                    self.worker("w1", 3, 2),
                    self.worker("w2", 0, 2)
                ],
                "least_loaded",
                "load_imbalance"
            ),
            (
                "large",
                [
                    self.worker(f"w{i}", 1, 2)
                    for i in range(4)
                ],
                "power_of_two",
                "large_worker_pool"
            ),
            (
                "balanced",
                [
                    self.worker("w1", 1, 2),
                    self.worker("w2", 1, 2)
                ],
                "round_robin",
                "similar_worker_loads"
            )
        ]

        for name, workers, expected_policy, expected_reason in cases:
            with self.subTest(name=name):
                selected = routing.choose_routing_policy(
                    workers
                )

                self.assertEqual(
                    selected,
                    (expected_policy, expected_reason)
                )

    def test_empty_worker_pool(self):
        policies = [
            "round_robin",
            "least_loaded",
            "capacity_aware",
            "power_of_two"
        ]

        for policy in policies:
            with self.subTest(policy=policy):
                selected = routing.select_worker(
                    policy, "model-a", []
                )

                self.assertIsNone(selected)

    def test_single_worker_pool(self):
        worker = self.worker("worker-1", 0, 2)

        policies = [
            "round_robin",
            "least_loaded",
            "capacity_aware",
            "power_of_two"
        ]

        for policy in policies:
            with self.subTest(policy=policy):
                selected = routing.select_worker(
                    policy, "model-a", [worker]
                )

                self.assertEqual(selected, worker)

    def test_unsupported_policy(self):
        with self.assertRaises(ValueError):
            routing.select_worker(
                "unsupported",
                "model-a",
                [self.worker("worker-1")]
            )


class TestMiddlewareRouting(unittest.TestCase):
    def setUp(self):
        self.original_policy = middleware.ROUTING_POLICY

        with middleware.registry_lock:
            self.original_registry = (
                middleware.worker_registry.copy()
            )
            middleware.worker_registry.clear()

        self.client = middleware.app.test_client()

        workers = [
            ("worker-1", 9001, 8),
            ("worker-2", 9002, 2)
        ]

        for worker_id, port, capacity in workers:
            response = self.client.post(
                "/register",
                json={
                    "worker_id": worker_id,
                    "host": "127.0.0.1",
                    "port": port,
                    "models": ["model-a"],
                    "capacity": capacity
                }
            )

            self.assertEqual(
                response.status_code, 201
            )

    def tearDown(self):
        middleware.ROUTING_POLICY = self.original_policy

        with middleware.registry_lock:
            middleware.worker_registry.clear()
            middleware.worker_registry.update(
                self.original_registry
            )

    def test_heartbeat_updates_registry(self):
        response = self.client.post(
            "/heartbeat",
            json={
                "worker_id": "worker-1",
                "active_requests": 2,
                "capacity": 8
            }
        )

        self.assertEqual(
            response.status_code, 200
        )

        with middleware.registry_lock:
            worker = middleware.worker_registry["worker-1"]

            self.assertEqual(
                worker["active_requests"], 2
            )

            self.assertEqual(
                worker["capacity"], 8
            )

    def test_unhealthy_worker_excluded_under_all_policies(self):
        with middleware.registry_lock:
            middleware.worker_registry["worker-1"][
                "status"
            ] = "unavailable"

        policies = [
            "round_robin",
            "least_loaded",
            "capacity_aware",
            "power_of_two",
            "adaptive"
        ]

        for policy in policies:
            with self.subTest(policy=policy):
                middleware.ROUTING_POLICY = policy

                fake_response = Mock(status_code=200)

                fake_response.json.return_value = {
                    "worker_id": "worker-2",
                    "model": "model-a",
                    "result": "ok"
                }

                with patch.object(
                    middleware.requests,
                    "post",
                    return_value=fake_response
                ) as forward:
                    response = self.client.post(
                        "/infer",
                        json={
                            "model": "model-a",
                            "input": "test"
                        }
                    )

                self.assertEqual(
                    response.status_code, 200
                )

                self.assertEqual(
                    response.get_json()["worker_id"],
                    "worker-2"
                )

                expected_policy = (
                    "round_robin"
                    if policy == "adaptive"
                    else policy
                )

                self.assertEqual(
                    response.headers["X-Routing-Policy"],
                    expected_policy
                )

                self.assertTrue(
                    forward.call_args.args[0].endswith(
                        ":9002/infer"
                    )
                )

    def test_all_workers_unavailable_returns_404(self):
        with middleware.registry_lock:
            for worker in middleware.worker_registry.values():
                worker["status"] = "unavailable"

        response = self.client.post(
            "/infer",
            json={
                "model": "model-a",
                "input": "test"
            }
        )

        self.assertEqual(
            response.status_code, 404
        )

    def test_missing_model_rejected(self):
        response = self.client.post(
            "/infer",
            json={
                "input": "test"
            }
        )

        self.assertEqual(
            response.status_code, 400
        )


if __name__ == "__main__":
    unittest.main()
