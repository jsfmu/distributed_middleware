
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

from distributed_routing import (
    WorkerQuarantine,
    build_routing_candidates,
    safe_to_retry
)


def worker(
    worker_id,
    owner=None,
    model="llama",
    status="healthy",
    port=9102
):
    record = {
        "worker_id": worker_id,
        "host": "127.0.0.1",
        "port": port,
        "models": [model],
        "status": status,
        "active_requests": 0,
        "capacity": 4
    }

    if owner is not None:
        record["owner"] = owner

    return record


def candidates(
    local,
    remote,
    fresh=None,
    include_remote=True
):
    return build_routing_candidates(
        local_workers=local,
        replica_snapshot={
            "remote_workers": remote,
            "fresh_remote_workers": (
                remote if fresh is None else fresh
            )
        },
        node_id="middleware-1",
        model="llama",
        include_remote=include_remote,
        trusted_owners={
            "middleware-2",
            "middleware-3"
        },
        allowed_remote_hosts={"127.0.0.1"},
        remote_port_range=(9100, 9199)
    )


class TestDistributedCandidates(unittest.TestCase):

    def test_local_and_remote_pool(self):
        result = candidates(
            [worker("local")],
            [worker("remote", "middleware-2")]
        )

        self.assertEqual(
            [r["worker_id"] for r in result],
            ["local", "remote"]
        )

        self.assertEqual(
            [r["route_scope"] for r in result],
            ["local", "remote"]
        )

    def test_remote_disabled(self):
        result = candidates(
            [],
            [worker("remote", "middleware-2")],
            include_remote=False
        )

        self.assertEqual(result, [])

    def test_stale_or_unhealthy_remote_excluded(self):
        remote = [
            worker("stale", "middleware-2"),
            worker(
                "down",
                "middleware-2",
                status="unavailable"
            )
        ]

        result = candidates(
            [],
            remote,
            fresh=[remote[1]]
        )

        self.assertEqual(result, [])

    def test_local_claim_wins(self):
        result = candidates(
            [worker("same")],
            [worker("same", "middleware-2")]
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["owner"], "middleware-1")

    def test_conflicting_remote_owners_excluded(self):
        result = candidates(
            [],
            [
                worker("shared", "middleware-2"),
                worker("shared", "middleware-3")
            ]
        )

        self.assertEqual(result, [])

    def test_remote_owner_host_and_port_allowlist(self):
        remote = [
            worker("good", "middleware-2"),
            worker("foreign", "middleware-9"),
            worker(
                "wrong-port",
                "middleware-2",
                port=8000
            ),
            {
                **worker("wrong-host", "middleware-2"),
                "host": "192.0.2.7"
            }
        ]

        result = candidates([], remote)

        self.assertEqual(
            [w["worker_id"] for w in result],
            ["good"]
        )

    def test_model_and_tombstone_excluded(self):
        remote = [
            worker(
                "wrong-model",
                "middleware-2",
                model="other"
            ),
            {
                **worker("deleted", "middleware-2"),
                "deleted": True
            }
        ]

        self.assertEqual(
            candidates([], remote),
            []
        )

    def test_quarantine_and_safe_retry(self):
        quarantine = WorkerQuarantine(
            cooldown_seconds=10
        )

        w = {
            **worker("remote", "middleware-2"),
            "route_scope": "remote"
        }

        self.assertFalse(
            quarantine.is_quarantined(w)
        )

        quarantine.mark_failed(w)

        self.assertTrue(
            quarantine.is_quarantined(w)
        )

        quarantine.clear()

        self.assertFalse(
            quarantine.is_quarantined(w)
        )

        self.assertTrue(
            safe_to_retry(
                requests.exceptions.ConnectTimeout(
                    "no connect"
                )
            )
        )

        self.assertFalse(
            safe_to_retry(
                requests.exceptions.ReadTimeout(
                    "response lost"
                )
            )
        )

        self.assertFalse(
            safe_to_retry(
                requests.exceptions.ConnectionError(
                    "reset"
                )
            )
        )


class TestDistributedInferEndpoints(unittest.TestCase):

    def setUp(self):
        import middleware
        from peers import PeerRegistry
        from registry import VersionedWorkerRegistry

        self.middleware = middleware

        self.saved = (
            middleware.NODE_ID,
            middleware.replicated_registry,
            middleware.peer_registry,
            middleware.ROUTING_POLICY,
            middleware.ENABLE_DISTRIBUTED_ROUTING,
            middleware.routing_quarantine
        )

        with middleware.registry_lock:
            self.old_workers = dict(
                middleware.worker_registry
            )
            middleware.worker_registry.clear()

        middleware.NODE_ID = "middleware-1"
        middleware.replicated_registry = VersionedWorkerRegistry(
            "middleware-1"
        )

        middleware.peer_registry = PeerRegistry()
        middleware.peer_registry.add_or_update_peer(
            node_id="middleware-2",
            host="127.0.0.1",
            port=8001
        )

        middleware.ROUTING_POLICY = "least_loaded"
        middleware.ENABLE_DISTRIBUTED_ROUTING = True
        middleware.routing_quarantine = WorkerQuarantine()

        self.client = middleware.app.test_client()

    def tearDown(self):
        m = self.middleware

        with m.registry_lock:
            m.worker_registry.clear()
            m.worker_registry.update(self.old_workers)

        (
            m.NODE_ID,
            m.replicated_registry,
            m.peer_registry,
            m.ROUTING_POLICY,
            m.ENABLE_DISTRIBUTED_ROUTING,
            m.routing_quarantine
        ) = self.saved

    def add_remote(self, worker_id="a-remote"):
        record = {
            **worker(worker_id, "middleware-2"),
            "version": 1,
            "deleted": False
        }

        self.assertEqual(
            self.middleware.replicated_registry.apply_remote(
                record
            ),
            "added"
        )

    def add_local(self, worker_id="z-local"):
        response = self.client.post("/register", json={
            "worker_id": worker_id,
            "host": "127.0.0.1",
            "port": 9101,
            "models": ["llama"],
            "capacity": 4
        })

        self.assertEqual(response.status_code, 201)

    def test_remote_only_inference(self):
        self.add_remote()

        mock_response = Mock(status_code=200)
        mock_response.json.return_value = {
            "worker_id": "a-remote",
            "result": "ok"
        }

        with patch.object(
            self.middleware.requests,
            "post",
            return_value=mock_response
        ) as request_post:

            response = self.client.post("/infer", json={
                "model": "llama",
                "input": "hello"
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["X-Worker-Scope"],
            "remote"
        )
        self.assertEqual(
            response.headers["X-Worker-Owner"],
            "middleware-2"
        )
        self.assertEqual(request_post.call_count, 1)

        self.assertTrue(
            request_post.call_args.args[0].endswith(
                ":9102/infer"
            )
        )

    def test_connection_failure_falls_back(self):
        self.add_remote()
        self.add_local()

        mock_response = Mock(status_code=200)
        mock_response.json.return_value = {
            "worker_id": "z-local",
            "result": "ok"
        }

        errors = [
            requests.exceptions.ConnectTimeout(
                "connect failed"
            ),
            mock_response
        ]

        with patch.object(
            self.middleware.requests,
            "post",
            side_effect=errors
        ) as post:

            response = self.client.post("/infer", json={
                "model": "llama",
                "input": "hello"
            })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(post.call_count, 2)
        self.assertEqual(
            response.headers["X-Routing-Attempts"],
            "2"
        )

        self.assertTrue(
            post.call_args.args[0].endswith(":9101/infer")
        )

    def test_read_timeout_does_not_retry(self):
        self.add_remote()
        self.add_local()

        with patch.object(
            self.middleware.requests,
            "post",
            side_effect=requests.exceptions.ReadTimeout(
                "response lost"
            )
        ) as post:

            response = self.client.post("/infer", json={
                "model": "llama",
                "input": "hello"
            })

        self.assertEqual(response.status_code, 502)
        self.assertEqual(post.call_count, 1)

    def test_http_error_does_not_retry(self):
        self.add_remote()
        self.add_local()

        mock_response = Mock(status_code=503)
        mock_response.json.return_value = {
            "error": "busy"
        }

        with patch.object(
            self.middleware.requests,
            "post",
            return_value=mock_response
        ) as post:

            response = self.client.post("/infer", json={
                "model": "llama",
                "input": "hello"
            })

        self.assertEqual(response.status_code, 503)
        self.assertEqual(post.call_count, 1)

    def test_local_only_mode_and_candidate_endpoint(self):
        self.add_remote()
        self.add_local()

        result = self.client.get(
            "/routing/candidates?model=llama"
        ).get_json()

        self.assertEqual(result["local_count"], 1)
        self.assertEqual(result["remote_count"], 1)

        self.middleware.ENABLE_DISTRIBUTED_ROUTING = False

        result = self.client.get(
            "/routing/candidates?model=llama"
        ).get_json()

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["remote_count"], 0)


if __name__ == "__main__":
    unittest.main()
