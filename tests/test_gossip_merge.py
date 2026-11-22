
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import middleware


class TestGossipMerge(unittest.TestCase):

    def setUp(self):
        self.original_node_id = middleware.NODE_ID
        middleware.NODE_ID = "middleware-1"

        with middleware.registry_lock:
            self.original_workers = middleware.worker_registry.copy()
            middleware.worker_registry.clear()

        with middleware.received_gossip_lock:
            self.original_gossip = middleware.received_gossip
            middleware.received_gossip = {}

        self.client = middleware.app.test_client()

    def tearDown(self):
        with middleware.registry_lock:
            middleware.worker_registry.clear()
            middleware.worker_registry.update(self.original_workers)

        with middleware.received_gossip_lock:
            middleware.received_gossip = self.original_gossip

        middleware.NODE_ID = self.original_node_id

    def worker(
        self, worker_id="worker-2",
        owner="middleware-2",
        port=9102,
        load=0
    ):
        return {
            "worker_id": worker_id,
            "host": "127.0.0.1",
            "port": port,
            "models": ["llama"],
            "status": "healthy",
            "owner": owner,
            "active_requests": load,
            "capacity": 4
        }

    def send_gossip(self, sender_id, workers):
        response = self.client.post("/gossip", json={
            "sender_id": sender_id,
            "workers": workers
        })
        self.assertEqual(response.status_code, 200)
        return response

    def register_local(self, worker_id, port=9101):
        response = self.client.post("/register", json={
            "worker_id": worker_id,
            "host": "127.0.0.1",
            "port": port,
            "models": ["llama"],
            "capacity": 4
        })
        self.assertEqual(response.status_code, 201)

    def consolidated(self):
        response = self.client.get("/gossip/workers")
        self.assertEqual(response.status_code, 200)
        return response.get_json()

    def test_merge_local_and_remote_workers(self):
        self.register_local("worker-1")
        self.send_gossip(
            "middleware-2",
            [self.worker()]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 2)
        self.assertEqual(result["local_count"], 1)
        self.assertEqual(result["remote_count"], 1)
        self.assertEqual(result["conflict_count"], 0)

        workers = {
            w["worker_id"]: w for w in result["workers"]
        }

        self.assertEqual(workers["worker-1"]["source"], "local")
        self.assertEqual(workers["worker-2"]["source"], "gossip")
        self.assertEqual(
            workers["worker-2"]["owner"], "middleware-2"
        )

        # The original routing registry is unchanged.
        self.assertEqual(
            self.client.get("/workers").get_json()["count"], 1
        )

    def test_duplicate_delivery_updates_existing_worker(self):
        self.send_gossip(
            "middleware-2",
            [self.worker(load=0)]
        )

        self.send_gossip(
            "middleware-2",
            [self.worker(load=3)]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["remote_count"], 1)
        self.assertEqual(
            result["workers"][0]["active_requests"], 3
        )

    def test_empty_snapshot_removes_remote_worker(self):
        self.send_gossip(
            "middleware-2",
            [self.worker()]
        )

        self.assertEqual(self.consolidated()["count"], 1)

        self.send_gossip("middleware-2", [])

        self.assertEqual(self.consolidated()["count"], 0)

    def test_local_worker_takes_priority(self):
        self.register_local("worker-2", port=9101)

        self.send_gossip(
            "middleware-2",
            [self.worker(port=9102)]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["local_count"], 1)
        self.assertEqual(result["remote_count"], 0)

        selected = result["workers"][0]

        self.assertEqual(selected["source"], "local")
        self.assertEqual(selected["owner"], "middleware-1")
        self.assertEqual(selected["port"], 9101)

    def test_conflicting_remote_owners_are_excluded(self):
        self.send_gossip(
            "middleware-2",
            [self.worker(
                worker_id="worker-shared",
                owner="middleware-2"
            )]
        )

        self.send_gossip(
            "middleware-3",
            [self.worker(
                worker_id="worker-shared",
                owner="middleware-3",
                port=9103
            )]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 0)
        self.assertEqual(result["conflict_count"], 1)
        self.assertEqual(
            result["conflicts"], ["worker-shared"]
        )

    def test_forwarded_owner_claim_is_ignored(self):
        # The current protocol only consolidates direct-owner
        # snapshots, not forwarded worker information.
        self.send_gossip(
            "middleware-2",
            [self.worker(owner="middleware-3")]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 0)

        # Raw gossip remains available for inspection.
        raw = self.client.get("/gossip/received").get_json()
        self.assertEqual(raw["source_count"], 1)

    def test_expired_snapshot_is_excluded(self):
        self.send_gossip(
            "middleware-2",
            [self.worker()]
        )

        with middleware.received_gossip_lock:
            middleware.received_gossip[
                "middleware-2"
            ]["received_at"] = (
                time.time() - middleware.GOSSIP_SNAPSHOT_TTL - 1
            )

        result = self.consolidated()

        self.assertEqual(result["count"], 0)

    def test_new_message_refreshes_expired_snapshot(self):
        self.send_gossip(
            "middleware-2",
            [self.worker()]
        )

        with middleware.received_gossip_lock:
            middleware.received_gossip[
                "middleware-2"
            ]["received_at"] = (
                time.time() - middleware.GOSSIP_SNAPSHOT_TTL - 1
            )

        self.assertEqual(self.consolidated()["count"], 0)

        self.send_gossip(
            "middleware-2",
            [self.worker(load=2)]
        )

        result = self.consolidated()

        self.assertEqual(result["count"], 1)
        self.assertEqual(
            result["workers"][0]["active_requests"], 2
        )

    def test_malformed_update_preserves_merged_state(self):
        self.send_gossip(
            "middleware-2",
            [self.worker()]
        )

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-2",
            "workers": "invalid"
        })

        self.assertEqual(response.status_code, 400)

        result = self.consolidated()

        self.assertEqual(result["count"], 1)
        self.assertEqual(
            result["workers"][0]["worker_id"], "worker-2"
        )


if __name__ == "__main__":
    unittest.main()
