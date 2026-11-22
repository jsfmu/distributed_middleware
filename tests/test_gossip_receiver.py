
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import gossip
import middleware


class TestGossipReceiver(unittest.TestCase):

    def setUp(self):
        self.original_node_id = middleware.NODE_ID
        self.original_gossip = middleware.received_gossip

        middleware.NODE_ID = "middleware-1"
        middleware.received_gossip = {}

        self.client = middleware.app.test_client()

        self.worker = {
            "worker_id": "worker-2",
            "host": "127.0.0.1",
            "port": 9102,
            "models": ["llama"],
            "status": "healthy",
            "owner": "middleware-2",
            "active_requests": 0,
            "capacity": 4
        }

    def tearDown(self):
        middleware.NODE_ID = self.original_node_id
        middleware.received_gossip = self.original_gossip

    def payload(self, workers=None):
        return {
            "sender_id": "middleware-2",
            "workers": [self.worker.copy()]
            if workers is None else workers
        }

    def test_inspect_empty_gossip_registry(self):
        response = self.client.get("/gossip/received")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["source_count"], 0)
        self.assertEqual(response.get_json()["sources"], [])

    def test_receive_valid_gossip(self):
        response = self.client.post(
            "/gossip", json=self.payload()
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["worker_count"], 1)

        result = self.client.get("/gossip/received").get_json()

        self.assertEqual(result["source_count"], 1)
        self.assertEqual(result["sources"][0]["worker_count"], 1)
        self.assertEqual(
            result["sources"][0]["workers"][0]["worker_id"],
            "worker-2"
        )
        self.assertIsNotNone(
            result["sources"][0]["received_at"]
        )

    def test_duplicate_delivery_does_not_duplicate_records(self):
        message = self.payload()

        self.client.post("/gossip", json=message)
        self.client.post("/gossip", json=message)

        result = self.client.get("/gossip/received").get_json()

        self.assertEqual(result["source_count"], 1)
        self.assertEqual(result["sources"][0]["worker_count"], 1)

    def test_new_snapshot_replaces_old_snapshot(self):
        self.client.post("/gossip", json=self.payload())

        updated_worker = {
            **self.worker,
            "active_requests": 3
        }

        self.client.post(
            "/gossip",
            json=self.payload([updated_worker])
        )

        result = self.client.get("/gossip/received").get_json()

        workers = result["sources"][0]["workers"]

        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["active_requests"], 3)

        # An empty snapshot clears the sender's previous workers.
        self.client.post(
            "/gossip", json=self.payload([])
        )

        result = self.client.get("/gossip/received").get_json()
        self.assertEqual(result["sources"][0]["worker_count"], 0)

    def test_reject_invalid_message_structure(self):
        invalid_messages = [
            [],
            {},
            {"sender_id": "", "workers": []},
            {"sender_id": "middleware-1", "workers": []},
            {"sender_id": "middleware-2"},
            {"sender_id": "middleware-2", "workers": "invalid"}
        ]

        for message in invalid_messages:
            with self.subTest(message=message):
                response = self.client.post(
                    "/gossip", json=message
                )
                self.assertEqual(response.status_code, 400)

    def test_reject_invalid_worker_fields(self):
        invalid_fields = [
            ("worker_id", ""),
            ("host", ""),
            ("port", -1),
            ("port", 70000),
            ("port", "9102"),
            ("models", "llama"),
            ("models", [""]),
            ("status", "invalid"),
            ("owner", ""),
            ("active_requests", -1),
            ("capacity", 0)
        ]

        for field, value in invalid_fields:
            with self.subTest(field=field, value=value):
                worker = {
                    **self.worker,
                    field: value
                }

                response = self.client.post(
                    "/gossip",
                    json=self.payload([worker])
                )

                self.assertEqual(response.status_code, 400)

    def test_reject_duplicate_worker_ids(self):
        response = self.client.post(
            "/gossip",
            json=self.payload([
                self.worker.copy(),
                self.worker.copy()
            ])
        )

        self.assertEqual(response.status_code, 400)

    def test_invalid_update_preserves_previous_snapshot(self):
        self.client.post(
            "/gossip", json=self.payload()
        )

        before = self.client.get(
            "/gossip/received"
        ).get_json()

        invalid_worker = {
            **self.worker,
            "port": -1
        }

        response = self.client.post(
            "/gossip",
            json=self.payload([
                self.worker.copy(),
                invalid_worker
            ])
        )

        self.assertEqual(response.status_code, 400)

        after = self.client.get(
            "/gossip/received"
        ).get_json()

        self.assertEqual(before, after)

    def test_received_gossip_does_not_modify_local_workers(self):
        with middleware.registry_lock:
            original_workers = middleware.worker_registry.copy()

        response = self.client.post(
            "/gossip", json=self.payload()
        )

        self.assertEqual(response.status_code, 200)

        with middleware.registry_lock:
            self.assertEqual(
                middleware.worker_registry,
                original_workers
            )

    def test_compatible_with_gossip_sender(self):
        message = gossip.build_gossip_message(
            "middleware-2",
            {"workers": [self.worker.copy()]}
        )

        response = self.client.post(
            "/gossip", json=message
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["sender_id"],
            "middleware-2"
        )


if __name__ == "__main__":
    unittest.main()
