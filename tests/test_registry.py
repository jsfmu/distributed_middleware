
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

from registry import ReplicatedWorkerRegistry


class TestReplicatedWorkerRegistry(unittest.TestCase):

    def setUp(self):
        self.registry = ReplicatedWorkerRegistry("middleware-1")

    def test_empty_registry(self):
        state = self.registry.snapshot()

        self.assertEqual(state["local_count"], 0)
        self.assertEqual(state["remote_count"], 0)
        self.assertEqual(state["local_workers"], [])
        self.assertEqual(state["remote_workers"], [])

    def test_local_worker_gets_local_owner(self):
        stored = self.registry.upsert_local({
            "worker_id": "worker-1",
            "models": ["llama"]
        })

        self.assertEqual(stored["owner"], "middleware-1")
        self.assertEqual(
            self.registry.get_local("worker-1")["models"],
            ["llama"]
        )

    def test_remote_worker_preserves_owner(self):
        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2"
        })

        worker = self.registry.get_remote(
            "worker-2", "middleware-2"
        )

        self.assertEqual(worker["owner"], "middleware-2")
        self.assertIsNone(self.registry.get_local("worker-2"))

    def test_local_and_remote_are_separate(self):
        self.registry.upsert_local({
            "worker_id": "worker-1"
        })

        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2"
        })

        state = self.registry.snapshot()

        self.assertEqual(state["local_count"], 1)
        self.assertEqual(state["remote_count"], 1)

    def test_upsert_does_not_duplicate_same_key(self):
        self.registry.upsert_local({
            "worker_id": "worker-1",
            "active_requests": 0
        })

        self.registry.upsert_local({
            "worker_id": "worker-1",
            "active_requests": 2
        })

        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2",
            "active_requests": 0
        })

        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2",
            "active_requests": 3
        })

        state = self.registry.snapshot()

        self.assertEqual(state["local_count"], 1)
        self.assertEqual(state["remote_count"], 1)

        self.assertEqual(
            self.registry.get_local("worker-1")["active_requests"],
            2
        )

        self.assertEqual(
            self.registry.get_remote(
                "worker-2", "middleware-2"
            )["active_requests"],
            3
        )

    def test_conflicting_remote_owners_remain_separate(self):
        self.registry.upsert_remote({
            "worker_id": "shared",
            "owner": "middleware-2"
        })

        self.registry.upsert_remote({
            "worker_id": "shared",
            "owner": "middleware-3"
        })

        claims = self.registry.get_remote_claims("shared")

        self.assertEqual(
            [record["owner"] for record in claims],
            ["middleware-2", "middleware-3"]
        )

        self.assertEqual(
            self.registry.snapshot()["remote_count"], 2
        )

    def test_input_is_copied_on_write(self):
        record = {
            "worker_id": "worker-1",
            "models": ["llama"],
            "hardware": {"gpu": "none"}
        }

        self.registry.upsert_local(record)

        record["models"].append("changed")
        record["hardware"]["gpu"] = "changed"

        stored = self.registry.get_local("worker-1")

        self.assertEqual(stored["models"], ["llama"])
        self.assertEqual(stored["hardware"]["gpu"], "none")

    def test_output_is_copied_on_read(self):
        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2",
            "models": ["llama"]
        })

        state = self.registry.snapshot()
        state["remote_workers"][0]["models"].append("changed")

        worker = self.registry.get_remote(
            "worker-2", "middleware-2"
        )

        self.assertEqual(worker["models"], ["llama"])

        worker["models"].append("changed-again")

        self.assertEqual(
            self.registry.get_remote(
                "worker-2", "middleware-2"
            )["models"],
            ["llama"]
        )

    def test_unknown_metadata_is_preserved(self):
        self.registry.upsert_remote({
            "worker_id": "worker-2",
            "owner": "middleware-2",
            "future_version": {"opaque": [7, 8]}
        })

        stored = self.registry.get_remote(
            "worker-2", "middleware-2"
        )

        self.assertEqual(
            stored["future_version"],
            {"opaque": [7, 8]}
        )

    def test_invalid_identity_rejected(self):
        invalid_records = [
            None,
            {},
            {"worker_id": ""},
            {"worker_id": 5}
        ]

        for record in invalid_records:
            with self.subTest(record=record):
                with self.assertRaises(ValueError):
                    self.registry.upsert_local(record)

        with self.assertRaises(ValueError):
            self.registry.upsert_local({
                "worker_id": "worker-1",
                "owner": "middleware-2"
            })

        for owner in (None, "", "middleware-1"):
            with self.subTest(owner=owner):
                with self.assertRaises(ValueError):
                    self.registry.upsert_remote({
                        "worker_id": "worker-2",
                        "owner": owner
                    })

    def test_remove_only_requested_partition(self):
        self.registry.upsert_local({
            "worker_id": "same"
        })

        self.registry.upsert_remote({
            "worker_id": "same",
            "owner": "middleware-2"
        })

        self.assertTrue(
            self.registry.remove_remote("same", "middleware-2")
        )

        self.assertFalse(
            self.registry.remove_remote("same", "middleware-2")
        )

        self.assertIsNotNone(
            self.registry.get_local("same")
        )

        self.assertTrue(self.registry.remove_local("same"))
        self.assertFalse(self.registry.remove_local("same"))

    def test_concurrent_upserts(self):
        def write(index):
            worker_id = f"worker-{index}"

            self.registry.upsert_local({
                "worker_id": worker_id
            })

            self.registry.upsert_remote({
                "worker_id": worker_id,
                "owner": "middleware-2"
            })

            self.assertIsNotNone(
                self.registry.get_local(worker_id)
            )

            self.assertIsNotNone(
                self.registry.get_remote(
                    worker_id, "middleware-2"
                )
            )

        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(write, range(200)))

        state = self.registry.snapshot()

        self.assertEqual(state["local_count"], 200)
        self.assertEqual(state["remote_count"], 200)


if __name__ == "__main__":
    unittest.main()
