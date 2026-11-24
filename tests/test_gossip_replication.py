
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

from registry import VersionedWorkerRegistry
from gossip import build_gossip_message
from middleware import validate_gossip_message


def worker(worker_id, load=0):
    return {
        "worker_id": worker_id,
        "host": "127.0.0.1",
        "port": 9101,
        "models": ["llama"],
        "status": "healthy",
        "active_requests": load,
        "capacity": 4,
        "last_heartbeat": 1.0
    }


class TestVersionedGossipFlow(unittest.TestCase):

    def setUp(self):
        self.a = VersionedWorkerRegistry("middleware-1")
        self.b = VersionedWorkerRegistry("middleware-2")
        self.c = VersionedWorkerRegistry("middleware-3")

    def deliver(self, source, target):
        """
        Simulate one middleware-to-middleware gossip exchange
        using the actual message builder and validator.
        """

        message = build_gossip_message(
            source.node_id,
            {
                "workers": source.export_gossip_records()
            }
        )

        sender, records = validate_gossip_message(message)

        self.assertEqual(sender, source.node_id)

        return target.apply_remote_batch(records)

    def test_two_node_newer_update_and_replay(self):
        """
        A newer update must replace an older version.
        A delayed stale message must not overwrite it.
        """

        self.a.sync_local_workers([
            worker("worker-1")
        ])

        old = self.a.export_gossip_records()[0]

        # First propagation: A -> B
        self.deliver(self.a, self.b)

        self.a.sync_local_workers([
            worker("worker-1", load=3)
        ])

        # Propagate newer version: A -> B
        self.deliver(self.a, self.b)

        # Replay an older message after the newer state.
        self.assertEqual(
            self.b.apply_remote(old),
            "stale"
        )

        record = self.b.replication_snapshot()[
            "remote_workers"
        ][0]

        self.assertEqual(
            record["active_requests"], 3
        )

        self.assertEqual(record["version"], 2)

    def test_multi_hop_tombstone_and_late_replay(self):
        """
        A worker deletion must propagate through multiple
        middleware nodes and prevent stale resurrection.
        """

        self.a.sync_local_workers([
            worker("worker-1")
        ])

        stale = self.a.export_gossip_records()[0]

        # Initial worker propagation: A -> B -> C
        self.deliver(self.a, self.b)
        self.deliver(self.b, self.c)

        self.assertEqual(
            self.c.replication_snapshot()["remote_count"],
            1
        )

        # The owner removes the worker.
        self.a.sync_local_workers([])

        # Deletion propagates through another middleware.
        self.deliver(self.a, self.b)
        self.deliver(self.b, self.c)

        state = self.c.replication_snapshot()

        self.assertEqual(state["remote_count"], 0)
        self.assertEqual(state["tombstone_count"], 1)

        # Delayed old data cannot restore the deleted worker.
        self.assertEqual(
            self.c.apply_remote(stale),
            "stale"
        )

        self.assertEqual(
            self.c.replication_snapshot()["remote_count"],
            0
        )

    def test_multiple_workers_relayed_together(self):
        """
        Multiple workers should propagate together without
        losing records or changing their original ownership.
        """

        self.a.sync_local_workers([
            worker("w1"),
            worker("w2"),
            worker("w3")
        ])

        # A -> B -> C
        self.deliver(self.a, self.b)
        self.deliver(self.b, self.c)

        state = self.c.replication_snapshot()

        self.assertEqual(state["remote_count"], 3)

        self.assertEqual(
            {
                record["worker_id"]
                for record in state["remote_workers"]
            },
            {"w1", "w2", "w3"}
        )

        # Forwarding must preserve the authoritative owner.
        self.assertTrue(
            all(
                record["owner"] == "middleware-1"
                for record in state["remote_workers"]
            )
        )

    def test_invalid_version_is_rejected_before_apply(self):
        """
        Invalid version formats must be rejected during
        gossip validation before changing registry state.
        """

        self.a.sync_local_workers([
            worker("worker-1")
        ])

        good = self.a.export_gossip_records()[0]

        invalid_versions = [
            -1,
            True,
            {
                "generation": 100,
                "revision": 1
            }
        ]

        for value in invalid_versions:
            with self.subTest(version=value):
                message = build_gossip_message(
                    "middleware-1",
                    {
                        "workers": [
                            {
                                **good,
                                "version": value
                            }
                        ]
                    }
                )

                with self.assertRaises(ValueError):
                    validate_gossip_message(message)

        # No invalid records should reach the target registry.
        self.assertEqual(
            self.b.replication_snapshot()["remote_count"],
            0
        )

    def test_legacy_unversioned_message_stays_snapshot_only(self):
        """
        Legacy gossip without versions should remain compatible
        but must not modify the versioned registry.
        """

        legacy = {
            key: value
            for key, value in worker("worker-1").items()
            if key != "last_heartbeat"
        }

        legacy["owner"] = "middleware-1"

        message = build_gossip_message(
            "middleware-1",
            {
                "workers": [legacy]
            }
        )

        sender, records = validate_gossip_message(message)

        self.assertEqual(sender, "middleware-1")

        # Only versioned records enter the replicated registry.
        outcomes = self.b.apply_remote_batch(records)

        self.assertEqual(outcomes, {})

        self.assertEqual(
            self.b.replication_snapshot()["remote_count"],
            0
        )


if __name__ == "__main__":
    unittest.main()
