
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

from registry import VersionedWorkerRegistry
import replication

class TestVersionedRegistry(unittest.TestCase):

    def setUp(self):
        self.a = VersionedWorkerRegistry("middleware-1")
        self.b = VersionedWorkerRegistry("middleware-2")

    @staticmethod
    def worker(worker_id="worker-1", load=0, owner=None):
        worker = {
            "worker_id": worker_id,
            "host": "127.0.0.1",
            "port": 9101,
            "models": ["llama"],
            "status": "healthy",
            "active_requests": load,
            "capacity": 4,
            "last_heartbeat": time.time()
        }

        if owner is not None:
            worker["owner"] = owner

        return worker

    def test_local_version_increases_on_worker_change(self):
        self.a.sync_local_workers([
            self.worker(load=0)
        ])

        old = self.a.export_gossip_records()[0]

        self.a.sync_local_workers([
            self.worker(load=2)
        ])

        new = self.a.export_gossip_records()[0]

        self.assertEqual(new["owner"], "middleware-1")
        self.assertEqual(new["active_requests"], 2)

        self.assertEqual(
            self.a.compare_versions(
                new["version"],
                old["version"]
            ),
            1
        )

    def test_out_of_order_and_duplicate_updates(self):
        self.a.sync_local_workers([
            self.worker(load=0)
        ])

        old = self.a.export_gossip_records()[0]

        self.a.sync_local_workers([
            self.worker(load=2)
        ])

        new = self.a.export_gossip_records()[0]

        self.assertEqual(
            self.b.apply_remote(new),
            "added"
        )

        self.assertEqual(
            self.b.apply_remote(old),
            "stale"
        )

        self.assertEqual(
            self.b.apply_remote(new),
            "duplicate"
        )

        state = self.b.replication_snapshot()

        self.assertEqual(
            state["remote_workers"][0]["active_requests"],
            2
        )

    def test_equal_version_conflict_uses_resolver(self):
        self.a.sync_local_workers([
            self.worker()
        ])

        record = self.a.export_gossip_records()[0]

        self.assertEqual(
            self.b.apply_remote(record),
            "added"
        )

        altered = {
            **record,
            "active_requests": 99
        }

        # Joseph's resolver determines the winner.
        winner, expected_outcome = replication.resolve_update(
            record, altered
        )

        outcome = self.b.apply_remote(altered)

        self.assertEqual(outcome, expected_outcome)

        stored = self.b.replication_snapshot()[
            "remote_workers"
        ][0]

        self.assertEqual(
            stored["active_requests"],
            winner["active_requests"]
        )

    def test_multi_hop_preserves_origin(self):
        c = VersionedWorkerRegistry("middleware-3")

        self.a.sync_local_workers([
            self.worker()
        ])

        original = self.a.export_gossip_records()[0]

        self.assertEqual(
            self.b.apply_remote(original),
            "added"
        )

        forwarded = self.b.export_gossip_records()[0]

        self.assertEqual(
            forwarded["owner"],
            "middleware-1"
        )

        self.assertEqual(
            forwarded["version"],
            original["version"]
        )

        self.assertEqual(
            c.apply_remote(forwarded),
            "added"
        )

        state = c.replication_snapshot()

        self.assertEqual(
            state["remote_workers"][0]["owner"],
            "middleware-1"
        )

    def test_tombstone_blocks_stale_resurrection(self):
        self.a.sync_local_workers([
            self.worker()
        ])

        old = self.a.export_gossip_records()[0]

        self.assertEqual(
            self.b.apply_remote(old),
            "added"
        )

        # The authoritative owner removes the worker.
        self.a.sync_local_workers([])

        tombstone = self.a.export_gossip_records()[0]

        self.assertTrue(tombstone["deleted"])

        self.assertEqual(
            self.b.apply_remote(tombstone),
            "deleted"
        )

        # An older healthy record must not restore it.
        self.assertEqual(
            self.b.apply_remote(old),
            "stale"
        )

        state = self.b.replication_snapshot()

        self.assertEqual(state["remote_count"], 0)
        self.assertEqual(state["tombstone_count"], 1)

    def test_reconnect_accepts_latest_version(self):
        self.a.sync_local_workers([
            self.worker(load=0)
        ])

        old = self.a.export_gossip_records()[0]

        self.b.apply_remote(old)

        # Simulate a partition: B misses these updates.
        self.a.sync_local_workers([
            self.worker(load=1)
        ])

        self.a.sync_local_workers([
            self.worker(load=3)
        ])

        latest = self.a.export_gossip_records()[0]

        self.assertEqual(
            self.b.apply_remote(latest),
            "updated"
        )

        state = self.b.replication_snapshot()

        self.assertEqual(
            state["remote_workers"][0]["active_requests"],
            3
        )

    def test_same_worker_different_owners_keeps_claims(self):
        self.a.sync_local_workers([
            self.worker(worker_id="shared")
        ])

        self.b.sync_local_workers([
            self.worker(worker_id="shared")
        ])

        c = VersionedWorkerRegistry("middleware-3")

        self.assertEqual(
            c.apply_remote(
                self.a.export_gossip_records()[0]
            ),
            "added"
        )

        self.assertEqual(
            c.apply_remote(
                self.b.export_gossip_records()[0]
            ),
            "added"
        )

        state = c.replication_snapshot()

        self.assertEqual(state["remote_count"], 2)

    def test_duplicate_does_not_refresh_stale_origin(self):
        self.b.remote_ttl = -1

        self.a.sync_local_workers([
            self.worker()
        ])

        record = self.a.export_gossip_records()[0]

        self.b.apply_remote(record)
        self.b.apply_remote(record)

        state = self.b.replication_snapshot()

        self.assertEqual(
            state["fresh_remote_count"],
            0
        )

    def test_versions_preserved_as_defensive_copies(self):
        self.a.sync_local_workers([
            self.worker()
        ])

        exported = self.a.export_gossip_records()

        exported[0]["version"] = 9999
        exported[0]["models"].append("changed")

        original = self.a.export_gossip_records()[0]

        self.assertNotEqual(
            original["version"], 9999
        )

        self.assertEqual(
            original["models"], ["llama"]
        )

    def test_equal_version_tombstone_wins(self):
        live = {
            **self.worker("worker-2"),
            "owner": "middleware-2",
            "version": 7,
            "deleted": False
        }

        tombstone = {
            "worker_id": "worker-2",
            "owner": "middleware-2",
            "version": 7,
            "deleted": True
        }

        self.assertEqual(
            self.a.apply_remote(live),
            "added"
        )

        self.assertEqual(
            self.a.apply_remote(tombstone),
            "deleted"
        )

        # An equal-version live update must not
        # resurrect a deleted worker.
        self.assertEqual(
            self.a.apply_remote(live),
            replication.CONFLICT_LOST
        )

        state = self.a.replication_snapshot()

        self.assertEqual(state["remote_count"], 0)
        self.assertEqual(state["tombstone_count"], 1)

    def test_re_registration_advances_past_tombstone(self):
        self.a.sync_local_workers([
            self.worker()
        ])

        first = self.a.export_gossip_records()[0]

        # Remove the local worker.
        self.a.sync_local_workers([])

        tombstone = self.a.export_gossip_records()[0]

        self.assertTrue(tombstone["deleted"])

        # The same worker registers again.
        self.a.sync_local_workers([
            self.worker()
        ])

        restored = self.a.export_gossip_records()[0]

        self.assertFalse(restored["deleted"])

        # A newly registered worker must receive
        # a newer version than its tombstone.
        self.assertLess(
            first["version"],
            tombstone["version"]
        )

        self.assertLess(
            tombstone["version"],
            restored["version"]
        )

    def test_heartbeat_advances_version(self):
        worker = self.worker()

        self.a.sync_local_workers([worker])

        first = self.a.export_gossip_records()[0]

        # The same snapshot must not increment
        # the version unnecessarily.
        self.a.sync_local_workers([worker])

        unchanged = self.a.export_gossip_records()[0]

        self.assertEqual(
            first["version"],
            unchanged["version"]
        )

        # Simulate a new heartbeat timestamp.
        updated = {
            **worker,
            "last_heartbeat": worker["last_heartbeat"] + 1
        }

        self.a.sync_local_workers([updated])

        newer = self.a.export_gossip_records()[0]

        self.assertGreater(
            newer["version"],
            first["version"]
        )

    def test_multiple_worker_updates_in_batch(self):
        workers = [
            self.worker("worker-1"),
            self.worker("worker-2"),
            self.worker("worker-3")
        ]

        self.a.sync_local_workers(workers)

        outcomes = self.b.apply_remote_batch(
            self.a.export_gossip_records()
        )

        self.assertEqual(len(outcomes), 3)

        self.assertTrue(
            all(
                outcome == "added"
                for outcome in outcomes.values()
            )
        )

        # Update the load of only worker-2.
        changed = [
            {
                **worker,
                "active_requests": 4
            }
            if worker["worker_id"] == "worker-2"
            else worker
            for worker in workers
        ]

        self.a.sync_local_workers(changed)

        self.b.apply_remote_batch(
            self.a.export_gossip_records()
        )

        state = self.b.replication_snapshot()

        by_id = {
            record["worker_id"]: record
            for record in state["remote_workers"]
        }

        self.assertEqual(len(by_id), 3)

        self.assertEqual(
            by_id["worker-2"]["active_requests"],
            4
        )

        self.assertEqual(
            by_id["worker-1"]["active_requests"],
            0
        )

    def test_conflicting_owners_have_deterministic_winner(self):
        first = {
            **self.worker("shared"),
            "owner": "middleware-2",
            "version": 7,
            "deleted": False
        }

        second = {
            **self.worker("shared"),
            "owner": "middleware-3",
            "version": 7,
            "deleted": False
        }

        self.a.apply_remote(first)
        self.a.apply_remote(second)

        state = self.a.replication_snapshot()

        # Both claims remain available for
        # ownership-conflict detection.
        self.assertEqual(state["remote_count"], 2)

        self.assertEqual(
            state["conflicting_worker_ids"],
            ["shared"]
        )

        # Joseph's resolver chooses the same winner
        # regardless of message arrival order.
        self.assertEqual(
            state["resolved_remote_workers"][0]["owner"],
            "middleware-3"
        )

        other = VersionedWorkerRegistry("middleware-4")

        other.apply_remote(second)
        other.apply_remote(first)

        other_state = other.replication_snapshot()

        self.assertEqual(
            state["resolved_remote_workers"],
            other_state["resolved_remote_workers"]
        )



class TestVersionedMiddlewareEndpoints(unittest.TestCase):

    def setUp(self):
        import middleware

        self.middleware = middleware

        self.old_node_id = middleware.NODE_ID
        self.old_registry = middleware.replicated_registry

        middleware.NODE_ID = "middleware-1"
        middleware.replicated_registry = VersionedWorkerRegistry(
            "middleware-1"
        )

        with middleware.registry_lock:
            self.old_workers = dict(
                middleware.worker_registry
            )
            middleware.worker_registry.clear()

        with middleware.received_gossip_lock:
            self.old_gossip = middleware.received_gossip
            middleware.received_gossip = {}

        self.client = middleware.app.test_client()

    def tearDown(self):
        m = self.middleware

        with m.registry_lock:
            m.worker_registry.clear()
            m.worker_registry.update(self.old_workers)

        with m.received_gossip_lock:
            m.received_gossip = self.old_gossip

        m.replicated_registry = self.old_registry
        m.NODE_ID = self.old_node_id

    def test_register_and_heartbeat_are_versioned(self):
        response = self.client.post("/register", json={
            "worker_id": "worker-1",
            "host": "127.0.0.1",
            "port": 9101,
            "models": ["llama"],
            "capacity": 3
        })

        self.assertEqual(response.status_code, 201)

        old = self.client.get(
            "/replication/workers"
        ).get_json()["local_workers"][0]

        response = self.client.post("/heartbeat", json={
            "worker_id": "worker-1",
            "active_requests": 2,
            "capacity": 3
        })

        self.assertEqual(response.status_code, 200)

        newer = self.client.get(
            "/replication/workers"
        ).get_json()["local_workers"][0]

        self.assertEqual(newer["active_requests"], 2)

        self.assertEqual(
            VersionedWorkerRegistry.compare_versions(
                newer["version"],
                old["version"]
            ),
            1
        )

        self.assertEqual(
            self.client.get("/workers").get_json()["count"],
            1
        )

    def test_versioned_gossip_rejects_stale_remote(self):
        remote = {
            "worker_id": "remote-1",
            "owner": "middleware-2",
            "host": "127.0.0.1",
            "port": 9102,
            "models": ["llama"],
            "status": "healthy",
            "active_requests": 2,
            "capacity": 3,
            "version": 2,
            "deleted": False
        }

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-2",
            "workers": [remote]
        })

        self.assertEqual(response.status_code, 200)

        older = {
            **remote,
            "version": 1,
            "active_requests": 0
        }

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-3",
            "workers": [older]
        })

        self.assertEqual(response.status_code, 200)

        state = self.client.get(
            "/replication/workers"
        ).get_json()

        self.assertEqual(state["remote_count"], 1)

        self.assertEqual(
            state["remote_workers"][0]["active_requests"],
            2
        )

        # Active routing is still local-only.
        self.assertEqual(
            self.client.get("/workers").get_json()["count"],
            0
        )

    def test_forwarded_tombstone_and_safe_validation(self):
        remote = {
            "worker_id": "remote-1",
            "owner": "middleware-2",
            "host": "127.0.0.1",
            "port": 9102,
            "models": ["llama"],
            "status": "healthy",
            "active_requests": 0,
            "capacity": 3,
            "version": 1,
            "deleted": False
        }

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-2",
            "workers": [remote]
        })

        self.assertEqual(response.status_code, 200)

        tombstone = {
            "worker_id": "remote-1",
            "owner": "middleware-2",
            "deleted": True,
            "version": 2
        }

        # Another peer forwards the deletion.
        response = self.client.post("/gossip", json={
            "sender_id": "middleware-3",
            "workers": [tombstone]
        })

        self.assertEqual(response.status_code, 200)

        state = self.client.get(
            "/replication/workers"
        ).get_json()

        self.assertEqual(state["remote_count"], 0)
        self.assertEqual(state["tombstone_count"], 1)

        # Existing inspection endpoints should still work.
        self.assertEqual(
            self.client.get("/gossip/received").status_code,
            200
        )

        self.assertEqual(
            self.client.get("/gossip/workers").status_code,
            200
        )

        # Invalid version rejects the complete message.
        bad = {
            **remote,
            "version": {
                "generation": -1,
                "revision": 200
            }
        }

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-3",
            "workers": [bad]
        })

        self.assertEqual(response.status_code, 400)

        self.assertEqual(
            self.client.get(
                "/replication/workers"
            ).get_json()["remote_count"],
            0
        )

    def test_versioned_gossip_preserves_routing_metadata(self):
        remote = {
            "worker_id": "remote-metadata",
            "owner": "middleware-2",
            "host": "127.0.0.1",
            "port": 9102,
            "models": ["llama"],
            "status": "healthy",
            "active_requests": 1,
            "capacity": 4,
            "cpu_load_1m": 1.25,
            "hardware": {"gpu": "RTX"},
            "version": 7,
            "deleted": False
        }

        response = self.client.post("/gossip", json={
            "sender_id": "middleware-2",
            "workers": [remote]
        })

        self.assertEqual(response.status_code, 200)

        state = self.client.get(
            "/replication/workers"
        ).get_json()

        stored = state["remote_workers"][0]

        self.assertEqual(stored["cpu_load_1m"], 1.25)
        self.assertEqual(stored["hardware"], {"gpu": "RTX"})

        # The original full record must now compare as
        # a duplicate, not an equal-version conflict.
        self.assertEqual(
            self.middleware.replicated_registry.apply_remote(remote),
            replication.DUPLICATE
        )


if __name__ == "__main__":
    unittest.main()
