import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import replication
from replication import (
    CONFLICT_LOST,
    CONFLICT_WON,
    DUPLICATE,
    NEW,
    NEWER,
    STALE,
)


def state(version, owner="middleware-1", status="healthy",
          worker_id="worker-1"):
    return {
        "worker_id": worker_id,
        "host": "127.0.0.1",
        "port": 9001,
        "models": ["model-a"],
        "status": status,
        "owner": owner,
        "version": version
    }


class TestVersionFormat(unittest.TestCase):

    def test_valid_versions(self):
        self.assertEqual(replication.validate_version(0), 0)
        self.assertEqual(replication.validate_version(7), 7)

    def test_invalid_versions_rejected(self):
        for bad in (-1, 1.5, "7", None, True):
            with self.assertRaises(ValueError):
                replication.validate_version(bad)

    def test_missing_version_defaults_to_initial(self):
        entry = state(0)
        del entry["version"]

        self.assertEqual(
            replication.get_version(entry), replication.INITIAL_VERSION
        )

    def test_bump_version_returns_copy(self):
        original = state(7)
        bumped = replication.bump_version(original)

        self.assertEqual(bumped["version"], 8)
        self.assertEqual(original["version"], 7)

    def test_versioned_fields_changed(self):
        # Worker status changes must update the version.
        self.assertTrue(
            replication.versioned_fields_changed(
                state(1),
                state(1, status="unavailable")
            )
        )

        # Worker load changes must also update the version.
        load_changed = state(1)
        load_changed["active_requests"] = 5

        self.assertTrue(
            replication.versioned_fields_changed(
                state(1), load_changed
            )
        )

        # Heartbeat timestamps alone do not require a new version.
        heartbeat_only = state(1)
        heartbeat_only["last_heartbeat"] = 123.0

        self.assertFalse(
            replication.versioned_fields_changed(
                state(1), heartbeat_only
            )
        )

    def test_routing_and_deletion_fields_changed(self):
        original = state(7)

        changes = {
            "active_requests": 4,
            "capacity": 8,
            "cpu_load_1m": 2.5,
            "hardware": {"gpu": "RTX"},
            "deleted": True
        }

        for field, value in changes.items():
            with self.subTest(field=field):
                updated = dict(original)
                updated[field] = value

                self.assertTrue(
                    replication.versioned_fields_changed(
                        original, updated
                    )
                )

        # Heartbeat timestamps are not versioned fields.
        updated = dict(original)
        updated["last_heartbeat"] = 123.0

        self.assertFalse(
            replication.versioned_fields_changed(
                original, updated
            )
        )


    def test_bump_version_deep_copies_metadata(self):
        original = state(7)
        original["hardware"] = {"gpu": "none"}

        updated = replication.bump_version(original)

        updated["models"].append("model-b")
        updated["hardware"]["gpu"] = "RTX"

        self.assertEqual(original["version"], 7)
        self.assertEqual(original["models"], ["model-a"])
        self.assertEqual(original["hardware"]["gpu"], "none")
        self.assertEqual(updated["version"], 8)


class TestComparison(unittest.TestCase):

    def test_compare_versions(self):
        self.assertEqual(replication.compare_versions(8, 7), 1)
        self.assertEqual(replication.compare_versions(7, 8), -1)
        self.assertEqual(replication.compare_versions(7, 7), 0)

    def test_is_stale(self):
        self.assertTrue(replication.is_stale(state(8), state(6)))
        self.assertFalse(replication.is_stale(state(8), state(9)))
        self.assertFalse(replication.is_stale(None, state(6)))

    def test_is_duplicate(self):
        self.assertTrue(replication.is_duplicate(state(7), state(7)))
        self.assertFalse(replication.is_duplicate(
            state(7), state(7, owner="middleware-2")
        ))
        self.assertFalse(replication.is_duplicate(None, state(7)))


class TestResolveUpdate(unittest.TestCase):

    def test_no_local_accepts_incoming(self):
        winner, outcome = replication.resolve_update(None, state(3))

        self.assertEqual(outcome, NEW)
        self.assertEqual(winner["version"], 3)

    def test_newer_wins(self):
        winner, outcome = replication.resolve_update(state(7), state(8))

        self.assertEqual(outcome, NEWER)
        self.assertEqual(winner["version"], 8)

    def test_stale_rejected(self):
        winner, outcome = replication.resolve_update(state(8), state(6))

        self.assertEqual(outcome, STALE)
        self.assertEqual(winner["version"], 8)

    def test_duplicate_keeps_local(self):
        local = state(7)
        winner, outcome = replication.resolve_update(local, state(7))

        self.assertEqual(outcome, DUPLICATE)
        self.assertIs(winner, local)

    def test_equal_version_conflict_is_deterministic(self):
        a = state(7, owner="middleware-1", status="healthy")
        b = state(7, owner="middleware-2", status="unavailable")

        winner_ab, outcome_ab = replication.resolve_update(a, b)
        winner_ba, outcome_ba = replication.resolve_update(b, a)

        self.assertEqual(outcome_ab, CONFLICT_WON)
        self.assertEqual(outcome_ba, CONFLICT_LOST)
        # Both nodes settle on the same state regardless of order.
        self.assertEqual(winner_ab, winner_ba)
        self.assertEqual(winner_ab["owner"], "middleware-2")

    def test_different_workers_rejected(self):
        with self.assertRaises(ValueError):
            replication.resolve_update(
                state(1), state(2, worker_id="worker-2")
            )

    def test_invalid_incoming_version_rejected(self):
        with self.assertRaises(ValueError):
            replication.resolve_update(state(1), state(-3))

    def test_inputs_not_mutated(self):
        local, incoming = state(7), state(8)
        replication.resolve_update(local, incoming)

        self.assertEqual(local, state(7))
        self.assertEqual(incoming, state(8))

    def test_should_apply(self):
        self.assertTrue(replication.should_apply(state(7), state(8)))
        self.assertFalse(replication.should_apply(state(8), state(6)))
        self.assertFalse(replication.should_apply(state(7), state(7)))

    def test_equal_version_same_owner_conflict_converges(self):
        a = state(7, status="healthy")
        b = state(7, status="unavailable")

        self.assertFalse(replication.is_duplicate(a, b))

        winner_ab, outcome_ab = replication.resolve_update(a, b)
        winner_ba, outcome_ba = replication.resolve_update(b, a)

        self.assertEqual(winner_ab, winner_ba)

        self.assertEqual(
            {outcome_ab, outcome_ba},
            {CONFLICT_WON, CONFLICT_LOST}
        )


class TestUpdateSequences(unittest.TestCase):

    def setUp(self):
        self.states = {}

    def apply_all(self, *versions):
        return [
            replication.apply_update(self.states, state(v))
            for v in versions
        ]

    def test_spec_example_7_then_8_then_6(self):
        outcomes = self.apply_all(7, 8, 6)

        self.assertEqual(outcomes, [NEW, NEWER, STALE])
        self.assertEqual(self.states["worker-1"]["version"], 8)

    def test_reordered_updates_converge(self):
        in_order = {}
        out_of_order = {}

        for v in (1, 2, 3, 4):
            replication.apply_update(in_order, state(v))

        for v in (3, 1, 4, 2):
            replication.apply_update(out_of_order, state(v))

        self.assertEqual(in_order, out_of_order)
        self.assertEqual(out_of_order["worker-1"]["version"], 4)

    def test_stale_update_does_not_overwrite(self):
        replication.apply_update(self.states, state(5, status="healthy"))
        outcome = replication.apply_update(
            self.states, state(4, status="unavailable")
        )

        self.assertEqual(outcome, STALE)
        self.assertEqual(self.states["worker-1"]["status"], "healthy")

    def test_duplicate_updates_change_nothing(self):
        self.apply_all(7)
        before = dict(self.states["worker-1"])

        outcomes = self.apply_all(7, 7, 7)

        self.assertEqual(outcomes, [DUPLICATE] * 3)
        self.assertEqual(self.states["worker-1"], before)

    def test_multiple_workers_are_independent(self):
        replication.apply_update(self.states, state(9, worker_id="worker-1"))
        replication.apply_update(self.states, state(2, worker_id="worker-2"))

        outcome = replication.apply_update(
            self.states, state(3, worker_id="worker-2")
        )

        self.assertEqual(outcome, NEWER)
        self.assertEqual(self.states["worker-1"]["version"], 9)
        self.assertEqual(self.states["worker-2"]["version"], 3)

    def test_stored_state_is_a_copy(self):
        incoming = state(1)
        replication.apply_update(self.states, incoming)
        incoming["status"] = "unavailable"

        self.assertEqual(self.states["worker-1"]["status"], "healthy")

    def test_missing_worker_id_rejected(self):
        with self.assertRaises(ValueError):
            replication.apply_update(self.states, {"version": 1})

    def test_nested_state_is_copied(self):
        incoming = state(1)
        incoming["hardware"] = {"gpu": "none"}

        replication.apply_update(self.states, incoming)

        incoming["models"].append("model-b")
        incoming["hardware"]["gpu"] = "RTX"

        stored = self.states["worker-1"]

        self.assertEqual(stored["models"], ["model-a"])
        self.assertEqual(stored["hardware"]["gpu"], "none")


if __name__ == "__main__":
    unittest.main()
