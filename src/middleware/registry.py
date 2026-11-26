
import copy
import threading
import time

import replication

class ReplicatedWorkerRegistry:
    """
    Version-neutral storage for local workers and remote ownership claims.

    This class does not decide whether incoming records are newer or stale.
    A future replication resolver must make that decision before calling
    upsert_remote().
    """

    def __init__(self, node_id):
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")

        self.node_id = node_id.strip()

        # Locally owned records, indexed by worker_id.
        self._local = {}

        # Remote records, indexed by (owner, worker_id).
        # Different ownership claims can coexist until conflict resolution.
        self._remote = {}

        self._lock = threading.RLock()

    @staticmethod
    def _get_worker_id(worker):
        if not isinstance(worker, dict):
            raise ValueError("worker must be a dictionary")

        worker_id = worker.get("worker_id")

        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must be a non-empty string")

        return worker_id.strip()

    def upsert_local(self, worker):
        """
        Insert or replace a locally owned worker record.
        """

        worker_id = self._get_worker_id(worker)

        if worker.get("owner") not in (None, self.node_id):
            raise ValueError("local worker owner must match node_id")

        record = copy.deepcopy(worker)
        record["worker_id"] = worker_id
        record["owner"] = self.node_id

        with self._lock:
            self._local[worker_id] = record

        return copy.deepcopy(record)

    def upsert_remote(self, worker):
        """
        Store an already-authorized remote record.

        Does not compare versions or reject stale updates.
        Version checking must occur before this method is called.
        """

        worker_id = self._get_worker_id(worker)
        owner = worker.get("owner")

        if not isinstance(owner, str) or not owner.strip():
            raise ValueError(
                "remote worker owner must be a non-empty string"
            )

        owner = owner.strip()

        if owner == self.node_id:
            raise ValueError(
                "remote worker cannot claim local node ownership"
            )

        record = copy.deepcopy(worker)
        record["worker_id"] = worker_id
        record["owner"] = owner

        with self._lock:
            self._remote[(owner, worker_id)] = record

        return copy.deepcopy(record)

    def get_local(self, worker_id):
        """
        Retrieve a locally owned worker.
        """

        with self._lock:
            return copy.deepcopy(self._local.get(worker_id))

    def get_remote(self, worker_id, owner):
        """
        Retrieve a remote worker belonging to a specific owner.
        """

        with self._lock:
            return copy.deepcopy(
                self._remote.get((owner, worker_id))
            )

    def get_remote_claims(self, worker_id):
        """
        Return all remote ownership claims for a worker.

        Conflict resolution will be implemented later.
        """

        with self._lock:
            return [
                copy.deepcopy(self._remote[(owner, stored_id)])
                for owner, stored_id in sorted(self._remote)
                if stored_id == worker_id
            ]

    def get_all_local(self):
        with self._lock:
            return [
                copy.deepcopy(self._local[key])
                for key in sorted(self._local)
            ]

    def get_all_remote(self):
        with self._lock:
            return [
                copy.deepcopy(self._remote[key])
                for key in sorted(self._remote)
            ]

    def remove_local(self, worker_id):
        """
        Remove a local record from storage.

        This does not create a distributed deletion tombstone.
        """

        with self._lock:
            return self._local.pop(worker_id, None) is not None

    def remove_remote(self, worker_id, owner):
        """
        Remove a remote record from storage.

        This does not create a distributed deletion tombstone.
        """

        with self._lock:
            return (
                self._remote.pop((owner, worker_id), None)
                is not None
            )

    def snapshot(self):
        """
        Return a consistent, independent copy of registry state.
        """

        with self._lock:
            local = [
                copy.deepcopy(self._local[key])
                for key in sorted(self._local)
            ]

            remote = [
                copy.deepcopy(self._remote[key])
                for key in sorted(self._remote)
            ]

            return {
                "node_id": self.node_id,
                "local_count": len(local),
                "remote_count": len(remote),
                "local_workers": local,
                "remote_workers": remote
            }



class VersionedWorkerRegistry(ReplicatedWorkerRegistry):
    """
    Versioned worker storage using replication.py.

    Local workers remain authoritative on their owner.
    Remote records are reconciled through Joseph's resolver.
    """

    def __init__(self, node_id, remote_ttl=45):
        super().__init__(node_id)

        # Track versions independently for each local worker.
        self._last_local_versions = {}
        self._last_local_heartbeats = {}

        # Remote freshness and deletion metadata.
        self._remote_updated_at = {}
        self._tombstones = {}

        self.remote_ttl = remote_ttl

    @staticmethod
    def validate_version(version):
        return replication.validate_version(version)

    @staticmethod
    def compare_versions(incoming, current):
        return replication.compare_versions(
            incoming, current
        )

    def _new_version(self, worker_id):
        """
        Generate the next version for one local worker.

        Must be called while holding self._lock.
        """

        previous = self._last_local_versions.get(
            worker_id,
            replication.INITIAL_VERSION
        )

        version = replication.bump_version({
            "version": previous
        })["version"]

        self._last_local_versions[worker_id] = version

        return version

    def _local_record(self, worker):
        """
        Convert a locally registered worker into a
        replication record.
        """

        worker_id = self._get_worker_id(worker)

        return {
            "worker_id": worker_id,
            "host": worker["host"],
            "port": worker["port"],
            "models": copy.deepcopy(worker["models"]),
            "status": worker["status"],
            "owner": self.node_id,
            "active_requests": worker.get(
                "active_requests", 0
            ),
            "capacity": worker.get("capacity"),
            "cpu_load_1m": worker.get("cpu_load_1m"),
            "hardware": copy.deepcopy(
                worker.get("hardware", {})
            ),
            "deleted": False
        }

    def sync_local_workers(self, workers):
        """
        Synchronize authoritative local worker state.

        Changed fields receive a new integer version.
        Owner heartbeats also advance the version so
        fresh worker information continues propagating.

        Removed workers become versioned tombstones.
        """

        with self._lock:
            seen = set()

            for worker in workers:
                record = self._local_record(worker)
                worker_id = record["worker_id"]
                seen.add(worker_id)

                old = self._local.get(worker_id)
                heartbeat = worker.get("last_heartbeat")

                # Use Joseph's field-change detection.
                changed = replication.versioned_fields_changed(
                    old, record
                )

                heartbeat_changed = (
                    heartbeat is not None
                    and heartbeat !=
                    self._last_local_heartbeats.get(worker_id)
                )

                if changed or heartbeat_changed:
                    record["version"] = self._new_version(
                        worker_id
                    )

                    self._local[worker_id] = record

                    self._tombstones.pop(
                        (self.node_id, worker_id),
                        None
                    )

                self._last_local_heartbeats[
                    worker_id
                ] = heartbeat

            # Workers removed from the authoritative
            # registry become tombstones.
            for worker_id in list(self._local):
                if worker_id in seen:
                    continue

                self._local.pop(worker_id)

                self._last_local_heartbeats.pop(
                    worker_id, None
                )

                self._tombstones[
                    (self.node_id, worker_id)
                ] = {
                    "worker_id": worker_id,
                    "owner": self.node_id,
                    "deleted": True,
                    "version": self._new_version(worker_id)
                }

    def apply_remote(self, worker):
        """
        Apply a remote ownership claim using
        replication.resolve_update().

        Records belonging to different owners remain
        separately identifiable for ownership checks.
        """

        worker_id = self._get_worker_id(worker)
        owner = worker.get("owner")

        if not isinstance(owner, str) or not owner.strip():
            raise ValueError(
                "remote record requires owner"
            )

        owner = owner.strip()

        # Never overwrite locally authoritative state.
        if owner == self.node_id:
            return "ignored"

        version = replication.validate_version(
            worker.get("version")
        )

        deleted = worker.get("deleted", False)

        if not isinstance(deleted, bool):
            raise ValueError(
                "deleted must be a boolean"
            )

        key = (owner, worker_id)

        record = copy.deepcopy(worker)
        record.update({
            "worker_id": worker_id,
            "owner": owner,
            "version": version,
            "deleted": deleted
        })

        with self._lock:
            old = self._remote.get(key)

            if old is None:
                old = self._tombstones.get(key)

            # A tombstone wins over a live record when
            # both claim the same owner and version.
            if (
                old is not None
                and old["version"] == version
                and old.get("deleted", False) != deleted
            ):
                outcome = (
                    replication.CONFLICT_WON
                    if deleted
                    else replication.CONFLICT_LOST
                )

                if not deleted:
                    return outcome

                winner = record

            else:
                winner, outcome = replication.resolve_update(
                    old, record
                )

            # Reject stale, duplicate, or losing updates.
            if outcome not in replication.ACCEPTED_OUTCOMES:
                return outcome

            accepted = copy.deepcopy(winner)

            if accepted["deleted"]:
                self._remote.pop(key, None)

                self._remote_updated_at.pop(
                    key, None
                )

                self._tombstones[key] = accepted

                return "deleted"

            self._remote[key] = accepted
            self._tombstones.pop(key, None)

            self._remote_updated_at[key] = time.time()

            # Preserve the existing registry result names
            # for ordinary accepted updates.
            if outcome == replication.NEW:
                return "added"

            if outcome == replication.NEWER:
                return "updated"

            return outcome

    def apply_remote_batch(self, workers):
        """
        Apply incoming versioned records.

        Legacy unversioned gossip remains snapshot-only.
        """

        outcomes = {}

        for worker in workers:
            if "version" not in worker:
                continue

            key = (
                worker["owner"],
                worker["worker_id"]
            )

            outcomes[key] = self.apply_remote(worker)

        return outcomes

    def export_gossip_records(self):
        """
        Export local workers, remote records, and
        tombstones for multi-hop gossip.
        """

        with self._lock:
            records = (
                list(self._local.values())
                + list(self._remote.values())
                + list(self._tombstones.values())
            )

            return sorted(
                copy.deepcopy(records),
                key=lambda worker: (
                    worker["worker_id"],
                    worker["owner"]
                )
            )

    def replication_snapshot(self):
        """
        Return a consistent registry snapshot.

        Fresh remote workers are those with accepted
        recent owner-state updates.
        """

        now = time.time()

        with self._lock:
            local = copy.deepcopy(
                list(self._local.values())
            )

            remote = copy.deepcopy(
                list(self._remote.values())
            )

            tombstones = copy.deepcopy(
                list(self._tombstones.values())
            )

            fresh = [
                copy.deepcopy(self._remote[key])
                for key in self._remote
                if (
                    now - self._remote_updated_at.get(
                        key, 0
                    ) <= self.remote_ttl
                )
            ]

        for records in (
            local, remote, fresh, tombstones
        ):
            records.sort(
                key=lambda worker: (
                    worker["worker_id"],
                    worker["owner"]
                )
            )

        # Build a diagnostic view of remote ownership claims.
        # Preserve all original claims for conflict detection.
        resolved = {}
        owners_by_worker = {}

        remote_records = remote + [
            record
            for record in tombstones
            if record["owner"] != self.node_id
        ]

        for record in remote_records:
            worker_id = record["worker_id"]

            owners_by_worker.setdefault(
                worker_id, set()
            ).add(record["owner"])

            # Use Joseph's deterministic resolver.
            winner, _ = replication.resolve_update(
                resolved.get(worker_id),
                record
            )

            resolved[worker_id] = copy.deepcopy(winner)

        resolved_remote_workers = sorted(
            resolved.values(),
            key=lambda worker: (
                worker["worker_id"],
                worker["owner"]
            )
        )

        conflicting_worker_ids = sorted(
            worker_id
            for worker_id, owners in owners_by_worker.items()
            if len(owners) > 1
        )

        return {
            "node_id": self.node_id,
            "local_count": len(local),
            "remote_count": len(remote),
            "fresh_remote_count": len(fresh),
            "tombstone_count": len(tombstones),
            "local_workers": local,
            "remote_workers": remote,
            "fresh_remote_workers": fresh,
            "tombstones": tombstones,
            "resolved_remote_workers": resolved_remote_workers,
            "conflicting_worker_ids": conflicting_worker_ids
        }
