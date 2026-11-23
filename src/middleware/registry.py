
import copy
import threading
import time


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
    Versioned worker registry for distributed replication.

    Local workers are authoritative on their owning node.
    Remote records are applied only when their version is newer.

    This registry does not change active inference routing.
    """

    def __init__(self, node_id, comparator=None, remote_ttl=45):
        super().__init__(node_id)

        # Provisional process incarnation and revision counter
        self._generation = time.time_ns()
        self._revision = 0

        # Can be replaced by Joseph's version comparator later
        self._comparator = comparator or self.compare_versions

        # Deleted worker records
        self._tombstones = {}

        # Track local heartbeats and remote update freshness
        self._last_local_heartbeats = {}
        self._remote_updated_at = {}

        self.remote_ttl = remote_ttl

    @staticmethod
    def validate_version(version):
        """
        Validate the provisional version format:
        {"generation": int, "revision": int}
        """

        if not isinstance(version, dict):
            raise ValueError("version must be an object")

        for key in ("generation", "revision"):
            value = version.get(key)

            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"version.{key} must be a non-negative integer"
                )

        if set(version) != {"generation", "revision"}:
            raise ValueError(
                "version must contain only generation and revision"
            )

    @staticmethod
    def compare_versions(incoming, current):
        """
        Compare two versions.

        Returns:
            1  incoming is newer
            0  versions are equal
           -1  incoming is older
        """

        VersionedWorkerRegistry.validate_version(incoming)
        VersionedWorkerRegistry.validate_version(current)

        a = (
            incoming["generation"],
            incoming["revision"]
        )

        b = (
            current["generation"],
            current["revision"]
        )

        return (a > b) - (a < b)

    def _new_version(self):
        """
        Generate the next local state version.

        Must be called while holding the registry lock.
        """

        self._revision += 1

        return {
            "generation": self._generation,
            "revision": self._revision
        }

    def _local_record(self, worker):
        """
        Convert an existing middleware worker into a
        canonical replication record.
        """

        worker_id = self._get_worker_id(worker)

        return {
            "worker_id": worker_id,
            "host": worker["host"],
            "port": worker["port"],
            "models": copy.deepcopy(worker["models"]),
            "status": worker["status"],
            "owner": self.node_id,
            "active_requests": worker.get("active_requests", 0),
            "capacity": worker.get("capacity"),
            "deleted": False
        }

    def sync_local_workers(self, workers):
        """
        Synchronize authoritative local worker state.

        New or changed records receive a new version.
        Missing local workers become tombstones.
        """

        with self._lock:
            seen = set()

            for worker in workers:
                record = self._local_record(worker)
                worker_id = record["worker_id"]

                seen.add(worker_id)

                heartbeat = worker.get("last_heartbeat")
                old = self._local.get(worker_id)

                old_data = (
                    {
                        key: value
                        for key, value in old.items()
                        if key != "version"
                    }
                    if old
                    else None
                )

                changed = old_data != record

                heartbeat_changed = (
                    heartbeat is not None
                    and self._last_local_heartbeats.get(worker_id)
                    != heartbeat
                )

                if changed or heartbeat_changed:
                    record["version"] = self._new_version()

                    self._local[worker_id] = record

                    self._tombstones.pop(
                        (self.node_id, worker_id),
                        None
                    )

                self._last_local_heartbeats[worker_id] = heartbeat

            # Generate tombstones for removed local workers
            for worker_id in list(self._local):
                if worker_id not in seen:
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
                        "version": self._new_version()
                    }

    def apply_remote(self, worker):
        """
        Apply one incoming versioned worker record.

        Returns:
            added
            updated
            deleted
            duplicate
            stale
            conflict
            ignored
        """

        worker_id = self._get_worker_id(worker)
        owner = worker.get("owner")

        if not isinstance(owner, str) or not owner.strip():
            raise ValueError("remote record requires owner")

        owner = owner.strip()

        # Never allow gossip to overwrite local ownership.
        if owner == self.node_id:
            return "ignored"

        version = worker.get("version")
        self.validate_version(version)

        deleted = worker.get("deleted", False)

        if not isinstance(deleted, bool):
            raise ValueError("deleted must be a boolean")

        key = (owner, worker_id)

        record = copy.deepcopy(worker)
        record["owner"] = owner
        record["worker_id"] = worker_id
        record["deleted"] = deleted

        with self._lock:
            old = (
                self._remote.get(key)
                or self._tombstones.get(key)
            )

            if old is not None:
                comparison = self._comparator(
                    version,
                    old["version"]
                )

                if comparison < 0:
                    return "stale"

                if comparison == 0:
                    if record == old:
                        return "duplicate"

                    return "conflict"

            if deleted:
                self._remote.pop(key, None)
                self._remote_updated_at.pop(key, None)

                self._tombstones[key] = record

                return "deleted"

            self._remote[key] = record
            self._tombstones.pop(key, None)

            self._remote_updated_at[key] = time.time()

            return "updated" if old is not None else "added"

    def apply_remote_batch(self, workers):
        """
        Apply incoming versioned records.

        Legacy gossip messages without versions are ignored
        by replication but can still be stored as snapshots.
        """

        outcomes = {}

        for worker in workers:
            if "version" not in worker:
                continue

            label = (
                worker["owner"],
                worker["worker_id"]
            )

            outcomes[label] = self.apply_remote(worker)

        return outcomes

    def export_gossip_records(self):
        """
        Export all known versioned records.

        Includes:
        - Locally owned workers
        - Replicated remote workers
        - Tombstones

        This enables multi-hop propagation.
        """

        with self._lock:
            result = list(self._local.values())
            result += list(self._remote.values())
            result += list(self._tombstones.values())

            return sorted(
                copy.deepcopy(result),
                key=lambda worker: (
                    worker["worker_id"],
                    worker["owner"]
                )
            )

    def replication_snapshot(self):
        """
        Return a snapshot of the versioned registry.

        Freshness affects diagnostic eligibility but does
        not erase retained version metadata.
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
                copy.deepcopy(record)
                for key, record in self._remote.items()
                if (
                    now - self._remote_updated_at.get(key, 0)
                    <= self.remote_ttl
                )
            ]

        for records in (
            local,
            remote,
            fresh,
            tombstones
        ):
            records.sort(
                key=lambda worker: (
                    worker["worker_id"],
                    worker["owner"]
                )
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
            "tombstones": tombstones
        }
