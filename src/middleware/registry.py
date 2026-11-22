
import copy
import threading


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
