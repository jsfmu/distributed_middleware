import threading
import time


VALID_PEER_STATUSES = {"unknown", "healthy", "unavailable"}


class PeerRegistry:
    """
    Thread-safe registry of known middleware peers.

    Each peer stores:
        node_id: Unique middleware identifier
        host: Network address
        port: HTTP port
        status: unknown, healthy, or unavailable
        last_seen: Last successful communication timestamp
    """

    def __init__(self):
        self._peers = {}
        self._lock = threading.RLock()

    def add_or_update_peer(self, node_id, host, port):
        """
        Add a new middleware peer or update an existing one.

        New peers begin with unknown health status because
        knowing their address does not mean they are reachable.
        """

        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")

        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be a non-empty string")

        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("port must be between 1 and 65535")

        with self._lock:
            existing = self._peers.get(node_id)

            peer = {
                "node_id": node_id,
                "host": host,
                "port": port,
                "status": (
                    existing["status"] if existing else "unknown"
                ),
                "last_seen": (
                    existing["last_seen"] if existing else None
                ),
            }

            self._peers[node_id] = peer

            return peer.copy()

    def get_peer(self, node_id):
        """Retrieve one peer by node ID."""

        with self._lock:
            peer = self._peers.get(node_id)
            return peer.copy() if peer else None

    def get_all_peers(self):
        """Return a snapshot of all known peers."""

        with self._lock:
            return [
                self._peers[node_id].copy()
                for node_id in sorted(self._peers)
            ]

    def set_peer_status(self, node_id, status):
        """
        Update a peer's health status.

        Successful communication updates last_seen.
        Returns False if the peer does not exist.
        """

        if status not in VALID_PEER_STATUSES:
            raise ValueError(f"Invalid peer status: {status}")

        with self._lock:
            peer = self._peers.get(node_id)

            if peer is None:
                return False

            peer["status"] = status

            if status == "healthy":
                peer["last_seen"] = time.time()

            return True

    def remove_peer(self, node_id):
        """Remove a peer from the registry."""

        with self._lock:
            return self._peers.pop(node_id, None) is not None

    def get_peer_count(self):
        """Return the number of known peers."""

        with self._lock:
            return len(self._peers)