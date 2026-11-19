
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import middleware
from peers import PeerRegistry


def mock_response(payload=None):
    response = Mock()
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


class TestPeerRegistry(unittest.TestCase):

    def setUp(self):
        self.registry = PeerRegistry()

    def test_add_and_lookup_peer(self):
        peer = self.registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8001
        )

        self.assertEqual(peer["node_id"], "middleware-2")
        self.assertEqual(peer["status"], "unknown")
        self.assertIsNone(peer["last_seen"])
        self.assertEqual(self.registry.get_peer_count(), 1)

    def test_duplicate_registration_updates_existing_peer(self):
        self.registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8001
        )

        self.registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8101
        )

        self.assertEqual(self.registry.get_peer_count(), 1)
        self.assertEqual(
            self.registry.get_peer("middleware-2")["port"], 8101
        )

    def test_peer_health_status(self):
        self.registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8001
        )

        self.assertTrue(
            self.registry.set_peer_status("middleware-2", "healthy")
        )

        peer = self.registry.get_peer("middleware-2")
        self.assertEqual(peer["status"], "healthy")
        self.assertIsNotNone(peer["last_seen"])

        self.registry.set_peer_status("middleware-2", "unavailable")

        self.assertEqual(
            self.registry.get_peer("middleware-2")["status"],
            "unavailable"
        )

    def test_snapshot_and_removal(self):
        self.registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8001
        )

        snapshot = self.registry.get_all_peers()
        snapshot[0]["status"] = "healthy"

        self.assertEqual(
            self.registry.get_peer("middleware-2")["status"],
            "unknown"
        )

        self.assertTrue(self.registry.remove_peer("middleware-2"))
        self.assertFalse(self.registry.remove_peer("middleware-2"))
        self.assertEqual(self.registry.get_peer_count(), 0)

    def test_invalid_peer_configuration(self):
        invalid = [
            ("", "127.0.0.1", 8001),
            ("middleware-2", "", 8001),
            ("middleware-2", "127.0.0.1", 0),
            ("middleware-2", "127.0.0.1", 70000),
            ("middleware-2", "127.0.0.1", "8001"),
        ]

        for node_id, host, port in invalid:
            with self.subTest(node_id=node_id, port=port):
                with self.assertRaises(ValueError):
                    self.registry.add_or_update_peer(
                        node_id, host, port
                    )


class IsolatedMiddlewareTest(unittest.TestCase):
    """
    Isolate middleware global state so tests do not affect
    the existing routing regression tests.
    """

    def setUp(self):
        self.original_node_id = middleware.NODE_ID
        self.original_peer_registry = middleware.peer_registry

        middleware.NODE_ID = "middleware-1"
        middleware.peer_registry = PeerRegistry()

        self.client = middleware.app.test_client()

    def tearDown(self):
        middleware.NODE_ID = self.original_node_id
        middleware.peer_registry = self.original_peer_registry


class TestPeerEndpoints(IsolatedMiddlewareTest):

    def test_join_and_list_peers(self):
        response = self.client.post("/peer/join", json={
            "node_id": "middleware-2",
            "host": "127.0.0.1",
            "port": 8001
        })

        self.assertEqual(response.status_code, 200)

        response = self.client.get("/peers")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["node_id"], "middleware-1")
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["peers"][0]["node_id"], "middleware-2")
        self.assertEqual(data["peers"][0]["status"], "unknown")

    def test_duplicate_join_does_not_duplicate_peer(self):
        payload = {
            "node_id": "middleware-2",
            "host": "127.0.0.1",
            "port": 8001
        }

        self.client.post("/peer/join", json=payload)
        self.client.post("/peer/join", json=payload)

        response = self.client.get("/peers")
        self.assertEqual(response.get_json()["count"], 1)

    def test_invalid_join_requests(self):
        invalid_payloads = [
            {},
            {"node_id": "middleware-2"},
            {
                "node_id": "middleware-1",
                "host": "127.0.0.1",
                "port": 8000
            },
            {
                "node_id": "middleware-2",
                "host": "127.0.0.1",
                "port": -1
            },
            []
        ]

        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                response = self.client.post(
                    "/peer/join", json=payload
                )
                self.assertEqual(response.status_code, 400)

        self.assertEqual(
            middleware.peer_registry.get_peer_count(), 0
        )

    def test_seed_file_excludes_local_node(self):
        entries = [
            {
                "node_id": "middleware-1",
                "host": "127.0.0.1",
                "port": 8000
            },
            {
                "node_id": "middleware-2",
                "host": "127.0.0.1",
                "port": 8001
            }
        ]

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "peers.json"
            path.write_text(json.dumps(entries), encoding="utf-8")

            count = middleware.load_seed_peers(str(path))

        self.assertEqual(count, 1)
        self.assertIsNone(
            middleware.peer_registry.get_peer("middleware-1")
        )
        self.assertIsNotNone(
            middleware.peer_registry.get_peer("middleware-2")
        )


class TestPeerDiscovery(IsolatedMiddlewareTest):

    def setUp(self):
        super().setUp()

        middleware.peer_registry.add_or_update_peer(
            "middleware-2", "127.0.0.1", 8001
        )

    def test_successful_discovery_learns_new_peer(self):
        remote_data = {
            "node_id": "middleware-2",
            "peers": [
                {
                    "node_id": "middleware-1",
                    "host": "127.0.0.1",
                    "port": 8000
                },
                {
                    "node_id": "middleware-3",
                    "host": "127.0.0.1",
                    "port": 8002
                }
            ]
        }

        with patch.object(
            middleware.requests,
            "post",
            return_value=mock_response()
        ) as post:
            with patch.object(
                middleware.requests,
                "get",
                return_value=mock_response(remote_data)
            ) as get:
                middleware.discover_peers_once("127.0.0.1", 8000)

        post.assert_called_once()
        get.assert_called_once()

        announcement = post.call_args.kwargs["json"]
        self.assertEqual(announcement["node_id"], "middleware-1")
        self.assertEqual(announcement["port"], 8000)

        self.assertEqual(
            middleware.peer_registry.get_peer("middleware-2")["status"],
            "healthy"
        )

        self.assertIsNotNone(
            middleware.peer_registry.get_peer("middleware-3")
        )

        self.assertEqual(
            middleware.peer_registry.get_peer("middleware-3")["status"],
            "unknown"
        )

        self.assertIsNone(
            middleware.peer_registry.get_peer("middleware-1")
        )

    def test_unavailable_peer_recovers(self):
        with patch.object(
            middleware.requests,
            "post",
            side_effect=requests.ConnectionError("Connection refused")
        ):
            middleware.discover_peers_once("127.0.0.1", 8000)

        self.assertEqual(
            middleware.peer_registry.get_peer("middleware-2")["status"],
            "unavailable"
        )

        remote_data = {
            "node_id": "middleware-2",
            "peers": []
        }

        with patch.object(
            middleware.requests,
            "post",
            return_value=mock_response()
        ):
            with patch.object(
                middleware.requests,
                "get",
                return_value=mock_response(remote_data)
            ):
                middleware.discover_peers_once("127.0.0.1", 8000)

        peer = middleware.peer_registry.get_peer("middleware-2")

        self.assertEqual(peer["status"], "healthy")
        self.assertIsNotNone(peer["last_seen"])

    def test_mismatched_peer_identity_is_rejected(self):
        remote_data = {
            "node_id": "middleware-3",
            "peers": []
        }

        with patch.object(
            middleware.requests,
            "post",
            return_value=mock_response()
        ):
            with patch.object(
                middleware.requests,
                "get",
                return_value=mock_response(remote_data)
            ):
                middleware.discover_peers_once("127.0.0.1", 8000)

        self.assertEqual(
            middleware.peer_registry.get_peer("middleware-2")["status"],
            "unavailable"
        )


if __name__ == "__main__":
    unittest.main()
