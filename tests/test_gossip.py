import socket
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from flask import Flask, jsonify, request
from werkzeug.serving import make_server


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "middleware"))

import gossip
from peers import PeerRegistry


FAKE_STATE = {
    "workers": [
        {
            "worker_id": "worker-1",
            "host": "127.0.0.1",
            "port": 9001,
            "models": ["model-a"],
            "status": "healthy",
            "owner": "middleware-1"
        }
    ]
}


def fake_state():
    return FAKE_STATE


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


PEER = {
    "node_id": "middleware-2",
    "host": "127.0.0.1",
    "port": 8001,
    "status": "healthy"
}


class TestPeerSelection(unittest.TestCase):

    def test_no_peers(self):
        self.assertEqual(gossip.select_gossip_peers([]), [])

    def test_prefers_reachable_peers(self):
        peers = [
            {"node_id": "a", "status": "unavailable"},
            {"node_id": "b", "status": "healthy"},
            {"node_id": "c", "status": "unknown"},
        ]

        for _ in range(50):
            chosen = gossip.select_gossip_peers(peers, fanout=1)
            self.assertIn(chosen[0]["node_id"], {"b", "c"})

    def test_falls_back_when_all_unavailable(self):
        peers = [{"node_id": "a", "status": "unavailable"}]

        chosen = gossip.select_gossip_peers(peers)

        self.assertEqual(chosen[0]["node_id"], "a")

    def test_fanout_is_capped_and_distinct(self):
        peers = [
            {"node_id": str(i), "status": "healthy"} for i in range(3)
        ]

        self.assertEqual(
            len(gossip.select_gossip_peers(peers, fanout=10)), 3
        )

        chosen = gossip.select_gossip_peers(peers, fanout=2)
        self.assertEqual(len({p["node_id"] for p in chosen}), 2)


class TestGossipMessage(unittest.TestCase):

    def test_message_shape(self):
        message = gossip.build_gossip_message("middleware-1", FAKE_STATE)

        self.assertEqual(message["sender_id"], "middleware-1")
        self.assertEqual(message["workers"], FAKE_STATE["workers"])

    def test_bad_state_gives_empty_workers(self):
        message = gossip.build_gossip_message("middleware-1", None)

        self.assertEqual(message["workers"], [])


class TestSendGossip(unittest.TestCase):

    @patch("gossip.requests.post")
    def test_success(self, post):
        post.return_value = Mock(ok=True, status_code=200)

        outcome, error = gossip.send_gossip(PEER, {"sender_id": "m"})

        self.assertEqual(outcome, "ok")
        self.assertIsNone(error)
        self.assertEqual(
            post.call_args.args[0], "http://127.0.0.1:8001/gossip"
        )

    @patch("gossip.time.sleep")
    @patch(
        "gossip.requests.post",
        side_effect=requests.ConnectionError("refused")
    )
    def test_unreachable_is_retried(self, post, sleep):
        outcome, _ = gossip.send_gossip(PEER, {}, retries=2)

        self.assertEqual(outcome, "unreachable")
        self.assertEqual(post.call_count, 3)

    @patch("gossip.requests.post")
    def test_http_error_is_not_retried(self, post):
        post.return_value = Mock(ok=False, status_code=404)

        outcome, error = gossip.send_gossip(PEER, {})

        self.assertEqual(outcome, "rejected")
        self.assertEqual(error, "HTTP 404")
        self.assertEqual(post.call_count, 1)


class TestGossipRound(unittest.TestCase):

    def setUp(self):
        self.peers = PeerRegistry()
        self.peers.add_or_update_peer("middleware-2", "127.0.0.1", 8001)

    def test_no_peers_does_nothing(self):
        self.assertEqual(
            gossip.gossip_once("middleware-1", PeerRegistry(), fake_state),
            {}
        )

    @patch("gossip.requests.post")
    def test_success_marks_peer_healthy(self, post):
        post.return_value = Mock(ok=True, status_code=200)

        results = gossip.gossip_once("middleware-1", self.peers, fake_state)

        self.assertEqual(results, {"middleware-2": "ok"})
        self.assertEqual(
            self.peers.get_peer("middleware-2")["status"], "healthy"
        )
        self.assertEqual(
            post.call_args.kwargs["json"]["sender_id"], "middleware-1"
        )

    @patch("gossip.time.sleep")
    @patch(
        "gossip.requests.post",
        side_effect=requests.ConnectionError("refused")
    )
    def test_unreachable_peer_marked_unavailable(self, post, sleep):
        results = gossip.gossip_once("middleware-1", self.peers, fake_state)

        self.assertEqual(results, {"middleware-2": "unreachable"})
        self.assertEqual(
            self.peers.get_peer("middleware-2")["status"], "unavailable"
        )


class TestGossipLoop(unittest.TestCase):

    def test_loop_survives_errors_and_stops(self):
        peers = PeerRegistry()
        peers.add_or_update_peer("middleware-2", "127.0.0.1", 8001)
        calls = []

        def broken_state():
            calls.append(1)
            raise RuntimeError("boom")

        thread, stop = gossip.start_gossip(
            "middleware-1", peers, broken_state, interval=0.05
        )

        time.sleep(0.3)

        self.assertTrue(thread.is_alive())
        self.assertGreaterEqual(len(calls), 2)

        stop.set()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())


class TestTemporaryEndpoint(unittest.TestCase):
    """Send real HTTP gossip to a throwaway Flask server."""

    def setUp(self):
        self.received = []
        app = Flask("fake-peer")

        @app.post("/gossip")
        def receive():
            self.received.append(request.get_json())
            return jsonify({"message": "gossip received"})

        self.server = make_server("127.0.0.1", 0, app)
        threading.Thread(
            target=self.server.serve_forever, daemon=True
        ).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_payload_delivered(self):
        peers = PeerRegistry()
        peers.add_or_update_peer(
            "middleware-2", "127.0.0.1", self.server.server_port
        )

        results = gossip.gossip_once("middleware-1", peers, fake_state)

        self.assertEqual(results, {"middleware-2": "ok"})
        self.assertEqual(
            self.received,
            [{"sender_id": "middleware-1", **FAKE_STATE}]
        )

    def test_real_unreachable_peer_does_not_crash(self):
        peers = PeerRegistry()
        peers.add_or_update_peer("middleware-3", "127.0.0.1", free_port())

        results = gossip.gossip_once(
            "middleware-1", peers, fake_state, retries=0
        )

        self.assertEqual(results, {"middleware-3": "unreachable"})


if __name__ == "__main__":
    unittest.main()
