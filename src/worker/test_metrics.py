import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from metrics import WorkerMetrics


class TestWorkerMetrics(unittest.TestCase):
    def setUp(self):
        # A fresh instance per test, never the shared worker_metrics
        self.m = WorkerMetrics()

    def test_increment_and_decrement(self):
        self.m.increment()
        self.m.increment()
        self.assertEqual(self.m.get_load(), 2)

        self.m.decrement()
        self.m.decrement()
        self.assertEqual(self.m.get_load(), 0)
        self.assertEqual(self.m.snapshot()["completed_requests"], 2)

    def test_decrement_below_zero_raises(self):
        with self.assertRaises(RuntimeError):
            self.m.decrement()
        self.assertEqual(self.m.get_load(), 0)

    def test_track_request_decrements_on_exception(self):
        with self.assertRaises(ValueError):
            with self.m.track_request():
                self.assertEqual(self.m.get_load(), 1)
                raise ValueError("inference failed")

        self.assertEqual(self.m.get_load(), 0)

    def test_many_threads_end_at_zero(self):
        threads = 50
        iterations = 1000

        def run_requests():
            for _ in range(iterations):
                with self.m.track_request():
                    pass

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(run_requests) for _ in range(threads)]
            for future in futures:
                future.result()  # re-raises any exception from a thread

        snap = self.m.snapshot()
        self.assertEqual(snap["active_requests"], 0)
        self.assertEqual(snap["completed_requests"], threads * iterations)

    def test_load_reaches_n_when_n_requests_overlap(self):
        n = 20
        all_inside = threading.Barrier(n + 1)  # n request threads + this one
        release = threading.Event()

        def request():
            with self.m.track_request():
                all_inside.wait(timeout=5)
                release.wait(timeout=5)

        threads = [threading.Thread(target=request) for _ in range(n)]
        for t in threads:
            t.start()

        try:
            all_inside.wait(timeout=5)  # every thread is now inside
            self.assertEqual(self.m.get_load(), n)
        finally:
            release.set()
            for t in threads:
                t.join()

        self.assertEqual(self.m.get_load(), 0)
        self.assertEqual(self.m.snapshot()["peak_load"], n)

    def test_snapshot_reports_capacity(self):
        m = WorkerMetrics(capacity=4)
        m.increment()

        snap = m.snapshot()
        self.assertEqual(snap["capacity"], 4)
        self.assertEqual(snap["available_slots"], 3)

    # Only if you did Step 6b
    def test_try_acquire_never_exceeds_capacity(self):
        m = WorkerMetrics(capacity=5)
        n = 50
        start = threading.Barrier(n)
        results = []

        def attempt():
            start.wait(timeout=5)  # all threads try at the same moment
            results.append(m.try_acquire())

        threads = [threading.Thread(target=attempt) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(results.count(True), 5)
        self.assertEqual(m.get_load(), 5)


if __name__ == "__main__":
    unittest.main()