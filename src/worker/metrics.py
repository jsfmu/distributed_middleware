import threading
from contextlib import contextmanager
from typing import Optional

class WorkerMetrics:
    def __init__(self, capacity: Optional[int] = None):
        self.capacity = capacity
        self._lock = threading.Lock()
        self._active = 0
        self._peak = 0
        self._completed = 0

    def increment(self)->int:
        with self._lock:
            self._active += 1
            self._peak = max(self._peak, self._active)
            return self._active

    def decrement(self) -> int:
        with self._lock:
            if self._active == 0:
                raise RuntimeError("decrement() called with no active requests")
            self._active -= 1
            self._completed += 1
            return self._active

    def snapshot(self)->dict:
        with self._lock:
            available = None
            if self.capacity is not None: available = max(self.capacity - self._active, 0)

            return {"active_requests": self._active,
                    "peak_load": self._peak,
                    "completed_requests": self._completed,
                    "capacityy": self.capacity,
                    "available_slots": available,
            }

    @contextmanager
    def track_request(self):
        self.increment()
        try:
            yield
        finally:
            self.decrement()

worker_metrics = WorkerMetrics()