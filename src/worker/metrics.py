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