"""Bounded Acquisition worker pool with the accepted short retry policy."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import threading
import time


class AcquisitionWorkers:
    """Run notification handlers without an unbounded executor queue."""

    def __init__(self, handler, workers, max_pending_notifications, retry_count,
                 retry_admissible_age_seconds, retry_delay_seconds=0.25,
                 clock=None, monotonic=None, sleep=None, metrics=None):
        if type(workers) is not int or workers <= 0:
            raise ValueError("workers must be a positive integer")
        if type(max_pending_notifications) is not int or max_pending_notifications < 0:
            raise ValueError("max_pending_notifications must be a non-negative integer")
        if type(retry_count) is not int or retry_count < 0:
            raise ValueError("retry_count must be a non-negative integer")
        if retry_admissible_age_seconds < 0 or retry_delay_seconds < 0:
            raise ValueError("retry durations must be non-negative")
        self.handler = handler
        self.retry_count = retry_count
        self.retry_admissible_age_seconds = retry_admissible_age_seconds
        self.retry_delay_seconds = retry_delay_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic or time.monotonic
        self.sleep = sleep or time.sleep
        self.metrics = metrics
        self._executor = ThreadPoolExecutor(max_workers=workers,
                                            thread_name_prefix="acquisition")
        # Running tasks plus configured pending notifications are admitted.
        self._capacity = threading.BoundedSemaphore(workers + max_pending_notifications)
        self._closed = False
        self._lock = threading.Lock()

    def submit(self, payload, timeout=None):
        """Block at the admission boundary instead of growing an internal queue."""
        with self._lock:
            if self._closed:
                raise RuntimeError("worker pool is closed")
        if not self._capacity.acquire(timeout=timeout):
            raise TimeoutError("acquisition worker queue is full")
        if self.metrics is not None:
            self.metrics.queue.inc()
        try:
            availability = self.clock()
            started = self.monotonic()
            future = self._executor.submit(self._run, payload, availability, started)
        except BaseException:
            if self.metrics is not None:
                self.metrics.queue.dec()
            self._capacity.release()
            raise
        future.add_done_callback(lambda completed: self._capacity.release())
        return future

    def _run(self, payload, availability, started):
        if self.metrics is not None:
            self.metrics.queue.dec()
            self.metrics.active_workers.inc()
            self.metrics.queue_wait_seconds.observe(self.monotonic() - started)
        failures = 0
        try:
            while True:
                try:
                    return self.handler.handle(payload, acquisition_timestamp=availability)
                except Exception:
                    if (failures >= self.retry_count
                            or self.monotonic() - started > self.retry_admissible_age_seconds):
                        raise
                    failures += 1
                    if self.retry_delay_seconds:
                        self.sleep(self.retry_delay_seconds)
        finally:
            if self.metrics is not None:
                self.metrics.active_workers.dec()

    def close(self, wait=True):
        with self._lock:
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=not wait)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
