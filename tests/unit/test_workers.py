from datetime import datetime, timezone
import threading
import unittest

from weow_ml.acquisition.workers import AcquisitionWorkers


class Handler:
    def __init__(self, failures=0, release=None):
        self.failures = failures
        self.release = release
        self.calls = []

    def handle(self, payload, acquisition_timestamp):
        self.calls.append((payload, acquisition_timestamp))
        if self.release is not None:
            self.release.wait(2)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("temporary")
        return payload


class WorkersTests(unittest.TestCase):
    def setUp(self):
        self.availability = datetime(2026, 4, 15, 10, 25, tzinfo=timezone.utc)

    def workers(self, handler, **changes):
        settings = dict(
            workers=1,
            max_pending_notifications=0,
            retry_count=1,
            retry_admissible_age_seconds=10,
            retry_delay_seconds=0,
            clock=lambda: self.availability,
        )
        settings.update(changes)
        return AcquisitionWorkers(handler, **settings)

    def test_retry_reuses_original_acquisition_timestamp(self):
        handler = Handler(failures=1)
        with self.workers(handler) as workers:
            self.assertEqual(workers.submit({"id": 1}).result(2), {"id": 1})
        self.assertEqual(len(handler.calls), 2)
        self.assertEqual({call[1] for call in handler.calls}, {self.availability})

    def test_failure_after_retry_is_visible(self):
        handler = Handler(failures=2)
        with self.workers(handler) as workers:
            with self.assertRaisesRegex(RuntimeError, "temporary"):
                workers.submit({"id": 1}).result(2)
        self.assertEqual(len(handler.calls), 2)

    def test_queue_capacity_is_bounded(self):
        release = threading.Event()
        handler = Handler(release=release)
        with self.workers(handler) as workers:
            first = workers.submit({"id": 1})
            with self.assertRaisesRegex(TimeoutError, "queue is full"):
                workers.submit({"id": 2}, timeout=0.01)
            release.set()
            self.assertEqual(first.result(2), {"id": 1})


if __name__ == "__main__":
    unittest.main()
