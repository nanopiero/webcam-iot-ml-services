from types import SimpleNamespace
import unittest

from prometheus_client import generate_latest

from weow_ml.acquisition.metrics import AcquisitionMetrics


class MetricsTests(unittest.TestCase):
    def test_terminal_result_records_counts_and_bytes(self):
        metrics = AcquisitionMetrics()
        metrics.notification("received")
        metrics.notification("completed")
        metrics.handled(SimpleNamespace(
            status="published", archived=True, archived_bytes=1234, published_jobs=2,
        ))
        metrics.completed(SimpleNamespace(status="published"), 0.75)

        exposition = generate_latest(metrics.registry).decode()
        self.assertIn(
            'weow_acquisition_notifications_total{outcome="received"} 1.0', exposition,
        )
        self.assertIn(
            'weow_acquisition_images_total{outcome="published"} 1.0', exposition,
        )
        self.assertIn("weow_acquisition_archived_bytes_total 1234.0", exposition)
        self.assertIn("weow_acquisition_jobs_published_total 2.0", exposition)
        self.assertIn(
            'weow_acquisition_notification_completion_seconds_sum{outcome="published"} 0.75',
            exposition,
        )

    def test_stage_failure_is_counted_and_propagated(self):
        times = iter((10.0, 10.25))
        metrics = AcquisitionMetrics(monotonic=lambda: next(times))

        with self.assertRaisesRegex(RuntimeError, "failed"):
            with metrics.stage("archive"):
                raise RuntimeError("failed")

        exposition = generate_latest(metrics.registry).decode()
        self.assertIn(
            'weow_acquisition_stage_failures_total{stage="archive"} 1.0', exposition,
        )
        self.assertIn(
            'weow_acquisition_stage_seconds_sum{stage="archive"} 0.25', exposition,
        )


if __name__ == "__main__":
    unittest.main()
