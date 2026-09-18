"""Bounded-cardinality Prometheus metrics for Acquisition."""

from contextlib import contextmanager
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class AcquisitionMetrics:
    """Own one registry so service instances and tests cannot collide."""

    def __init__(self, monotonic=None):
        self.registry = CollectorRegistry()
        self.monotonic = monotonic or time.monotonic
        self.notifications = Counter(
            "weow_acquisition_notifications_total",
            "MQTT notifications observed by Acquisition.",
            ("outcome",), registry=self.registry,
        )
        self.images = Counter(
            "weow_acquisition_images_total",
            "Images reaching a terminal Acquisition outcome.",
            ("outcome",), registry=self.registry,
        )
        self.stage_seconds = Histogram(
            "weow_acquisition_stage_seconds",
            "Duration of a processing stage.",
            ("stage",), registry=self.registry,
        )
        self.stage_failures = Counter(
            "weow_acquisition_stage_failures_total",
            "Unsuccessful processing stage attempts.",
            ("stage",), registry=self.registry,
        )
        self.archived_bytes = Counter(
            "weow_acquisition_archived_bytes_total",
            "Source-image bytes successfully archived.",
            registry=self.registry,
        )
        self.jobs = Counter(
            "weow_acquisition_jobs_published_total",
            "Kafka processing jobs successfully published.",
            registry=self.registry,
        )
        self.queue = Gauge(
            "weow_acquisition_worker_queue",
            "Notifications waiting for an Acquisition worker.",
            registry=self.registry,
        )
        self.active_workers = Gauge(
            "weow_acquisition_active_workers",
            "Acquisition workers currently handling a notification.",
            registry=self.registry,
        )
        self.queue_wait_seconds = Histogram(
            "weow_acquisition_worker_queue_wait_seconds",
            "Time a notification waits before an Acquisition worker starts it.",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
                     5, 10, 30, 60, 120, 300), registry=self.registry,
        )
        self.completion_seconds = Histogram(
            "weow_acquisition_notification_completion_seconds",
            "MQTT receipt through successful terminal handling and acknowledgement.",
            ("outcome",),
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
                     30, 60, 120, 300), registry=self.registry,
        )
        self.ready = Gauge(
            "weow_acquisition_ready",
            "Whether Acquisition has completed startup and is accepting MQTT work.",
            registry=self.registry,
        )

    def notification(self, outcome):
        self.notifications.labels(outcome=outcome).inc()

    @contextmanager
    def stage(self, name):
        started = self.monotonic()
        try:
            yield
        except BaseException:
            self.stage_failures.labels(stage=name).inc()
            raise
        finally:
            self.stage_seconds.labels(stage=name).observe(self.monotonic() - started)

    def handled(self, result):
        self.images.labels(outcome=result.status).inc()
        if result.archived:
            self.archived_bytes.inc(result.archived_bytes)
        if result.published_jobs:
            self.jobs.inc(result.published_jobs)

    def completed(self, result, elapsed_seconds):
        self.completion_seconds.labels(outcome=result.status).observe(elapsed_seconds)
