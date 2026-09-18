"""Ordered handling of one valid Acquisition notification."""

from dataclasses import dataclass
from datetime import datetime, timezone
import shutil

from .archive import archive_image, fetch_image
from .contracts import parse_notification
from .images import prepare_processing_images, publish_processing_images
from .kafka import ProcessingJob
from .policies import acquisition_decision, classify_solar


@dataclass(frozen=True)
class HandlingResult:
    image_id: str
    status: str
    archived: bool
    published_jobs: int
    archived_bytes: int = 0


class NotificationHandler:
    """Coordinate side effects in the architecture's required order.

    Dependencies are long-lived and supplied by the runnable service. The
    initial-state publisher must be idempotent for an existing stream.
    """

    def __init__(self, registry, spool, archive, archive_bucket, nfs_root,
                 state_publisher, kafka_publisher, timestamp_fields,
                 default_timestamp_field, transition_margin_seconds, clock=None,
                 metrics=None, output_prefix="", notification_guard=None,
                 minimum_nfs_free_bytes=0):
        self.registry = registry
        self.spool = spool
        self.archive = archive
        self.archive_bucket = archive_bucket
        self.nfs_root = nfs_root
        self.state_publisher = state_publisher
        self.kafka_publisher = kafka_publisher
        self.timestamp_fields = dict(timestamp_fields)
        self.default_timestamp_field = default_timestamp_field
        self.transition_margin_seconds = transition_margin_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.metrics = metrics
        self.output_prefix = output_prefix
        self.notification_guard = notification_guard
        self.minimum_nfs_free_bytes = minimum_nfs_free_bytes

    def _stage(self, name):
        if self.metrics is None:
            from contextlib import nullcontext
            return nullcontext()
        return self.metrics.stage(name)

    def handle(self, payload, acquisition_timestamp=None):
        acquisition_timestamp = acquisition_timestamp or self.clock()
        if acquisition_timestamp.tzinfo is None:
            raise ValueError("acquisition timestamp must include a timezone")
        acquisition_timestamp = acquisition_timestamp.astimezone(timezone.utc)
        notification = parse_notification(payload)
        if self.notification_guard is not None:
            self.notification_guard(notification)
        with self._stage("registry"):
            resolution = self.registry.resolve(payload)
        if resolution.status == "blacklist":
            return HandlingResult(notification.image_id, "blacklisted", False, 0)

        with self._stage("solar"):
            solar = classify_solar(
                notification,
                self.timestamp_fields,
                self.default_timestamp_field,
                self.transition_margin_seconds,
            )
        decision = acquisition_decision(resolution.status, solar.phase)
        with self._stage("download"):
            image = fetch_image(self.spool, notification)
        with self._stage("archive"):
            archive_image(self.archive, self.archive_bucket, notification, image, {
                "mode": "live",
                "acquisition_timestamp": acquisition_timestamp.isoformat(),
                "processing_stream_ids": [s.processing_stream_id for s in resolution.streams],
                "processing_context_status": "resolved",
                "solar": {
                    "phase": solar.phase,
                    "reference_timestamp": solar.reference_timestamp.isoformat(),
                    "sunrise_offset_seconds": solar.sunrise_offset_seconds,
                    "sunset_offset_seconds": solar.sunset_offset_seconds,
                },
            }, key_prefix=self.output_prefix)
        if not decision.publish_jobs:
            return HandlingResult(notification.image_id, decision.reason, True, 0, len(image))

        if (self.minimum_nfs_free_bytes
                and shutil.disk_usage(self.nfs_root).free < self.minimum_nfs_free_bytes):
            raise OSError("NFS free space is below the configured stop threshold")

        with self._stage("image_preparation"):
            prepared = prepare_processing_images(
                image, notification, resolution.streams, self.output_prefix
            )
        with self._stage("nfs_publish"):
            publish_processing_images(self.nfs_root, prepared)
        by_stream = {item.processing_stream_id: item for item in prepared}
        with self._stage("state_initialization"):
            for stream in resolution.streams:
                self.state_publisher.ensure(
                    stream.processing_stream_id,
                    by_stream[stream.processing_stream_id].relative_path,
                )

        published = 0
        image_metadata = {
            "timestamps": notification.payload["timestamps"],
            "source_image": notification.payload["source_image"],
            "derived_image": notification.payload["derived_image"],
        }
        with self._stage("kafka_publish"):
            for stream in resolution.streams:
                job = ProcessingJob(
                    image_id=notification.image_id,
                    processing_image=by_stream[stream.processing_stream_id].relative_path,
                    processing_stream_id=stream.processing_stream_id,
                    kafka_partition=stream.kafka_partition,
                    process_snow=stream.process_snow,
                    process_visibility=stream.process_visibility,
                    acquisition_timestamp=acquisition_timestamp,
                    solar_phase=solar.phase,
                    sunrise_offset_seconds=solar.sunrise_offset_seconds,
                    sunset_offset_seconds=solar.sunset_offset_seconds,
                    image_signature=notification.image_signature,
                    image_metadata=image_metadata,
                )
                self.kafka_publisher.publish(job)
                published += 1
        return HandlingResult(notification.image_id, "published", True, published, len(image))
