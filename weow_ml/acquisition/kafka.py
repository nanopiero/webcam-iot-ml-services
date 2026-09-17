"""Versioned processing-job contract and acknowledged Kafka publication."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import threading
import time

from confluent_kafka import Producer

from .contracts import identifier


class JobContractError(ValueError):
    """A processing job does not satisfy the J0V0 contract."""


@dataclass(frozen=True)
class ProcessingJob:
    image_id: str
    processing_image: str
    processing_stream_id: str
    kafka_partition: int
    process_snow: bool
    process_visibility: bool
    acquisition_timestamp: datetime
    solar_phase: int
    sunrise_offset_seconds: float
    sunset_offset_seconds: float
    image_signature: float
    image_metadata: dict

    def __post_init__(self):
        identifier(self.processing_stream_id, "processing_stream_id")
        if not self.image_id.endswith(".jpg"):
            raise JobContractError("image_id must name a JPEG image")
        if not self.processing_image.endswith(".jpg") or self.processing_image.startswith("/"):
            raise JobContractError("processing_image must be a relative JPEG path")
        if self.kafka_partition < 0:
            raise JobContractError("kafka_partition must be non-negative")
        if type(self.process_snow) is not bool or type(self.process_visibility) is not bool:
            raise JobContractError("parameter flags must be booleans")
        if self.acquisition_timestamp.tzinfo is None:
            raise JobContractError("acquisition_timestamp must include a timezone")
        if self.solar_phase not in (0, 1, 2):
            raise JobContractError("solar_phase must be 0, 1, or 2")
        for name, value in (
            ("sunrise_offset_seconds", self.sunrise_offset_seconds),
            ("sunset_offset_seconds", self.sunset_offset_seconds),
            ("image_signature", self.image_signature),
        ):
            if type(value) not in (int, float) or not math.isfinite(value):
                raise JobContractError(name + " must be finite")
        if not isinstance(self.image_metadata, dict):
            raise JobContractError("image_metadata must be an object")

    def document(self):
        return {
            "schema_version": "J0V0",
            "image_id": self.image_id,
            "processing_image": self.processing_image,
            "processing_stream_id": self.processing_stream_id,
            "kafka_partition": self.kafka_partition,
            "parameters": {
                "snow": self.process_snow,
                "visibility": self.process_visibility,
            },
            "acquisition_timestamp": self.acquisition_timestamp.astimezone(timezone.utc).isoformat(),
            "solar": {
                "phase": self.solar_phase,
                "sunrise_offset_seconds": self.sunrise_offset_seconds,
                "sunset_offset_seconds": self.sunset_offset_seconds,
            },
            "image_signature": self.image_signature,
            "image_metadata": self.image_metadata,
        }

    def encoded(self):
        try:
            return json.dumps(
                self.document(), sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise JobContractError("image_metadata must be JSON serializable") from exc


@dataclass(frozen=True)
class KafkaAcknowledgement:
    topic: str
    partition: int
    offset: int


class KafkaPublisher:
    def __init__(self, bootstrap_servers, topic, timeout_seconds=5, producer=None):
        if not bootstrap_servers or not topic or timeout_seconds <= 0:
            raise ValueError("Kafka endpoint, topic, and positive timeout are required")
        self.topic = topic
        self.timeout_seconds = timeout_seconds
        self.producer = producer or Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "weow-acquisition",
            "acks": "all",
            "enable.idempotence": True,
            "message.send.max.retries": 1,
            "message.timeout.ms": int(timeout_seconds * 1000),
            "socket.timeout.ms": int(timeout_seconds * 1000),
            "linger.ms": 5,
        })

    def publish(self, job):
        if not isinstance(job, ProcessingJob):
            raise TypeError("publish requires a ProcessingJob")
        completed = threading.Event()
        result = {}

        def delivered(error, message):
            result["error"] = error
            result["message"] = message
            completed.set()

        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=job.processing_stream_id.encode("utf-8"),
                    value=job.encoded(),
                    partition=job.kafka_partition,
                    on_delivery=delivered,
                )
                break
            except BufferError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Kafka producer queue remained full")
                self.producer.poll(min(0.1, remaining))

        while not completed.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Kafka acknowledgement timed out")
            self.producer.poll(min(0.1, remaining))
        if result["error"] is not None:
            raise RuntimeError("Kafka delivery failed: " + str(result["error"]))
        message = result["message"]
        return KafkaAcknowledgement(message.topic(), message.partition(), message.offset())

    def close(self, timeout_seconds=None):
        timeout = self.timeout_seconds if timeout_seconds is None else timeout_seconds
        remaining = self.producer.flush(timeout)
        if remaining:
            raise TimeoutError(f"{remaining} Kafka messages remain unacknowledged")
