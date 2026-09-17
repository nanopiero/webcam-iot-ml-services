from datetime import datetime, timezone
import json
import unittest

try:
    from weow_ml.acquisition.kafka import KafkaPublisher, ProcessingJob
except ModuleNotFoundError as exc:
    if exc.name != "confluent_kafka":
        raise
    KafkaPublisher = ProcessingJob = None


class Message:
    def topic(self):
        return "jobs"

    def partition(self):
        return 7

    def offset(self):
        return 42


class Producer:
    def __init__(self, error=None, deliver=True):
        self.error = error
        self.deliver = deliver
        self.pending = None
        self.produced = None

    def produce(self, topic, **kwargs):
        self.produced = (topic, kwargs)
        self.pending = kwargs["on_delivery"]

    def poll(self, timeout):
        if self.deliver and self.pending:
            callback, self.pending = self.pending, None
            callback(self.error, Message())

    def flush(self, timeout):
        return 0


@unittest.skipIf(ProcessingJob is None, "install requirements-kafka.txt")
class KafkaPublisherTests(unittest.TestCase):
    def job(self):
        return ProcessingJob(
            image_id="20260415T102500Z_fin123T0.jpg",
            processing_image="images/fin123T0/2026/04/15/10/image_P0S0V0.jpg",
            processing_stream_id="fin123T0_P0S0V0",
            kafka_partition=7,
            process_snow=True,
            process_visibility=False,
            acquisition_timestamp=datetime(2026, 4, 15, 10, 25, tzinfo=timezone.utc),
            solar_phase=1,
            sunrise_offset_seconds=3600.0,
            sunset_offset_seconds=-7200.0,
            image_signature=12.5,
            image_metadata={"derived_image": {"width": 400, "height": 224}},
        )

    def test_job_encoding_and_acknowledged_partition(self):
        producer = Producer()
        acknowledgement = KafkaPublisher("broker:9092", "jobs", producer=producer).publish(self.job())
        self.assertEqual((acknowledgement.partition, acknowledgement.offset), (7, 42))
        topic, arguments = producer.produced
        self.assertEqual(topic, "jobs")
        self.assertEqual(arguments["partition"], 7)
        document = json.loads(arguments["value"])
        self.assertEqual(document["schema_version"], "J0V0")
        self.assertEqual(document["processing_stream_id"], "fin123T0_P0S0V0")

    def test_delivery_failure_is_reported(self):
        publisher = KafkaPublisher("broker:9092", "jobs", producer=Producer(error="rejected"))
        with self.assertRaisesRegex(RuntimeError, "rejected"):
            publisher.publish(self.job())

    def test_acknowledgement_timeout_is_bounded(self):
        publisher = KafkaPublisher(
            "broker:9092", "jobs", timeout_seconds=0.01, producer=Producer(deliver=False)
        )
        with self.assertRaises(TimeoutError):
            publisher.publish(self.job())


if __name__ == "__main__":
    unittest.main()
