"""Acknowledged publication check against an isolated temporary Kafka topic."""

from datetime import datetime, timezone
import os
import unittest
import uuid


RUN_KAFKA_TESTS = os.environ.get("WEOW_RUN_KAFKA_INTEGRATION") == "1"

if RUN_KAFKA_TESTS:
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient, NewTopic

    from weow_ml.acquisition.kafka import KafkaPublisher, ProcessingJob


@unittest.skipUnless(
    RUN_KAFKA_TESTS,
    "set WEOW_RUN_KAFKA_INTEGRATION=1 to use the development Kafka broker",
)
class KafkaBrokerTests(unittest.TestCase):
    bootstrap_servers = "192.168.1.135:9092"

    def setUp(self):
        self.topic = "weow.wp13.test." + uuid.uuid4().hex
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        future = self.admin.create_topics([NewTopic(self.topic, 1, 1)])[self.topic]
        future.result(10)

    def tearDown(self):
        self.admin.delete_topics([self.topic], operation_timeout=10)[self.topic].result(10)

    def test_publish_acknowledge_and_consume_exact_job(self):
        job = ProcessingJob(
            image_id="20260415T102500Z_fin123T0.jpg",
            processing_image="images/fin123T0/2026/04/15/10/image_P0S0V0.jpg",
            processing_stream_id="fin123T0_P0S0V0",
            kafka_partition=0,
            process_snow=True,
            process_visibility=True,
            acquisition_timestamp=datetime(2026, 4, 15, 10, 25, tzinfo=timezone.utc),
            solar_phase=1,
            sunrise_offset_seconds=3600.0,
            sunset_offset_seconds=-7200.0,
            image_signature=12.5,
            image_metadata={"derived_image": {"width": 400, "height": 224}},
        )
        publisher = KafkaPublisher(self.bootstrap_servers, self.topic, timeout_seconds=10)
        acknowledgement = publisher.publish(job)
        publisher.close()
        self.assertEqual(acknowledgement.partition, 0)

        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "weow-wp13-test-" + uuid.uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        try:
            consumer.subscribe([self.topic])
            message = consumer.poll(10)
            self.assertIsNotNone(message)
            self.assertIsNone(message.error())
            self.assertEqual(message.key(), job.processing_stream_id.encode("utf-8"))
            self.assertEqual(message.value(), job.encoded())
        finally:
            consumer.close()


if __name__ == "__main__":
    unittest.main()
