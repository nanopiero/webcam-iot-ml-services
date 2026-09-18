from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

import paho.mqtt.client as mqtt

from weow_ml.benchmark.images import load_profiles
from weow_ml.benchmark.publish import publish_workload, validate_spool_receipt
from weow_ml.benchmark.workload import SCENARIOS, Workload


ROOT = Path(__file__).parents[2]


class Publication:
    rc = mqtt.MQTT_ERR_SUCCESS

    def __init__(self):
        self.waited = False

    def wait_for_publish(self, timeout):
        self.waited = True

    def is_published(self):
        return self.waited


class Client:
    def __init__(self):
        self.messages = []

    def publish(self, topic, payload, qos, retain):
        publication = Publication()
        self.messages.append((topic, json.loads(payload), qos, retain, publication))
        return publication


class Clock:
    def __init__(self):
        self.value = 100.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


class BenchmarkPublishTests(unittest.TestCase):
    def test_smoke_workload_is_paced_and_acknowledged(self):
        workload = Workload(
            json.loads((ROOT / "tests/fixtures/notification_n0v0.json").read_text()),
            SCENARIOS["smoke"], "run_001",
            datetime(2026, 9, 19, 10, tzinfo=timezone.utc),
            load_profiles(json.loads((ROOT / "benchmarks/image_profiles.json").read_text())),
            spool_bucket="benchmark-spool",
        )
        client, clock = Client(), Clock()
        result = publish_workload(
            client, "weow/benchmark/N0V0", workload, max_inflight=3,
            monotonic=clock.monotonic, sleep=clock.sleep,
        )
        self.assertEqual(result["published"], 12)
        self.assertEqual(len(client.messages), 12)
        self.assertTrue(all(message[2:4] == (1, False) for message in client.messages))
        self.assertTrue(all(message[4].waited for message in client.messages))
        self.assertGreaterEqual(result["elapsed_seconds"], 11)

    def test_receipt_must_match_exact_workload(self):
        workload = Workload(
            json.loads((ROOT / "tests/fixtures/notification_n0v0.json").read_text()),
            SCENARIOS["smoke"], "run_001",
            datetime(2026, 9, 19, 10, tzinfo=timezone.utc),
            load_profiles(json.loads((ROOT / "benchmarks/image_profiles.json").read_text())),
            spool_bucket="benchmark-spool",
        )
        receipt = {
            "run_id": "run_001", "scenario": "smoke",
            "seed": 1,
            "start": "2026-09-19T10:00:00+00:00", "bucket": "benchmark-spool",
            "objects": 12, "spool_prefix": "benchmarks/wp1_5/run_001/spool/",
        }
        validate_spool_receipt(receipt, workload, "benchmark-spool")
        receipt["run_id"] = "another_run"
        with self.assertRaises(ValueError):
            validate_spool_receipt(receipt, workload, "benchmark-spool")


if __name__ == "__main__":
    unittest.main()
