from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

from weow_ml.acquisition.contracts import ContractError, parse_notification
from weow_ml.benchmark.guard import BenchmarkGuard, validate_benchmark_settings
from weow_ml.benchmark.images import load_profiles
from weow_ml.benchmark.workload import SCENARIOS, Workload


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"
PROFILES = Path(__file__).parents[2] / "benchmarks" / "image_profiles.json"


class BenchmarkGuardTests(unittest.TestCase):
    def settings(self):
        return {
            "benchmark": {"enabled": True, "run_id": "run_001"},
            "spool_s3": {"bucket": "REQUIRED_BENCHMARK_SPOOL_BUCKET"},
            "mqtt": {
                "topic": "weow/benchmark/N0V0",
                "client_id": "weow-benchmark-run_001",
            },
            "kafka": {"topic": "inference.jobs.benchmark"},
            "acquisition": {"output_prefix": "benchmarks/wp1_5/run_001"},
        }

    def notification(self):
        workload = Workload(
            json.loads(FIXTURE.read_text()), SCENARIOS["smoke"], "run_001",
            datetime(2026, 4, 15, 10, tzinfo=timezone.utc),
            load_profiles(json.loads(PROFILES.read_text())), seed=1,
        )
        return parse_notification(next(workload.events()))

    def test_matching_settings_and_notification_are_accepted(self):
        guard = validate_benchmark_settings(
            self.settings(), {"dbname": "weow_ml_benchmark"}
        )
        self.assertIsInstance(guard, BenchmarkGuard)
        guard(self.notification())

    def test_each_writable_target_is_fail_closed(self):
        cases = (
            ("mqtt", "topic", "webcam/T0"),
            ("mqtt", "client_id", "weow-acquisition-0"),
            ("kafka", "topic", "inference.jobs.live"),
            ("acquisition", "output_prefix", ""),
        )
        for section, key, value in cases:
            with self.subTest(section=section, key=key):
                settings = self.settings()
                settings[section][key] = value
                with self.assertRaises(ValueError):
                    validate_benchmark_settings(
                        settings, {"dbname": "weow_ml_benchmark"}
                    )
        with self.assertRaises(ValueError):
            validate_benchmark_settings(self.settings(), {"dbname": "weow_ml"})

    def test_notification_from_another_run_is_rejected(self):
        notification = self.notification()
        notification.payload["benchmark"]["run_id"] = "another_run"
        with self.assertRaises(ContractError):
            BenchmarkGuard("run_001", "REQUIRED_BENCHMARK_SPOOL_BUCKET")(notification)


if __name__ == "__main__":
    unittest.main()
