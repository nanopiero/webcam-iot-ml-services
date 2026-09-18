from datetime import datetime, timezone
import json
from pathlib import Path
import unittest

from weow_ml.acquisition.contracts import parse_notification
from weow_ml.benchmark.images import load_profiles
from weow_ml.benchmark.workload import SCENARIOS, Workload


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"
PROFILES = Path(__file__).parents[2] / "benchmarks" / "image_profiles.json"


class BenchmarkWorkloadTests(unittest.TestCase):
    def workload(self, scenario="smoke"):
        return Workload(
            json.loads(FIXTURE.read_text()), SCENARIOS[scenario], "run_001",
            datetime(2026, 4, 15, 10, tzinfo=timezone.utc),
            load_profiles(json.loads(PROFILES.read_text())), seed=7,
        )

    def test_presets_have_required_measured_event_counts(self):
        self.assertEqual(SCENARIOS["nominal"].measured_events, 10000)
        self.assertEqual(SCENARIOS["burst"].measured_events, 10000)
        self.assertEqual(SCENARIOS["margin"].measured_events, 12000)

    def test_each_run_has_distinct_stream_identities(self):
        first = next(self.workload().events())
        second_workload = Workload(
            json.loads(FIXTURE.read_text()), SCENARIOS["smoke"], "run_002",
            datetime(2026, 4, 15, 10, tzinfo=timezone.utc),
            load_profiles(json.loads(PROFILES.read_text())), seed=7,
        )
        second = next(second_workload.events())
        self.assertNotEqual(
            first["derived_stream"]["derived_stream_id"],
            second["derived_stream"]["derived_stream_id"],
        )

    def test_events_are_valid_and_isolated(self):
        workload = self.workload()
        events = list(workload.events())
        self.assertEqual(len(events), workload.summary()["total_events"])
        for payload in events:
            notification = parse_notification(payload)
            self.assertEqual(notification.network_id, "benchmark")
            self.assertTrue(notification.derived_stream_id.startswith("bm"))
            self.assertTrue(notification.object_key.startswith(
                "benchmarks/wp1_5/run_001/spool/"
            ))
        self.assertFalse(workload.summary()["network_side_effects"])

    def test_seed_reproduces_stream_variants_and_metadata_burst(self):
        first = list(self.workload().events())
        second = list(self.workload().events())
        self.assertEqual(first, second)
        measured = [event for event in first if event["benchmark"]["phase"] == "measured"]
        changed = [event for event in measured if event["benchmark"]["metadata_changed"]]
        self.assertTrue(changed)
        self.assertTrue(any(
            event["derived_stream"]["transformation_metadata"]["panoramic"]
            for event in first
        ))
        panorama = next(
            event for event in first
            if event["derived_stream"]["transformation_metadata"]["panoramic"]
        )
        self.assertEqual(
            (panorama["derived_image"]["width"], panorama["derived_image"]["height"]),
            (1600, 288),
        )
        counts = {}
        for event in measured:
            stream = event["derived_stream"]["derived_stream_id"]
            counts[stream] = counts.get(stream, 0) + 1
        self.assertGreater(max(counts.values()), min(counts.values()))


if __name__ == "__main__":
    unittest.main()
