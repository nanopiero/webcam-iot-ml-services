import copy
import json
from datetime import timedelta
from pathlib import Path
import unittest

from weow_ml.acquisition.contracts import parse_notification
from weow_ml.acquisition.policies import (
    PolicyError,
    acquisition_decision,
    classify_solar,
    coordinates,
    reference_timestamp,
)


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(FIXTURE.read_text())

    def notification(self, payload=None):
        return parse_notification(payload or self.payload)

    def test_network_timestamp_rule_and_missing_value(self):
        notification = self.notification()
        selected = reference_timestamp(
            notification, {"fin": "provider_update_timestamp"}, "download_timestamp"
        )
        self.assertEqual(selected.isoformat(), "2026-04-15T10:20:00+00:00")
        payload = copy.deepcopy(self.payload)
        payload["timestamps"]["provider_update_timestamp"] = None
        with self.assertRaisesRegex(PolicyError, "unavailable"):
            reference_timestamp(
                self.notification(payload),
                {"fin": "provider_update_timestamp"},
                "download_timestamp",
            )

    def test_coordinates_prefer_source_stream_and_validate(self):
        payload = copy.deepcopy(self.payload)
        payload["source_stream"]["latitude"] = 61.0
        payload["source_stream"]["longitude"] = 25.0
        self.assertEqual(coordinates(self.notification(payload)), (61.0, 25.0))
        payload["source_stream"]["latitude"] = 100.0
        with self.assertRaisesRegex(PolicyError, "invalid"):
            coordinates(self.notification(payload))

    def test_solar_classification_and_signed_offsets(self):
        result = classify_solar(
            self.notification(), {"fin": "download_timestamp"},
            "download_timestamp", 3600,
        )
        self.assertEqual(result.phase, 1)
        self.assertGreater(result.sunrise_offset_seconds, 0)
        self.assertLess(result.sunset_offset_seconds, 0)
        self.assertAlmostEqual(
            result.sunrise_offset_seconds,
            (result.reference_timestamp - result.sunrise).total_seconds(),
        )

        payload = copy.deepcopy(self.payload)
        transition_time = result.sunrise + timedelta(minutes=30)
        value = transition_time.isoformat().replace("+00:00", "Z")
        payload["timestamps"]["download_timestamp"] = value
        payload["image_id"] = transition_time.strftime("%Y%m%dT%H%M%SZ_") + "fin12345P01T0.jpg"
        payload["storage"]["object_key"] = (
            payload["storage"]["object_key"].rsplit("/", 1)[0]
            + "/" + payload["image_id"]
        )
        transition = classify_solar(
            self.notification(payload), {}, "download_timestamp", 3600
        )
        self.assertEqual(transition.phase, 2)

    def test_status_and_solar_decisions(self):
        expected = {
            ("blacklist", 1): (False, False, "blacklisted"),
            ("greylist", 1): (True, False, "greylisted"),
            ("whitelist", 0): (True, False, "nighttime"),
            ("whitelist", 1): (True, True, "eligible"),
            ("whitelist", 2): (True, True, "eligible"),
        }
        for inputs, output in expected.items():
            decision = acquisition_decision(*inputs)
            self.assertEqual(
                (decision.archive, decision.publish_jobs, decision.reason), output
            )


if __name__ == "__main__":
    unittest.main()
