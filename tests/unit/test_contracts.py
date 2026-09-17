import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

from weow_ml.acquisition import ContractError, parse_notification, partition_for


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(FIXTURE.read_text())

    def test_upstream_example_and_archive_identity(self):
        result = parse_notification(self.payload)
        self.assertEqual(result.archive_key,
                         "images/fin12345P01T0/2026/04/15/10/20260415T102500Z_fin12345P01T0.jpg")
        self.assertTrue(result.sidecar_key.endswith(".json"))
        self.assertIsNone(result.profile.colour_depth)

    def test_missing_source_depth_is_not_derived_depth(self):
        unknown = parse_notification(self.payload).profile.fingerprint
        self.payload["source_image"]["colour_depth"] = 24
        self.assertNotEqual(unknown, parse_notification(self.payload).profile.fingerprint)

    def test_unknown_metadata_preserved_without_caller_aliasing(self):
        self.payload["future_field"] = {"values": [1]}
        result = parse_notification(self.payload)
        self.payload["future_field"]["values"].append(2)
        self.assertEqual(result.payload["future_field"], {"values": [1]})

    def test_invalid_inputs_are_rejected(self):
        cases = [
            ("schema_version", "N9V9"),
            ("image_id", "../../bad.jpg"),
            ("timestamps.download_timestamp", "2026-04-15T10:25:00"),
            ("timestamps.download_timestamp", "2026-04-16T10:25:00Z"),
            ("storage.object_key", "../" + self.payload["image_id"]),
            ("source_image.width", True),
            ("derived_image.height", 0),
            ("derived_stream.transformation_metadata.image_signature", float("nan")),
        ]
        for path, value in cases:
            with self.subTest(path=path):
                payload = copy.deepcopy(self.payload)
                target = payload
                fields = path.split(".")
                for field in fields[:-1]:
                    target = target[field]
                target[fields[-1]] = value
                with self.assertRaises(ContractError):
                    parse_notification(payload)

    def test_partition_is_stable_in_another_python_process(self):
        stream = "fin12345P01T0_P0S0V0"
        output = subprocess.check_output([
            sys.executable, "-c",
            "from weow_ml.acquisition import partition_for; "
            f"print(partition_for({stream!r}))",
        ], text=True)
        self.assertEqual(int(output), partition_for(stream))
        self.assertTrue(0 <= int(output) < 50)

    def test_nonpositive_partition_count_rejected(self):
        with self.assertRaises(ContractError):
            partition_for("stream", 0)

    def test_cli_accepts_fixture(self):
        result = subprocess.run([sys.executable, "-m", "weow_ml", str(FIXTURE)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["image_id"], self.payload["image_id"])


if __name__ == "__main__":
    unittest.main()
