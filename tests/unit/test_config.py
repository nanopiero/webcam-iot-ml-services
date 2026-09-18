import json
from pathlib import Path
import tempfile
import unittest

from weow_ml.common.config import load_config


class ConfigTests(unittest.TestCase):
    def test_explicit_benchmark_bucket_and_credentials_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {
                "spool_s3": {
                    "endpoint_url": "REQUIRED", "bucket": "benchmark-bucket",
                    "credentials_prefix": "weows",
                },
                "archive_s3": {"endpoint_url": "REQUIRED", "bucket": "REQUIRED"},
            }
            endpoints = {
                "spool_s3": {"endpoint_url": "https://s3.example", "bucket": "live-spool"},
                "archive_s3": {"endpoint_url": "https://s3.example", "bucket": "archive"},
                "ca_bundle": "/ca.pem",
            }
            config_path = root / "config.json"
            config_path.write_text(json.dumps(config))
            secrets = root / "secrets"
            secrets.mkdir()
            (secrets / "s3_endpoints.json").write_text(json.dumps(endpoints))

            loaded = load_config(config_path, secrets)

            self.assertEqual(loaded["spool_s3"]["bucket"], "benchmark-bucket")
            self.assertEqual(loaded["archive_s3"]["bucket"], "archive")
            self.assertEqual(
                loaded["spool_s3"]["access_key_file"],
                str(secrets / "weows_s3_access_key"),
            )


if __name__ == "__main__":
    unittest.main()
