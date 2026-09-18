import io
import json
from datetime import datetime, timezone
from pathlib import Path
import unittest

from botocore.exceptions import ClientError

from weow_ml.benchmark.images import load_profiles
from weow_ml.benchmark.spool import prepare_spool
from weow_ml.benchmark.workload import SCENARIOS, Workload


ROOT = Path(__file__).parents[2]


class MemoryS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"},
                               "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Bucket, Key])}

    def put_object(self, Bucket, Key, Body, ContentType, IfNoneMatch):
        identity = Bucket, Key
        if identity in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"},
                               "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
        self.objects[identity] = Body


class BenchmarkSpoolTests(unittest.TestCase):
    def workload(self):
        return Workload(
            json.loads((ROOT / "tests/fixtures/notification_n0v0.json").read_text()),
            SCENARIOS["smoke"], "run_001",
            datetime(2026, 9, 19, 10, tzinfo=timezone.utc),
            load_profiles(json.loads((ROOT / "benchmarks/image_profiles.json").read_text())),
            spool_bucket="benchmark-spool",
        )

    def test_prepare_is_complete_and_idempotent(self):
        client = MemoryS3()
        first = prepare_spool(client, "benchmark-spool", self.workload())
        before = dict(client.objects)
        second = prepare_spool(client, "benchmark-spool", self.workload())
        self.assertEqual(first, second)
        self.assertEqual(before, client.objects)
        self.assertEqual(first["objects"], 12)
        self.assertEqual(first["bytes"], sum(map(len, client.objects.values())))


if __name__ == "__main__":
    unittest.main()
