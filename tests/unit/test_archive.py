import io
import json
from pathlib import Path
import unittest

try:
    from botocore.exceptions import ClientError
    from weow_ml.archive import archive_notification
except ModuleNotFoundError as exc:
    if exc.name not in ("boto3", "botocore"):
        raise
    archive_notification = None


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.writes = 0
        self.reads = 0

    def get_object(self, Bucket, Key):
        self.reads += 1
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"},
                               "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Bucket, Key])}

    def put_object(self, Bucket, Key, Body, ContentType, IfNoneMatch):
        assert IfNoneMatch == "*"
        if (Bucket, Key) in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"},
                               "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
        self.objects[Bucket, Key] = Body
        self.writes += 1


@unittest.skipIf(archive_notification is None, "install requirements-s3.txt")
class ArchiveTests(unittest.TestCase):
    def setUp(self):
        fixture = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"
        self.payload = json.loads(fixture.read_text())
        self.source, self.destination = MemoryS3(), MemoryS3()
        storage = self.payload["storage"]
        self.source.objects[storage["bucket"], storage["object_key"]] = b"\xff\xd8test\xff\xd9"

    def test_transfer_and_retry_preserve_sidecar(self):
        first = archive_notification(self.source, self.destination, "archive", self.payload)
        self.assertEqual(self.destination.reads, 0)
        before = dict(self.destination.objects)
        second = archive_notification(self.source, self.destination, "archive", self.payload)
        self.assertEqual(first, second)
        self.assertEqual(before, self.destination.objects)
        self.assertEqual(self.destination.writes, 2)
        self.assertEqual(self.destination.reads, 2)
        sidecar = json.loads(before["archive", first["sidecar_key"]])
        self.assertEqual(sidecar["notification"], self.payload)
        self.assertIsNone(sidecar["acquisition"]["processing_stream_ids"])

    def test_existing_image_is_not_overwritten(self):
        first = archive_notification(self.source, self.destination, "archive", self.payload)
        key = ("archive", first["archive_key"])
        self.destination.objects[key] = b"different"
        with self.assertRaises(ValueError):
            archive_notification(self.source, self.destination, "archive", self.payload)
        self.assertEqual(self.destination.objects[key], b"different")

    def test_missing_source_writes_nothing(self):
        self.source.objects.clear()
        with self.assertRaises(ClientError):
            archive_notification(self.source, self.destination, "archive", self.payload)
        self.assertEqual(self.destination.writes, 0)

    def test_indoor_image_is_excluded(self):
        self.payload["source_stream"]["tags"] = ["indoor"]
        result = archive_notification(self.source, self.destination, "archive", self.payload)
        self.assertEqual(result["status"], "excluded_indoor")
        self.assertEqual(self.destination.writes, 0)
