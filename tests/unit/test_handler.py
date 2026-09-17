import io
import json
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from botocore.exceptions import ClientError
from PIL import Image

from weow_ml.acquisition.handler import NotificationHandler
from weow_ml.acquisition.registry import Resolution, Stream


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.reads = 0

    def get_object(self, Bucket, Key):
        self.reads += 1
        try:
            content = self.objects[Bucket, Key]
        except KeyError:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"},
                 "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject"
            )
        return {"Body": io.BytesIO(content)}

    def put_object(self, Bucket, Key, Body, ContentType, IfNoneMatch):
        identity = (Bucket, Key)
        if identity in self.objects:
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed"},
                 "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject"
            )
        self.objects[identity] = Body


class Registry:
    def __init__(self, status, streams):
        self.status, self.streams = status, streams

    def resolve(self, payload):
        return Resolution(payload["derived_stream"]["derived_stream_id"],
                          self.status, self.streams)


class StatePublisher:
    def __init__(self, root, paths):
        self.root, self.paths, self.streams = Path(root), paths, []

    def ensure(self, stream_id, processing_image_path):
        self.assert_images_exist()
        self.streams.append(stream_id)

    def assert_images_exist(self):
        assert all((self.root / path).is_file() for path in self.paths)


class Publisher:
    def __init__(self, archive, archive_bucket, archive_key, state):
        self.archive, self.archive_bucket, self.archive_key, self.state, self.jobs = (
            archive, archive_bucket, archive_key, state, []
        )

    def publish(self, job):
        assert (self.archive_bucket, self.archive_key) in self.archive.objects
        self.state.assert_images_exist()
        assert job.processing_stream_id in self.state.streams
        self.jobs.append(job)


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(FIXTURE.read_text())
        output = io.BytesIO()
        Image.new("RGB", (512, 288), (20, 40, 60)).save(output, format="JPEG", quality=85)
        self.image = output.getvalue()
        self.spool, self.archive = MemoryS3(), MemoryS3()
        storage = self.payload["storage"]
        self.spool.objects[storage["bucket"], storage["object_key"]] = self.image
        self.stream = Stream("fin12345P01T0_P0S0V0", 0, 0, 512, True, True, 7)
        self.now = datetime(2026, 4, 15, 10, 25, 4, tzinfo=timezone.utc)

    def handler(self, root, status="whitelist", streams=None):
        streams = (self.stream,) if streams is None else streams
        path = "images/fin12345P01T0/2026/04/15/10/20260415T102500Z_fin12345P01T0_P0S0V0.jpg"
        state = StatePublisher(root, [path] if streams else [])
        archive_key = "images/fin12345P01T0/2026/04/15/10/20260415T102500Z_fin12345P01T0.jpg"
        publisher = Publisher(self.archive, "archive", archive_key, state)
        handler = NotificationHandler(
            Registry(status, streams), self.spool, self.archive, "archive", root,
            state, publisher, {"fin": "download_timestamp"},
            "download_timestamp", 3600, clock=lambda: self.now,
        )
        return handler, state, publisher

    def test_blacklist_does_not_download(self):
        with tempfile.TemporaryDirectory() as root:
            handler, state, publisher = self.handler(root, "blacklist", ())
            result = handler.handle(self.payload)
        self.assertEqual((result.status, result.archived, result.published_jobs),
                         ("blacklisted", False, 0))
        self.assertEqual(self.spool.reads, 0)
        self.assertFalse(self.archive.objects)
        self.assertFalse(state.streams)
        self.assertFalse(publisher.jobs)

    def test_greylist_archives_sidecar_without_nfs_or_job(self):
        with tempfile.TemporaryDirectory() as root:
            handler, state, publisher = self.handler(root, "greylist")
            result = handler.handle(self.payload)
            self.assertFalse(any(Path(root).rglob("*.jpg")))
        self.assertEqual((result.status, result.archived, result.published_jobs),
                         ("greylisted", True, 0))
        sidecar_key = "images/fin12345P01T0/2026/04/15/10/20260415T102500Z_fin12345P01T0.json"
        sidecar = json.loads(self.archive.objects["archive", sidecar_key])
        self.assertEqual(sidecar["acquisition"]["processing_stream_ids"],
                         [self.stream.processing_stream_id])
        self.assertEqual(sidecar["acquisition"]["solar"]["phase"], 1)
        self.assertFalse(state.streams)
        self.assertFalse(publisher.jobs)

    def test_whitelist_orders_archive_nfs_state_and_acknowledged_job(self):
        with tempfile.TemporaryDirectory() as root:
            handler, state, publisher = self.handler(root)
            result = handler.handle(self.payload)
            processing_path = Path(root) / publisher.jobs[0].processing_image
            self.assertEqual(processing_path.read_bytes(), self.image)
        self.assertEqual((result.status, result.archived, result.published_jobs),
                         ("published", True, 1))
        self.assertEqual(state.streams, [self.stream.processing_stream_id])
        self.assertEqual(publisher.jobs[0].kafka_partition, 7)


if __name__ == "__main__":
    unittest.main()
