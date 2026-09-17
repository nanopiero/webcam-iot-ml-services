"""PostgreSQL registry checks against the dedicated weow_ml_test database."""

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import unittest
import uuid


RUN_DATABASE_TESTS = os.environ.get("WEOW_RUN_DB_INTEGRATION") == "1"

if RUN_DATABASE_TESTS:
    import psycopg

    from weow_ml.acquisition.registry import Registry
    from weow_ml.common.config import database_settings


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"
MISSING = object()


@unittest.skipUnless(
    RUN_DATABASE_TESTS,
    "set WEOW_RUN_DB_INTEGRATION=1 to use the dedicated PostgreSQL test database",
)
class RegistryPostgresTests(unittest.TestCase):
    def setUp(self):
        self.settings = database_settings(test=True)
        self.created_streams = []

    def tearDown(self):
        if not self.created_streams:
            return
        with psycopg.connect(**self.settings) as connection:
            for stream_id in self.created_streams:
                connection.execute(
                    "DELETE FROM processing_stream WHERE derived_stream_id=%s", (stream_id,)
                )
                connection.execute(
                    "DELETE FROM processing_profile WHERE derived_stream_id=%s", (stream_id,)
                )
                connection.execute(
                    "DELETE FROM derived_stream WHERE derived_stream_id=%s", (stream_id,)
                )

    def new_stream_id(self):
        stream_id = "test" + uuid.uuid4().hex
        self.created_streams.append(stream_id)
        return stream_id

    def payload(self, stream_id, second, *, depth=MISSING, tags=None, latitude=None):
        value = copy.deepcopy(json.loads(FIXTURE.read_text()))
        timestamp = datetime(2026, 4, 15, 10, 25, second, tzinfo=timezone.utc)
        stamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
        image_id = f"{stamp}_{stream_id}.jpg"
        value["image_id"] = image_id
        value["storage"]["object_key"] = "/".join(
            value["storage"]["object_key"].split("/")[:-1] + [image_id]
        )
        value["timestamps"]["download_timestamp"] = timestamp.isoformat().replace("+00:00", "Z")
        value["derived_stream"]["derived_stream_id"] = stream_id
        if depth is MISSING:
            value["source_image"].pop("colour_depth", None)
        else:
            value["source_image"]["colour_depth"] = depth
        if tags is not None:
            value["source_stream"]["tags"] = tags
        if latitude is not None:
            value["source_stream"]["latitude"] = latitude
        return value

    def test_restricted_role_and_schema(self):
        with psycopg.connect(**self.settings) as connection:
            row = connection.execute(
                "SELECT current_user, current_database(), "
                "(SELECT max(version) FROM schema_migration)"
            ).fetchone()
        self.assertEqual(row, ("weow_acquisition", "weow_ml_test", 1))

    def test_profile_reuse_partition_inheritance_and_cache_reload(self):
        stream_id = self.new_stream_id()
        registry = Registry(self.settings)

        original = registry.resolve(self.payload(stream_id, 0))
        changed = registry.resolve(self.payload(stream_id, 1, depth=24))
        recurring = registry.resolve(self.payload(stream_id, 2))

        self.assertEqual(original.status, "whitelist")
        self.assertEqual(len(original.streams), 1)
        self.assertIn("_P0S0V0", original.streams[0].processing_stream_id)
        self.assertIn("_P1S0V0", changed.streams[0].processing_stream_id)
        self.assertEqual(original.streams[0].kafka_partition,
                         changed.streams[0].kafka_partition)
        self.assertEqual(recurring.streams, original.streams)

        reloaded = Registry(self.settings).resolve(self.payload(stream_id, 2))
        self.assertEqual(reloaded, recurring)

    def test_stale_metadata_does_not_replace_current_metadata(self):
        stream_id = self.new_stream_id()
        registry = Registry(self.settings)
        registry.resolve(self.payload(stream_id, 0, latitude=45.0))
        registry.resolve(self.payload(stream_id, 2, latitude=46.0))
        stale = registry.resolve(self.payload(stream_id, 1, tags=["indoor"], latitude=10.0))

        self.assertEqual(stale.status, "whitelist")
        with psycopg.connect(**self.settings) as connection:
            parent = connection.execute(
                "SELECT metadata, operational_status FROM derived_stream "
                "WHERE derived_stream_id=%s", (stream_id,)
            ).fetchone()
        self.assertEqual(parent[0]["source_stream"]["latitude"], 46.0)
        self.assertEqual(parent[1], "whitelist")

    def test_status_change_and_administrative_override_survive_reload(self):
        stream_id = self.new_stream_id()
        registry = Registry(self.settings)

        excluded = registry.resolve(self.payload(stream_id, 0, tags=["indoor"]))
        enabled = registry.resolve(self.payload(stream_id, 1, tags=[]))
        self.assertEqual(excluded.status, "blacklist")
        self.assertEqual(excluded.streams, ())
        self.assertEqual(enabled.status, "whitelist")
        self.assertTrue(enabled.streams)

        with psycopg.connect(**self.settings) as connection:
            connection.execute(
                "UPDATE derived_stream SET status_override='greylist' "
                "WHERE derived_stream_id=%s", (stream_id,)
            )
        reloaded = Registry(self.settings).resolve(self.payload(stream_id, 1, tags=[]))
        self.assertEqual(reloaded.status, "greylist")
        self.assertEqual(reloaded.streams, enabled.streams)

    def test_far_arctic_stream_is_blacklisted_before_stream_creation(self):
        stream_id = self.new_stream_id()
        resolution = Registry(self.settings).resolve(
            self.payload(stream_id, 0, latitude=68.57)
        )
        self.assertEqual(resolution.status, "blacklist")
        self.assertEqual(resolution.streams, ())
        with psycopg.connect(**self.settings) as connection:
            row = connection.execute(
                "SELECT operational_status FROM derived_stream WHERE derived_stream_id=%s",
                (stream_id,),
            ).fetchone()
        self.assertEqual(row[0], "blacklist")

    def test_concurrent_discovery_creates_one_profile_and_stream_set(self):
        stream_id = self.new_stream_id()
        payload = self.payload(stream_id, 0)
        registries = (Registry(self.settings), Registry(self.settings))
        barrier = threading.Barrier(2)

        def resolve(registry):
            barrier.wait()
            return registry.resolve(payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(resolve, registries))

        self.assertEqual(results[0], results[1])
        with psycopg.connect(**self.settings) as connection:
            profiles = connection.execute(
                "SELECT count(*) FROM processing_profile WHERE derived_stream_id=%s",
                (stream_id,),
            ).fetchone()[0]
            streams = connection.execute(
                "SELECT count(*) FROM processing_stream WHERE derived_stream_id=%s",
                (stream_id,),
            ).fetchone()[0]
        self.assertEqual(profiles, 1)
        self.assertEqual(streams, len(results[0].streams))


if __name__ == "__main__":
    unittest.main()
