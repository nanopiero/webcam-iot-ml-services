"""PostgreSQL-backed processing-stream registry with a startup snapshot cache."""

import threading
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .contracts import parse_notification
from .policies import DEFAULT_MAX_PROCESSING_LATITUDE, automatic_status
from .streams import fingerprint, partition_for, slice_intervals, stream_metadata


@dataclass(frozen=True)
class Stream:
    processing_stream_id: str
    slice_index: int
    slice_left: int
    slice_right: int
    process_snow: bool
    process_visibility: bool
    kafka_partition: int


@dataclass(frozen=True)
class Resolution:
    derived_stream_id: str
    status: str
    streams: tuple


class Registry:
    """One instance per Acquisition process; locks serialize its cache changes.

    PostgreSQL parent-row locks additionally serialize independent writers.
    Administrative changes become visible after reload(), matching daily renewal.
    """

    def __init__(self, connection_settings, partitions=50, generation=0, free_water_tags=(),
                 maximum_processing_latitude=DEFAULT_MAX_PROCESSING_LATITUDE):
        if generation != 0:
            raise ValueError("Only generation V0 is implemented; upgrades need explicit rules")
        self._settings = dict(connection_settings)
        self.partitions = partitions
        self.generation = generation
        self.free_water_tags = frozenset(free_water_tags)
        self.maximum_processing_latitude = maximum_processing_latitude
        self._lock = threading.RLock()
        self._cache = {}
        self.reload()

    def connect(self):
        return psycopg.connect(**self._settings, row_factory=dict_row)

    def reload(self):
        with self._lock, self.connect() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            settings = conn.execute("SELECT * FROM registry_settings").fetchone()
            if settings is None or settings["partition_count"] != self.partitions or settings["partition_hash"] != "sha256":
                raise ValueError("Configured partition mapping differs from persisted registry settings")
            self._cache = {}
            for parent in conn.execute("SELECT * FROM derived_stream").fetchall():
                profile = conn.execute(
                    "SELECT * FROM processing_profile WHERE derived_stream_id=%s "
                    "AND generation_version=%s AND profile_id=%s",
                    (parent["derived_stream_id"], parent["current_generation"], parent["current_profile_id"]),
                ).fetchone()
                self._cache[parent["derived_stream_id"]] = (parent, profile, self._streams(conn, parent))

    @staticmethod
    def _streams(conn, parent):
        rows = conn.execute(
            "SELECT processing_stream_id, slice_index, slice_left, slice_right, "
            "process_snow, process_visibility, kafka_partition FROM processing_stream "
            "WHERE derived_stream_id=%s AND generation_version=%s AND profile_id=%s ORDER BY slice_index",
            (parent["derived_stream_id"], parent["current_generation"], parent["current_profile_id"]),
        ).fetchall()
        return tuple(Stream(**row) for row in rows)

    @staticmethod
    def _status(parent):
        return parent["status_override"] or parent["operational_status"]

    def resolve(self, payload):
        notification = parse_notification(payload)
        stream_id = notification.derived_stream_id
        metadata = stream_metadata(payload)
        metadata_hash = fingerprint(metadata)
        width, height = (payload["derived_image"][key] for key in ("width", "height"))
        intervals = slice_intervals(width, height)
        with self._lock:
            cached = self._cache.get(stream_id)
            if cached:
                parent, profile, streams = cached
                if parent["metadata_fingerprint"] == metadata_hash:
                    if self._status(parent) == "blacklist":
                        return Resolution(stream_id, "blacklist", ())
                    if (profile and profile["profile_fingerprint"] == notification.profile.fingerprint
                            and profile["generation_version"] == self.generation
                            and (profile["derived_width"], profile["derived_height"]) == (width, height)):
                        return Resolution(stream_id, self._status(parent), streams)
            # Promote cache entries only after the transaction commits.
            with self.connect() as conn:
                auto_status = automatic_status(metadata, self.maximum_processing_latitude)
                conn.execute(
                    "INSERT INTO derived_stream (derived_stream_id, network_id, metadata, "
                    "metadata_fingerprint, metadata_timestamp, operational_status) VALUES (%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (derived_stream_id) DO NOTHING",
                    (stream_id, notification.network_id, Jsonb(metadata), metadata_hash,
                     notification.download_timestamp, auto_status),
                )
                parent = conn.execute("SELECT * FROM derived_stream WHERE derived_stream_id=%s FOR UPDATE",
                                      (stream_id,)).fetchone()
                current = notification.download_timestamp >= parent["metadata_timestamp"]
                if current:
                    parent = conn.execute(
                        "UPDATE derived_stream SET metadata=%s, metadata_fingerprint=%s, "
                        "metadata_timestamp=%s, operational_status=%s, updated_at=now() "
                        "WHERE derived_stream_id=%s RETURNING *",
                        (Jsonb(metadata), metadata_hash, notification.download_timestamp, auto_status, stream_id),
                    ).fetchone()
                profile, streams = None, ()
                if self._status(parent) != "blacklist":
                    profile = conn.execute(
                        "SELECT * FROM processing_profile WHERE derived_stream_id=%s AND generation_version=%s "
                        "AND profile_fingerprint=%s",
                        (stream_id, self.generation, notification.profile.fingerprint),
                    ).fetchone()
                    if profile is None:
                        previous = self._streams(conn, parent)
                        inherit = (parent["current_generation"] == self.generation
                                   and tuple((s.slice_left, s.slice_right) for s in previous) == intervals)
                        profile_id = conn.execute(
                            "SELECT COALESCE(MAX(profile_id), -1)+1 AS next_id FROM processing_profile "
                            "WHERE derived_stream_id=%s AND generation_version=%s", (stream_id, self.generation),
                        ).fetchone()["next_id"]
                        p = notification.profile
                        profile = conn.execute(
                            "INSERT INTO processing_profile VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                            (stream_id, self.generation, profile_id, p.fingerprint, p.width, p.height,
                             p.colour_mode, p.colour_depth, width, height),
                        ).fetchone()
                        tags = set(metadata["source_stream"].get("tags", []))
                        panoramic = payload["derived_stream"].get("transformation_metadata", {}).get("panoramic", False)
                        snow = not ((notification.network_id == "win" and (panoramic or len(intervals) > 1))
                                    or (notification.network_id in ("win", "ska") and tags & self.free_water_tags))
                        for index, (left, right) in enumerate(intervals):
                            slice_id = index + 1 if len(intervals) > 1 else 0
                            sid = f"{stream_id}_P{profile_id}S{slice_id}V{self.generation}"
                            old = previous[index] if inherit else None
                            conn.execute(
                                "INSERT INTO processing_stream VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                (sid, stream_id, self.generation, profile_id, slice_id, left, right,
                                 old.process_snow if old else snow,
                                 old.process_visibility if old else True,
                                 old.kafka_partition if old else partition_for(sid, self.partitions)),
                            )
                    elif (profile["derived_width"], profile["derived_height"]) != (width, height):
                        raise ValueError("Derived dimensions changed within one source profile; generation policy required")
                    resolved_parent = dict(parent, current_generation=self.generation,
                                           current_profile_id=profile["profile_id"])
                    streams = self._streams(conn, resolved_parent)
                    if current:
                        parent = conn.execute(
                            "UPDATE derived_stream SET current_generation=%s, current_profile_id=%s "
                            "WHERE derived_stream_id=%s RETURNING *",
                            (self.generation, profile["profile_id"], stream_id),
                        ).fetchone()
                resolution = Resolution(stream_id, self._status(parent), streams)
            if current:
                self._cache[stream_id] = (parent, profile, streams)
            else:
                self._cache.pop(stream_id, None)
            return resolution
