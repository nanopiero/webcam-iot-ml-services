"""Build deterministic, isolated Acquisition benchmark notifications."""

import argparse
import copy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import re

from ..acquisition.contracts import parse_notification
from .images import load_profiles


BENCHMARK_MQTT_TOPIC = "weow/benchmark/N0V0"
BENCHMARK_KAFKA_TOPIC = "inference.jobs.benchmark"


@dataclass(frozen=True)
class Scenario:
    name: str
    rate_per_second: float
    duration_seconds: int
    warmup_rate_per_second: float
    warmup_seconds: int
    streams: int
    metadata_change_fraction: float
    panoramic_fraction: float

    @property
    def warmup_events(self):
        return round(self.warmup_rate_per_second * self.warmup_seconds)

    @property
    def measured_events(self):
        return round(self.rate_per_second * self.duration_seconds)


SCENARIOS = {
    "smoke": Scenario("smoke", 1.0, 10, 1.0, 2, 5, 0.2, 0.2),
    "nominal": Scenario("nominal", 10000 / 300, 300, 10.0, 60, 1000, 0.05, 0.1),
    "burst": Scenario("burst", 10000 / 60, 60, 10.0, 60, 1000, 0.05, 0.1),
    "margin": Scenario("margin", 200.0, 60, 10.0, 60, 1000, 0.05, 0.1),
}


def _timestamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Workload:
    """Generate a stable sequence without contacting MQTT, S3, Kafka, or PostgreSQL."""

    def __init__(self, fixture, scenario, run_id, start, profiles, seed=1,
                 spool_bucket="REQUIRED_BENCHMARK_SPOOL_BUCKET"):
        self.fixture = copy.deepcopy(fixture)
        self.scenario = scenario
        self.run_id = run_id
        self.start = start.astimezone(timezone.utc)
        self.seed = seed
        self.profiles = tuple(profiles)
        self.spool_bucket = spool_bucket
        self.standard_profiles = tuple(p for p in self.profiles if not p.panoramic)
        self.panoramic_profiles = tuple(p for p in self.profiles if p.panoramic)
        if not self.standard_profiles or not self.panoramic_profiles:
            raise ValueError("benchmark requires standard and panoramic profiles")
        self.random = random.Random(seed)
        self.panorama_streams = set(self.random.sample(
            range(scenario.streams), round(scenario.streams * scenario.panoramic_fraction)
        ))
        self.changed_streams = set(self.random.sample(
            range(scenario.streams), round(scenario.streams * scenario.metadata_change_fraction)
        ))
        fast = max(1, round(scenario.streams * 0.2))
        medium = max(fast, round(scenario.streams * 0.5))
        weights = tuple(
            5 if stream < fast else 2 if stream < medium else 1
            for stream in range(scenario.streams)
        )
        self.stream_schedule = tuple(
            stream for repetition in range(max(weights))
            for stream, weight in enumerate(weights) if weight > repetition
        )

    def events(self):
        yield from self._phase("warmup", self.scenario.warmup_events,
                               self.scenario.warmup_rate_per_second, self.start)
        measured_start = self.start + timedelta(seconds=self.scenario.warmup_seconds)
        yield from self._phase("measured", self.scenario.measured_events,
                               self.scenario.rate_per_second, measured_start)

    def _phase(self, phase, count, rate, phase_start):
        for index in range(count):
            stream_number = self.stream_schedule[index % len(self.stream_schedule)]
            timestamp = phase_start + timedelta(seconds=index / rate)
            changed = (phase == "measured" and index >= count // 2
                       and stream_number in self.changed_streams)
            yield self._notification(stream_number, timestamp, phase, changed)

    def _notification(self, stream_number, timestamp, phase, changed):
        payload = copy.deepcopy(self.fixture)
        panoramic = stream_number in self.panorama_streams
        choices = self.panoramic_profiles if panoramic else self.standard_profiles
        profile = choices[stream_number % len(choices)]
        stem = f"bm{stream_number:08d}"
        stream_id = stem + "T0"
        image_id = timestamp.strftime("%Y%m%dT%H%M%SZ_") + stream_id + ".jpg"
        payload["image_id"] = image_id
        payload["network"]["network_id"] = "benchmark"
        payload["site"]["site_id"] = stem
        payload["source_stream"]["source_stream_id"] = stem + "P01"
        payload["source_stream"]["provider_stream_id"] = "benchmark-" + stem
        payload["source_stream"]["name"] = "WP1.5 benchmark stream"
        payload["derived_stream"]["derived_stream_id"] = stream_id
        payload["derived_stream"]["transformation_metadata"]["panoramic"] = panoramic
        for level in ("source_image", "derived_image"):
            payload[level]["width"] = profile.width
            payload[level]["height"] = profile.height
            payload[level]["size_bytes"] = profile.size_bytes
        if changed:
            payload["source_stream"]["name"] = "WP1.5 benchmark stream metadata-v2"
            payload["source_stream"]["latitude"] += 0.001
        payload["timestamps"]["download_timestamp"] = _timestamp(timestamp)
        payload["timestamps"]["provider_update_timestamp"] = _timestamp(
            timestamp - timedelta(seconds=30)
        )
        payload["timestamps"]["publication_timestamp"] = _timestamp(
            timestamp + timedelta(seconds=1)
        )
        payload["storage"] = {
            "type": "s3",
            "bucket": self.spool_bucket,
            "object_key": (
                f"benchmarks/wp1_5/{self.run_id}/spool/{phase}/"
                f"{timestamp:%Y/%m/%d/%H}/{image_id}"
            ),
        }
        payload["benchmark"] = {
            "run_id": self.run_id,
            "seed": self.seed,
            "scenario": self.scenario.name,
            "phase": phase,
            "metadata_changed": changed,
            "image_profile": profile.name,
        }
        parse_notification(payload)
        return payload

    def summary(self):
        return {
            "run_id": self.run_id,
            "seed": self.seed,
            "scenario": asdict(self.scenario),
            "warmup_events": self.scenario.warmup_events,
            "measured_events": self.scenario.measured_events,
            "total_events": self.scenario.warmup_events + self.scenario.measured_events,
            "mqtt_topic": BENCHMARK_MQTT_TOPIC,
            "kafka_topic": BENCHMARK_KAFKA_TOPIC,
            "spool_prefix": f"benchmarks/wp1_5/{self.run_id}/spool/",
            "network_side_effects": False,
            "image_profiles": [asdict(profile) for profile in self.profiles],
            "stream_rate_weights": {"fast": 5, "medium": 2, "baseline": 1},
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path,
                        default=Path("tests/fixtures/notification_n0v0.json"))
    parser.add_argument("--scenario", choices=SCENARIOS, default="smoke")
    parser.add_argument("--run-id", required=True,
                        help="safe identifier used in every benchmark object prefix")
    parser.add_argument("--start", default="2026-04-15T10:00:00Z")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--profiles", type=Path,
                        default=Path("benchmarks/image_profiles.json"))
    parser.add_argument("--output", type=Path,
                        help="write deterministic NDJSON; omission prints only the plan")
    args = parser.parse_args()
    if re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id) is None:
        parser.error("--run-id must contain only letters, digits, hyphens, and underscores")
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    if start.tzinfo is None:
        parser.error("--start must include a timezone")
    workload = Workload(
        json.loads(args.fixture.read_text()), SCENARIOS[args.scenario],
        args.run_id, start, load_profiles(json.loads(args.profiles.read_text())), args.seed,
    )
    if args.output:
        with args.output.open("x") as stream:
            for event in workload.events():
                stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    print(json.dumps(workload.summary(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
