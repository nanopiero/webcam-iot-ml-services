"""Publish a paced, isolated WP1.5 workload after its test spool is prepared."""

import argparse
from collections import deque
from datetime import datetime
import json
import os
from pathlib import Path
import threading
import time

import paho.mqtt.client as mqtt

from ..common.config import load_config
from .guard import validate_benchmark_settings
from .images import load_profiles
from .workload import SCENARIOS, Workload


def connect_client(client, settings, timeout_seconds):
    connected = threading.Event()
    failure = {}

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            failure["reason"] = str(reason_code)
        else:
            connected.set()

    client.on_connect = on_connect
    client.connect(settings["host"], settings["port"], keepalive=30)
    client.loop_start()
    if not connected.wait(timeout_seconds):
        client.loop_stop()
        reason = failure.get("reason", "timeout")
        raise ConnectionError("benchmark MQTT connection failed: " + reason)


def validate_spool_receipt(receipt, workload, bucket):
    summary = workload.summary()
    expected = {
        "run_id": summary["run_id"],
        "seed": summary["seed"],
        "scenario": workload.scenario.name,
        "start": workload.start.isoformat(),
        "bucket": bucket,
        "objects": summary["total_events"],
        "spool_prefix": summary["spool_prefix"],
    }
    for field, value in expected.items():
        if receipt.get(field) != value:
            raise ValueError(f"spool receipt {field} does not match the workload")


def publish_workload(client, topic, workload, timeout_seconds=10, max_inflight=200,
                     monotonic=None, sleep=None):
    if max_inflight <= 0:
        raise ValueError("max_inflight must be positive")
    monotonic = monotonic or time.monotonic
    sleep = sleep or time.sleep
    pending = deque()
    first_timestamp = None
    started = monotonic()
    maximum_lag = 0.0
    published = 0

    def await_one():
        info = pending.popleft()
        info.wait_for_publish(timeout=timeout_seconds)
        if not info.is_published():
            raise TimeoutError("MQTT publication acknowledgement timed out")

    for payload in workload.events():
        timestamp = datetime.fromisoformat(
            payload["timestamps"]["download_timestamp"].replace("Z", "+00:00")
        )
        if first_timestamp is None:
            first_timestamp = timestamp
        target = started + (timestamp - first_timestamp).total_seconds()
        delay = target - monotonic()
        if delay > 0:
            sleep(delay)
        maximum_lag = max(maximum_lag, monotonic() - target)
        info = client.publish(
            topic, json.dumps(payload, separators=(",", ":")), qos=1, retain=False
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError("MQTT client rejected benchmark publication")
        pending.append(info)
        published += 1
        if len(pending) >= max_inflight:
            await_one()
    while pending:
        await_one()
    return {
        "published": published,
        "elapsed_seconds": monotonic() - started,
        "maximum_schedule_lag_seconds": maximum_lag,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--secrets-dir", type=Path, default=Path(".secrets"))
    parser.add_argument("--fixture", type=Path,
                        default=Path("tests/fixtures/notification_n0v0.json"))
    parser.add_argument("--profiles", type=Path,
                        default=Path("benchmarks/image_profiles.json"))
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--start", required=True,
                        help="must exactly match the prepared spool workload")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--max-inflight", type=int, default=200)
    parser.add_argument("--receipt", type=Path, required=True,
                        help="receipt written by benchmark.spool for this exact workload")
    args = parser.parse_args()

    settings = load_config(args.config, args.secrets_dir)
    database_file = settings.get("postgres", {}).get("settings_file")
    if database_file != "database.benchmark.json":
        parser.error("benchmark must use database.benchmark.json")
    database_document = json.loads((args.secrets_dir / database_file).read_text())
    guard = validate_benchmark_settings(settings, database_document)
    if guard is None:
        parser.error("benchmark mode must be enabled")
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    if start.tzinfo is None:
        parser.error("--start must include a timezone")
    spool = settings["spool_s3"]
    workload = Workload(
        json.loads(args.fixture.read_text()), SCENARIOS[args.scenario], guard.run_id, start,
        load_profiles(json.loads(args.profiles.read_text())), seed=args.seed,
        spool_bucket=spool["bucket"],
    )
    validate_spool_receipt(json.loads(args.receipt.read_text()), workload, spool["bucket"])
    mqtt_settings = settings["mqtt"]
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id=mqtt_settings["client_id"],
        protocol=mqtt.MQTTv5,
    )
    if mqtt_settings.get("tls"):
        client.tls_set()
    username_env = mqtt_settings.get("username_env")
    password_env = mqtt_settings.get("password_env")
    if username_env:
        client.username_pw_set(
            os.environ[username_env], os.environ[password_env] if password_env else None
        )
    client.max_inflight_messages_set(args.max_inflight)
    connect_client(
        client, mqtt_settings, settings["acquisition"]["operation_timeout_seconds"]
    )
    try:
        result = publish_workload(
            client, mqtt_settings["topic"], workload,
            timeout_seconds=settings["acquisition"]["operation_timeout_seconds"],
            max_inflight=args.max_inflight,
        )
    finally:
        client.disconnect()
        client.loop_stop()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
