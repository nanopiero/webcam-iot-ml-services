"""Prepare the isolated S3 test spool for one deterministic WP1.5 run."""

import argparse
from datetime import datetime
import json
from pathlib import Path

from ..acquisition.archive import put_verified, s3_client
from ..common.config import load_config
from .guard import validate_benchmark_settings
from .images import fixture_jpeg, load_profiles
from .workload import SCENARIOS, Workload


def prepare_spool(client, bucket, workload):
    fixtures = {profile.name: fixture_jpeg(profile) for profile in workload.profiles}
    objects = total_bytes = 0
    for payload in workload.events():
        storage = payload["storage"]
        if storage["bucket"] != bucket:
            raise ValueError("notification bucket differs from benchmark spool bucket")
        content = fixtures[payload["benchmark"]["image_profile"]]
        if len(content) != payload["derived_image"]["size_bytes"]:
            raise ValueError("fixture size differs from notification")
        put_verified(client, bucket, storage["object_key"], content, "image/jpeg")
        objects += 1
        total_bytes += len(content)
    return {"objects": objects, "bytes": total_bytes,
            "spool_prefix": workload.summary()["spool_prefix"]}


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
                        help="UTC start used in object identities, for example 2026-09-19T10:00:00Z")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--receipt", type=Path, required=True,
                        help="new local JSON receipt required by the publisher")
    args = parser.parse_args()

    settings = load_config(args.config, args.secrets_dir)
    run_id = settings.get("benchmark", {}).get("run_id")
    guard = validate_benchmark_settings(settings, {"dbname": "weow_ml_benchmark"})
    if guard is None:
        parser.error("benchmark mode must be enabled")
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    if start.tzinfo is None:
        parser.error("--start must include a timezone")
    spool = settings["spool_s3"]
    workload = Workload(
        json.loads(args.fixture.read_text()), SCENARIOS[args.scenario], run_id, start,
        load_profiles(json.loads(args.profiles.read_text())), seed=args.seed,
        spool_bucket=spool["bucket"],
    )
    client = s3_client(
        spool["endpoint_url"], spool["access_key_file"], spool["secret_key_file"],
        settings.get("s3_ca_bundle"),
    )
    result = prepare_spool(client, spool["bucket"], workload)
    receipt = {
        **result, "run_id": run_id, "scenario": args.scenario,
        "start": start.isoformat(), "seed": args.seed, "bucket": spool["bucket"],
    }
    with args.receipt.open("x") as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
