"""Bounded spool-to-archive transfer, without inference or Kafka publication."""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .contracts import parse_notification, prefixed_key


def s3_client(endpoint, access_file, secret_file, ca_bundle=None):
    if not endpoint.startswith("https://"):
        raise ValueError("S3 endpoint must use HTTPS")
    access = Path(access_file).read_text().strip()
    secret = Path(secret_file).read_text().strip()
    if not access or not secret:
        raise ValueError("S3 credential file is empty")
    return boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access,
        aws_secret_access_key=secret, region_name="us-east-1",
        verify=ca_bundle or True,
        config=Config(signature_version="s3v4", connect_timeout=10, read_timeout=30,
                      max_pool_connections=32,
                      retries={"max_attempts": 1, "mode": "standard"},
                      s3={"addressing_style": "path"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def read_object(client, bucket, key, limit=10 * 1024 * 1024):
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    try:
        data = body.read(limit + 1)
        if len(data) > limit:
            raise ValueError("Object exceeds bounded transfer size")
        return data
    finally:
        body.close()


def put_verified(client, bucket, key, data, content_type):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type,
                          IfNoneMatch="*")
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise
        # Do not overwrite an existing object; check that the image is identical.
        if read_object(client, bucket, key) != data:
            raise ValueError("Archive object already exists with different content") from exc
    if read_object(client, bucket, key) != data:
        raise ValueError("Archive read-back verification failed")


def fetch_image(source, notification):
    """Retrieve one complete JPEG from the ingestion spool."""
    image = read_object(source, notification.bucket, notification.object_key)
    if not image.startswith(b"\xff\xd8") or not image.endswith(b"\xff\xd9"):
        raise ValueError("Spool object is not a complete JPEG byte stream")
    return image


def archive_image(destination, bucket, notification, image, acquisition, key_prefix=""):
    """Store a verified image and its immutable acquisition sidecar."""
    digest = hashlib.sha256(image).hexdigest()
    archive_key = prefixed_key(key_prefix, notification.archive_key)
    sidecar_key = prefixed_key(key_prefix, notification.sidecar_key)
    put_verified(destination, bucket, archive_key, image, "image/jpeg")
    sidecar = {
        "notification": notification.payload,
        "acquisition": dict(acquisition, image_sha256=digest),
    }
    sidecar_data = json.dumps(sidecar, ensure_ascii=False, allow_nan=False,
                              sort_keys=True, indent=2).encode("utf-8")
    try:
        existing = read_object(destination, bucket, sidecar_key)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        put_verified(destination, bucket, sidecar_key, sidecar_data, "application/json")
    else:
        stored = json.loads(existing)
        if (stored.get("notification") != notification.payload
                or stored.get("acquisition", {}).get("image_sha256") != digest):
            raise ValueError("Existing sidecar does not match this notification/image")
    return {
        "archive_key": archive_key,
        "sidecar_key": sidecar_key,
        "bytes": len(image),
        "sha256": digest,
    }


def archive_notification(source, destination, bucket, payload):
    notification = parse_notification(payload)
    if "indoor" in payload.get("source_stream", {}).get("tags", []):
        return {"image_id": notification.image_id, "status": "excluded_indoor"}
    image = fetch_image(source, notification)
    # Diagnostic archive only: do not invent registry IDs or an MQTT receipt time.
    archived = archive_image(destination, bucket, notification, image, {
        "mode": "archive_validation",
        "archived_at": datetime.now(timezone.utc).isoformat(),
        "acquisition_timestamp": None,
        "processing_stream_ids": None,
        "processing_context_status": "not_resolved",
    })
    return {"image_id": notification.image_id, "status": "verified",
            **archived}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("notifications", type=Path, nargs="+")
    parser.add_argument("--spool-endpoint", required=True)
    parser.add_argument("--archive-endpoint", required=True)
    parser.add_argument("--archive-bucket", required=True)
    parser.add_argument("--secrets-dir", type=Path, default=Path(".secrets"))
    args = parser.parse_args()
    try:
        source = s3_client(args.spool_endpoint, args.secrets_dir / "ingestion_s3_access_key",
                           args.secrets_dir / "ingestion_s3_secret_key")
        destination = s3_client(args.archive_endpoint, args.secrets_dir / "weows_s3_access_key",
                                args.secrets_dir / "weows_s3_secret_key")
        for path in args.notifications:
            result = archive_notification(source, destination, args.archive_bucket,
                                          json.loads(path.read_text()))
            print(json.dumps(result), flush=True)
    except ClientError as exc:
        print("S3 operation failed: " + exc.response["Error"]["Code"], file=sys.stderr)
        return 2
    except (BotoCoreError, OSError, ValueError) as exc:
        # Never print credential material or HTTP debug traces.
        print("Archive transfer failed (" + type(exc).__name__ + ")", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
