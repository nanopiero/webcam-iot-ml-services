"""Bounded spool-to-archive transfer, without inference or Kafka publication."""

import argparse
import gzip
import hashlib
import json
import struct
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


def put_if_absent(client, bucket, key, data, content_type, metadata=None):
    """Return True after an acknowledged create, False when the key exists."""
    try:
        arguments = {
            "Bucket": bucket, "Key": key, "Body": data,
            "ContentType": content_type, "IfNoneMatch": "*",
        }
        if metadata is not None:
            arguments["Metadata"] = metadata
        client.put_object(**arguments)
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise
        return False
    return True


def put_verified(client, bucket, key, data, content_type):
    """Create once, or verify that an existing immutable object is identical."""
    if not put_if_absent(client, bucket, key, data, content_type):
        if read_object(client, bucket, key) != data:
            raise ValueError("Archive object already exists with different content")


def fetch_image(source, notification):
    """Retrieve one complete JPEG from the ingestion spool."""
    image = read_object(source, notification.bucket, notification.object_key)
    if not image.startswith(b"\xff\xd8") or not image.endswith(b"\xff\xd9"):
        raise ValueError("Spool object is not a complete JPEG byte stream")
    return image


_SIDECAR_MAGIC = b"WEOWSC1"
_SIDECAR_HEADER = struct.Struct(">7sII")
_APP15_PAYLOAD_LIMIT = 65533


def bundle_image_sidecar(image, sidecar_data):
    """Embed a compressed JSON sidecar in JPEG APP15 segments."""
    if not image.startswith(b"\xff\xd8") or not image.endswith(b"\xff\xd9"):
        raise ValueError("Archive image is not a complete JPEG byte stream")
    compressed = gzip.compress(sidecar_data, mtime=0)
    chunk_size = _APP15_PAYLOAD_LIMIT - _SIDECAR_HEADER.size
    chunks = tuple(compressed[offset:offset + chunk_size]
                   for offset in range(0, len(compressed), chunk_size)) or (b"",)
    segments = []
    for index, chunk in enumerate(chunks):
        payload = _SIDECAR_HEADER.pack(_SIDECAR_MAGIC, index, len(chunks)) + chunk
        segments.append(b"\xff\xef" + struct.pack(">H", len(payload) + 2) + payload)
    return image[:2] + b"".join(segments) + image[2:]


def unbundle_image_sidecar(bundle):
    """Recover the exact source JPEG and JSON sidecar from an archive object."""
    if not bundle.startswith(b"\xff\xd8"):
        raise ValueError("Archive object is not a JPEG bundle")
    offset = 2
    chunks = []
    expected_count = None
    while bundle[offset:offset + 2] == b"\xff\xef":
        if offset + 4 > len(bundle):
            raise ValueError("Truncated archive sidecar segment")
        segment_length = struct.unpack(">H", bundle[offset + 2:offset + 4])[0]
        end = offset + 2 + segment_length
        payload = bundle[offset + 4:end]
        if end > len(bundle) or len(payload) < _SIDECAR_HEADER.size:
            raise ValueError("Truncated archive sidecar segment")
        magic, index, count = _SIDECAR_HEADER.unpack(
            payload[:_SIDECAR_HEADER.size]
        )
        if magic != _SIDECAR_MAGIC:
            break
        if index != len(chunks) or count == 0 or (expected_count not in (None, count)):
            raise ValueError("Invalid archive sidecar segment sequence")
        expected_count = count
        chunks.append(payload[_SIDECAR_HEADER.size:])
        offset = end
    if expected_count is None or len(chunks) != expected_count:
        raise ValueError("Archive sidecar segments are missing")
    try:
        sidecar = json.loads(gzip.decompress(b"".join(chunks)))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("Archive sidecar is invalid") from exc
    return bundle[:2] + bundle[offset:], sidecar


def archive_image(destination, bucket, notification, image, acquisition, key_prefix=""):
    """Store an image and its immutable sidecar in one conditional S3 PUT."""
    digest = hashlib.sha256(image).hexdigest()
    archive_key = prefixed_key(key_prefix, notification.archive_key)
    sidecar = {
        "notification": notification.payload,
        "acquisition": dict(acquisition, image_sha256=digest),
    }
    sidecar_data = json.dumps(sidecar, ensure_ascii=False, allow_nan=False,
                              sort_keys=True, indent=2).encode("utf-8")
    bundle = bundle_image_sidecar(image, sidecar_data)
    if not put_if_absent(destination, bucket, archive_key, bundle, "image/jpeg"):
        existing_image, stored = unbundle_image_sidecar(
            read_object(destination, bucket, archive_key, limit=len(bundle))
        )
        if (existing_image != image
                or stored.get("notification") != notification.payload
                or stored.get("acquisition", {}).get("image_sha256") != digest):
            raise ValueError("Existing archive object does not match this image/sidecar")
    return {
        "archive_key": archive_key,
        "sidecar_key": None,
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
