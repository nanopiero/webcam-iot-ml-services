"""Acquisition contracts. No network or persistence side effects."""

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Mapping, Optional


class ContractError(ValueError):
    """An upstream notification does not satisfy the acquisition contract."""


def identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ContractError(f"{field} must be a non-empty safe identifier")
    return value


def positive_integer(value: Any, field: str) -> int:
    if type(value) is not int or value <= 0:
        raise ContractError(f"{field} must be a positive integer")
    return value


def utc_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ContractError(f"{field} must be a timestamp string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"{field} is not a valid timestamp") from exc
    if result.tzinfo is None:
        raise ContractError(f"{field} must include a timezone")
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class ProcessingProfile:
    width: int
    height: int
    colour_mode: str
    colour_depth: Optional[int]

    @property
    def fingerprint(self) -> str:
        # Explicit null distinguishes unknown depth from a later observed value.
        value = [self.width, self.height, self.colour_mode, self.colour_depth]
        return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class Notification:
    image_id: str
    derived_stream_id: str
    network_id: str
    bucket: str
    object_key: str
    download_timestamp: datetime
    profile: ProcessingProfile
    image_signature: float
    jpeg_quality: int
    panoramic: bool
    payload: Mapping[str, Any]

    @property
    def archive_key(self) -> str:
        # The archive hierarchy is derived from the ingestion filename.
        date = datetime.strptime(self.image_id[:16], "%Y%m%dT%H%M%SZ")
        return f"images/{self.derived_stream_id}/{date:%Y/%m/%d/%H}/{self.image_id}"

    @property
    def sidecar_key(self) -> str:
        return self.archive_key[:-4] + ".json"

    def summary(self) -> dict:
        return {
            "image_id": self.image_id,
            "derived_stream_id": self.derived_stream_id,
            "network_id": self.network_id,
            "archive_key": self.archive_key,
            "sidecar_key": self.sidecar_key,
            "profile_fingerprint": self.profile.fingerprint,
            "source_colour_depth": self.profile.colour_depth,
            "jpeg_quality": self.jpeg_quality,
            "panoramic": self.panoramic,
        }


def parse_notification(payload: Mapping[str, Any]) -> Notification:
    """Validate N0V0 while preserving unknown fields for the archive sidecar.

    MQTT DUP is deliberately not an input: it is not proof of prior processing.
    """
    try:
        if payload["schema_version"] != "N0V0":
            raise ContractError("unsupported schema_version")
        stream = identifier(payload["derived_stream"]["derived_stream_id"], "derived_stream_id")
        network = identifier(payload["network"]["network_id"], "network_id")
        image_id = payload["image_id"]
        if not isinstance(image_id, str) or not re.fullmatch(
            r"\d{8}T\d{6}Z_" + re.escape(stream) + r"\.jpg", image_id
        ):
            raise ContractError("image_id must contain its download time and derived stream ID")
        filename_time = datetime.strptime(image_id[:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        download = utc_timestamp(payload["timestamps"]["download_timestamp"], "download_timestamp")
        if filename_time != download.replace(microsecond=0):
            raise ContractError("image_id and download_timestamp disagree")
        for field in ("provider_update_timestamp", "publication_timestamp"):
            value = payload["timestamps"].get(field)
            if value is not None:
                utc_timestamp(value, field)
        storage = payload["storage"]
        if storage["type"] != "s3":
            raise ContractError("storage.type must be s3")
        bucket, key = storage["bucket"], storage["object_key"]
        if not isinstance(bucket, str) or not bucket.strip():
            raise ContractError("storage.bucket must be non-empty")
        if (not isinstance(key, str) or key.startswith("/") or "\\" in key
                or any(part in ("", ".", "..") for part in key.split("/"))
                or PurePosixPath(key).name != image_id):
            raise ContractError("storage.object_key must be a relative key ending in image_id")
        source = payload["source_image"]
        depth = source.get("colour_depth")
        if depth is not None:
            positive_integer(depth, "source_image.colour_depth")
        mode = source["colour_mode"]
        if not isinstance(mode, str) or not mode:
            raise ContractError("source_image.colour_mode must be non-empty")
        profile = ProcessingProfile(
            positive_integer(source["width"], "source_image.width"),
            positive_integer(source["height"], "source_image.height"), mode, depth,
        )
        for field in ("width", "height"):
            positive_integer(payload["derived_image"][field], "derived_image." + field)
        transformation = payload["derived_stream"]["transformation_metadata"]
        signature = transformation["image_signature"]
        if type(signature) not in (int, float) or not math.isfinite(signature):
            raise ContractError("image_signature must be a finite number")
        quality = transformation["jpeg_quality"]
        if type(quality) is not int or not 1 <= quality <= 95:
            raise ContractError("jpeg_quality must be an integer from 1 to 95")
        panoramic = transformation["panoramic"]
        if type(panoramic) is not bool:
            raise ContractError("panoramic must be a boolean")
        # Snapshot rather than retain the caller's mutable metadata dictionary.
        retained = json.loads(json.dumps(payload, allow_nan=False))
        return Notification(image_id, stream, network, bucket, key, download,
                            profile, float(signature), quality, panoramic, retained)
    except ContractError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
        raise ContractError(f"invalid N0V0 notification: {exc}") from exc
