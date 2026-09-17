"""Deterministic V0 stream-generation rules, independent of storage."""

from fractions import Fraction
import hashlib
import json

from .contracts import identifier, positive_integer


def partition_for(stream_id: str, partition_count: int = 50) -> int:
    """Return the stable V0 partition; persist it when creating a stream."""
    identifier(stream_id, "processing_stream_id")
    positive_integer(partition_count, "partition_count")
    return int.from_bytes(hashlib.sha256(stream_id.encode()).digest(), "big") % partition_count


def slice_intervals(width, height):
    """Return half-open pixel intervals using the architecture's V0 ratios.

    Shared rational boundaries are floored once to obtain deterministic pixels.
    Tiny images for which rounding produces an empty crop are rejected.
    """
    positive_integer(width, "derived width")
    positive_integer(height, "derived height")
    ratio = Fraction(width, height)
    if ratio <= Fraction(9, 5):
        return ((0, width),)
    if ratio < Fraction(59, 30):
        crop = int(Fraction(9, 5) * height)
        left = (width - crop) // 2
        return ((left, left + crop),)
    intervals = []
    start = Fraction(0)
    nominal = Fraction(4, 3) * height
    while width - start - nominal >= height:
        intervals.append((start, start + nominal))
        start += nominal
    remaining = (width - start) / height
    if remaining <= Fraction(9, 5):
        intervals.append((start, Fraction(width)))
    elif remaining < Fraction(59, 30):
        # This branch always has a preceding nominal interval.
        start += Fraction(height, 6)
        intervals[-1] = (intervals[-1][0], start)
        intervals.append((start, Fraction(width)))
    else:
        intervals.extend(((start, start + nominal), (width - nominal, Fraction(width))))
    rounded = tuple((int(left), int(right)) for left, right in intervals)
    if any(right <= left for left, right in rounded):
        raise ValueError("Image too small for V0 slicing")
    return rounded


def stream_metadata(payload):
    """Keep standardized stream/site fields; provider-specific JSON stays in sidecars."""
    return {
        level: {key: value for key, value in payload.get(level, {}).items()
                if key != "provider_metadata"}
        for level in ("network", "site", "source_stream")
    }


def fingerprint(metadata):
    return hashlib.sha256(json.dumps(metadata, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()
