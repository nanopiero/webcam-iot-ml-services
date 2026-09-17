"""Acquisition eligibility and solar-phase policies without persistence effects."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math

from astral import Observer
from astral.sun import sun

from .contracts import Notification, utc_timestamp


class PolicyError(ValueError):
    """The configured policy cannot classify a valid notification."""


ARCTIC_CIRCLE_LATITUDE = 66.56
DEFAULT_MAX_PROCESSING_LATITUDE = ARCTIC_CIRCLE_LATITUDE + 2.0


def automatic_status(metadata, maximum_processing_latitude=DEFAULT_MAX_PROCESSING_LATITUDE):
    """Return the pilot status derived from retained webcam metadata."""
    if "indoor" in metadata["source_stream"].get("tags", []):
        return "blacklist"
    source_latitude = metadata["source_stream"].get("latitude")
    site_latitude = metadata["site"].get("latitude")
    latitude = source_latitude if source_latitude is not None else site_latitude
    if type(latitude) in (int, float) and latitude > maximum_processing_latitude:
        return "blacklist"
    return "whitelist"


@dataclass(frozen=True)
class SolarClassification:
    phase: int
    reference_timestamp: datetime
    sunrise: datetime
    sunset: datetime
    sunrise_offset_seconds: float
    sunset_offset_seconds: float


@dataclass(frozen=True)
class AcquisitionDecision:
    archive: bool
    publish_jobs: bool
    reason: str


_TIMESTAMP_FIELDS = frozenset(
    ("download_timestamp", "provider_update_timestamp", "publication_timestamp")
)


def reference_timestamp(notification: Notification, timestamp_fields, default_field):
    """Select a timestamp using the configured network-specific field."""
    field = timestamp_fields.get(notification.network_id, default_field)
    if field not in _TIMESTAMP_FIELDS:
        raise PolicyError(f"unsupported timestamp field for {notification.network_id}: {field}")
    value = notification.payload["timestamps"].get(field)
    if value is None:
        raise PolicyError(
            f"timestamp {field} is unavailable for network {notification.network_id}"
        )
    try:
        return utc_timestamp(value, field)
    except ValueError as exc:
        raise PolicyError(str(exc)) from exc


def coordinates(notification: Notification):
    """Prefer source-stream coordinates, then the parent site coordinates."""
    for level in ("source_stream", "site"):
        metadata = notification.payload.get(level, {})
        latitude, longitude = metadata.get("latitude"), metadata.get("longitude")
        if latitude is None or longitude is None:
            continue
        if (type(latitude) not in (int, float) or type(longitude) not in (int, float)
                or not math.isfinite(latitude) or not math.isfinite(longitude)
                or not -90 <= latitude <= 90 or not -180 <= longitude <= 180):
            raise PolicyError(f"invalid {level} coordinates")
        return float(latitude), float(longitude)
    raise PolicyError("solar classification requires latitude and longitude")


def classify_solar(notification, timestamp_fields, default_field, transition_margin_seconds):
    """Apply the pilot day/transition/night rule from the architecture."""
    if (type(transition_margin_seconds) not in (int, float)
            or not math.isfinite(transition_margin_seconds)
            or transition_margin_seconds < 0):
        raise PolicyError("transition margin must be a finite non-negative duration")
    reference = reference_timestamp(notification, timestamp_fields, default_field)
    latitude, longitude = coordinates(notification)
    try:
        events = sun(
            Observer(latitude=latitude, longitude=longitude),
            date=reference.date(),
            tzinfo=timezone.utc,
        )
        sunrise = events["sunrise"]
        sunset = events["sunset"]
    except ValueError as exc:
        # Polar day/night needs an explicit offset convention before jobs can
        # carry the architecture's required finite sunrise/sunset offsets.
        raise PolicyError("sunrise or sunset is undefined for this date and location") from exc

    margin = timedelta(seconds=float(transition_margin_seconds))
    if reference < sunrise or reference > sunset:
        phase = 0
    elif reference < sunrise + margin or reference > sunset - margin:
        phase = 2
    else:
        phase = 1
    return SolarClassification(
        phase=phase,
        reference_timestamp=reference,
        sunrise=sunrise,
        sunset=sunset,
        sunrise_offset_seconds=(reference - sunrise).total_seconds(),
        sunset_offset_seconds=(reference - sunset).total_seconds(),
    )


def acquisition_decision(status, solar_phase):
    """Map stream status and solar phase to archive/publication actions."""
    if status == "blacklist":
        return AcquisitionDecision(False, False, "blacklisted")
    if status == "greylist":
        return AcquisitionDecision(True, False, "greylisted")
    if status != "whitelist":
        raise PolicyError(f"unsupported operational status: {status}")
    if solar_phase == 0:
        return AcquisitionDecision(True, False, "nighttime")
    if solar_phase in (1, 2):
        return AcquisitionDecision(True, True, "eligible")
    raise PolicyError(f"unsupported solar phase: {solar_phase}")
