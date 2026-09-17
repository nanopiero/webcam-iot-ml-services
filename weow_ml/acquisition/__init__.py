"""Acquisition contracts and deterministic stream-generation rules."""

from .contracts import ContractError, Notification, ProcessingProfile, parse_notification
from .nfs import NFSPublicationError, publish_bytes
from .streams import partition_for, slice_intervals

__all__ = (
    "ContractError",
    "Notification",
    "NFSPublicationError",
    "ProcessingProfile",
    "parse_notification",
    "partition_for",
    "publish_bytes",
    "slice_intervals",
)
