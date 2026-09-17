"""Compatibility imports for Acquisition stream-generation rules."""

from .acquisition.streams import fingerprint, partition_for, slice_intervals, stream_metadata

__all__ = ("fingerprint", "partition_for", "slice_intervals", "stream_metadata")
