"""Versioned initial processing-stream state stored on NFS."""

from io import BytesIO
from pathlib import PurePosixPath

import numpy as np

from .contracts import identifier, prefixed_key, relative_prefix
from .nfs import publish_bytes_once


STATE_SCHEMA = "S0V0"
LATENT_DIMENSION = 2048


def initial_state_bytes():
    output = BytesIO()
    np.savez(
        output,
        schema_version=np.array(STATE_SCHEMA),
        latent_bank=np.empty((0, LATENT_DIMENSION), dtype=np.float32),
        latent_counts=np.empty((0,), dtype=np.int64),
        freshness_signature=np.empty((0,), dtype=np.float64),
        last_ingestion_timestamp=np.empty((0,), dtype="datetime64[ns]"),
    )
    return output.getvalue()


def initial_state_path(processing_stream_id, processing_image_path, path_prefix=""):
    identifier(processing_stream_id, "processing_stream_id")
    path_prefix = relative_prefix(path_prefix)
    if "_P" not in processing_stream_id:
        raise ValueError("processing stream ID has no profile suffix")
    derived_stream_id = processing_stream_id.rsplit("_P", 1)[0]
    image = PurePosixPath(processing_image_path)
    if path_prefix:
        prefix_parts = PurePosixPath(path_prefix).parts
        if image.parts[:len(prefix_parts)] != prefix_parts:
            raise ValueError("processing image does not use the configured prefix")
        image = PurePosixPath(*image.parts[len(prefix_parts):])
    if len(image.parts) < 3 or image.parts[:2] != ("images", derived_stream_id):
        raise ValueError("processing image and stream hierarchy disagree")
    relative = PurePosixPath("images", derived_stream_id, "state",
                            f"{processing_stream_id}_state_initial.npz")
    return PurePosixPath(prefixed_key(path_prefix, str(relative)))


class InitialStatePublisher:
    def __init__(self, nfs_root, path_prefix=""):
        self.nfs_root = nfs_root
        self.path_prefix = relative_prefix(path_prefix)
        self.content = initial_state_bytes()

    def ensure(self, processing_stream_id, processing_image_path):
        relative = initial_state_path(
            processing_stream_id, processing_image_path, self.path_prefix
        )
        path, created = publish_bytes_once(self.nfs_root, str(relative), self.content)
        if not created:
            with np.load(path, allow_pickle=False) as state:
                if (str(state["schema_version"]) != STATE_SCHEMA
                        or state["latent_bank"].shape != (0, LATENT_DIMENSION)
                        or state["latent_counts"].shape != (0,)
                        or state["freshness_signature"].shape != (0,)
                        or state["last_ingestion_timestamp"].shape != (0,)):
                    raise ValueError("Existing initial state has an incompatible schema")
        return path, created
