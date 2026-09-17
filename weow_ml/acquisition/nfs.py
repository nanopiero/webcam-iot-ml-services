"""Atomic publication of complete processing objects on the shared NFS."""

import os
from pathlib import Path, PurePosixPath
import tempfile


class NFSPublicationError(ValueError):
    """A requested NFS destination is invalid or cannot be published safely."""


def _destination(root, relative_path):
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or not relative.parts or any(
        part in ("", ".", "..") for part in relative.parts
    ):
        raise NFSPublicationError("NFS destination must be a safe relative path")
    root_path = Path(root).resolve(strict=True)
    destination = root_path.joinpath(*relative.parts)
    return root_path, destination


def publish_bytes(root, relative_path, content, mode=0o640):
    """Publish bytes through a temporary file and atomic same-directory rename.

    A successful return means the file was closed, renamed to its final path,
    and the containing directory was synchronized. A failed write never exposes
    a partial final path and removes its temporary file when possible.
    """
    if not isinstance(content, bytes):
        raise TypeError("NFS publication content must be bytes")
    root_path, destination = _destination(root, relative_path)
    destination.parent.mkdir(mode=0o2775, parents=True, exist_ok=True)
    if root_path not in destination.parents:
        raise NFSPublicationError("NFS destination escapes the configured root")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix="." + destination.name + ".", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as file:
            descriptor = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, destination)
        directory_descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return destination


def publish_bytes_once(root, relative_path, content, mode=0o640):
    """Atomically create an immutable file, returning False if it exists."""
    if not isinstance(content, bytes):
        raise TypeError("NFS publication content must be bytes")
    root_path, destination = _destination(root, relative_path)
    destination.parent.mkdir(mode=0o2775, parents=True, exist_ok=True)
    if root_path not in destination.parents:
        raise NFSPublicationError("NFS destination escapes the configured root")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix="." + destination.name + ".", suffix=".tmp", dir=destination.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as file:
            descriptor = -1
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            return destination, False
        directory_descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return destination, True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
