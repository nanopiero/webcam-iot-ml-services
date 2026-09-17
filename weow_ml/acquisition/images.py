"""Deterministic V0 processing-image construction and NFS publication."""

from dataclasses import dataclass
from io import BytesIO

from PIL import Image, JpegImagePlugin, UnidentifiedImageError

from .nfs import publish_bytes


class ProcessingImageError(ValueError):
    """An archived image cannot produce the registered processing slices."""


@dataclass(frozen=True)
class PreparedImage:
    processing_stream_id: str
    relative_path: str
    content: bytes


def processing_image_path(notification, processing_stream_id):
    prefix = notification.derived_stream_id + "_"
    if not processing_stream_id.startswith(prefix):
        raise ProcessingImageError("processing stream does not belong to the notification")
    suffix = processing_stream_id[len(prefix):]
    return notification.archive_key[:-4] + "_" + suffix + ".jpg"


def prepare_processing_images(image_bytes, notification, streams):
    """Return complete processing JPEGs without touching NFS."""
    try:
        with Image.open(BytesIO(image_bytes)) as source:
            if source.format != "JPEG":
                raise ProcessingImageError("processing source is not JPEG")
            source.load()
            expected = (
                notification.payload["derived_image"]["width"],
                notification.payload["derived_image"]["height"],
            )
            if source.size != expected:
                raise ProcessingImageError("decoded image dimensions differ from notification")
            sampling = JpegImagePlugin.get_sampling(source)
            prepared = []
            for stream in streams:
                if not 0 <= stream.slice_left < stream.slice_right <= source.width:
                    raise ProcessingImageError("registered slice is outside image boundaries")
                path = processing_image_path(notification, stream.processing_stream_id)
                if stream.slice_left == 0 and stream.slice_right == source.width:
                    content = image_bytes
                else:
                    slice_image = source.crop(
                        (stream.slice_left, 0, stream.slice_right, source.height)
                    )
                    output = BytesIO()
                    options = {
                        "format": "JPEG",
                        "quality": notification.jpeg_quality,
                        "optimize": False,
                        "progressive": False,
                    }
                    if sampling in (0, 1, 2):
                        options["subsampling"] = sampling
                    slice_image.save(output, **options)
                    content = output.getvalue()
                prepared.append(PreparedImage(stream.processing_stream_id, path, content))
            return tuple(prepared)
    except (KeyError, TypeError, UnidentifiedImageError, OSError) as exc:
        if isinstance(exc, ProcessingImageError):
            raise
        raise ProcessingImageError("invalid processing image or metadata") from exc


def publish_processing_images(nfs_root, prepared_images):
    return tuple(
        publish_bytes(nfs_root, image.relative_path, image.content)
        for image in prepared_images
    )
