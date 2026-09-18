"""Deterministic valid JPEG fixtures with controlled transfer sizes."""

from dataclasses import dataclass
from io import BytesIO

from PIL import Image, ImageDraw


@dataclass(frozen=True)
class ImageProfile:
    name: str
    width: int
    height: int
    size_bytes: int
    panoramic: bool

    def __post_init__(self):
        if (not self.name or type(self.width) is not int or self.width <= 0
                or type(self.height) is not int or self.height <= 0
                or type(self.size_bytes) is not int or self.size_bytes <= 0
                or type(self.panoramic) is not bool):
            raise ValueError("invalid benchmark image profile")


def load_profiles(document):
    profiles = tuple(ImageProfile(**item) for item in document)
    if not profiles or not any(profile.panoramic for profile in profiles):
        raise ValueError("profiles must include at least one panoramic image")
    if not any(not profile.panoramic for profile in profiles):
        raise ValueError("profiles must include at least one standard image")
    return profiles


def _base_jpeg(profile, quality):
    image = Image.new("RGB", (profile.width, profile.height), (36, 78, 112))
    draw = ImageDraw.Draw(image)
    step = max(8, profile.height // 12)
    for y in range(0, profile.height, step):
        colour = ((y * 7) % 190 + 30, (y * 11) % 180 + 35, (y * 13) % 170 + 40)
        draw.rectangle((0, y, profile.width, min(profile.height, y + step)), fill=colour)
    for x in range(0, profile.width, max(16, profile.width // 20)):
        draw.line((x, 0, profile.width - x // 2, profile.height - 1), fill=(210, 215, 220), width=2)
    output = BytesIO()
    image.save(output, format="JPEG", quality=quality, optimize=False,
               progressive=False, subsampling=2)
    return output.getvalue()


def _pad_jpeg(data, size):
    """Insert JPEG comment segments after SOI to reach an exact byte count."""
    remaining = size - len(data)
    if remaining == 0:
        return data
    if remaining < 4:
        raise ValueError("target JPEG size is too close to its encoded size")
    segments = bytearray()
    while remaining:
        total = min(remaining, 65535)
        if remaining - total in (1, 2, 3):
            total -= 4 - (remaining - total)
        payload_size = total - 4
        segments.extend(b"\xff\xfe")
        segments.extend((payload_size + 2).to_bytes(2, "big"))
        segments.extend(b"\0" * payload_size)
        remaining -= total
    return data[:2] + bytes(segments) + data[2:]


def fixture_jpeg(profile):
    """Return a decodable JPEG matching the profile dimensions and byte size."""
    for quality in range(85, 9, -1):
        data = _base_jpeg(profile, quality)
        difference = profile.size_bytes - len(data)
        if difference == 0 or difference >= 4:
            return _pad_jpeg(data, profile.size_bytes)
    raise ValueError(f"target size is too small for benchmark profile {profile.name}")
