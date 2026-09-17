from io import BytesIO
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

try:
    from PIL import Image
    from weow_ml.acquisition.contracts import parse_notification
    from weow_ml.acquisition.images import (
        ProcessingImageError,
        prepare_processing_images,
        publish_processing_images,
    )
except ModuleNotFoundError as exc:
    if exc.name not in ("PIL", "PIL.Image"):
        raise
    Image = None


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"


@unittest.skipIf(Image is None, "install requirements-image.txt")
class ProcessingImageTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(FIXTURE.read_text())
        self.payload["derived_image"]["width"] = 400
        self.payload["derived_image"]["height"] = 224
        self.notification = parse_notification(self.payload)
        image = Image.new("RGB", (400, 224), (10, 20, 30))
        output = BytesIO()
        image.save(output, format="JPEG", quality=self.notification.jpeg_quality, subsampling=2)
        self.image_bytes = output.getvalue()

    def stream(self, stream_id, left, right):
        return SimpleNamespace(
            processing_stream_id=stream_id, slice_left=left, slice_right=right
        )

    def test_unsliced_processing_image_preserves_original_bytes(self):
        stream = self.stream(self.notification.derived_stream_id + "_P0S0V0", 0, 400)
        prepared = prepare_processing_images(self.image_bytes, self.notification, (stream,))
        self.assertEqual(prepared[0].content, self.image_bytes)
        self.assertTrue(prepared[0].relative_path.endswith("_P0S0V0.jpg"))

    def test_crop_is_reencoded_with_registered_dimensions_and_published(self):
        stream = self.stream(self.notification.derived_stream_id + "_P0S1V0", 20, 300)
        prepared = prepare_processing_images(self.image_bytes, self.notification, (stream,))
        with Image.open(BytesIO(prepared[0].content)) as result:
            self.assertEqual(result.size, (280, 224))
        with tempfile.TemporaryDirectory() as directory:
            paths = publish_processing_images(directory, prepared)
            self.assertEqual(paths[0].read_bytes(), prepared[0].content)

    def test_dimension_mismatch_is_rejected(self):
        stream = self.stream(self.notification.derived_stream_id + "_P0S0V0", 0, 400)
        self.payload["derived_image"]["width"] = 399
        notification = parse_notification(self.payload)
        with self.assertRaises(ProcessingImageError):
            prepare_processing_images(self.image_bytes, notification, (stream,))


if __name__ == "__main__":
    unittest.main()
