from io import BytesIO
import json
from pathlib import Path
import unittest

from PIL import Image

from weow_ml.benchmark.images import fixture_jpeg, load_profiles


PROFILES = Path(__file__).parents[2] / "benchmarks" / "image_profiles.json"


class BenchmarkImageTests(unittest.TestCase):
    def test_every_profile_has_an_exact_decodable_fixture(self):
        profiles = load_profiles(json.loads(PROFILES.read_text()))
        for profile in profiles:
            with self.subTest(profile=profile.name):
                content = fixture_jpeg(profile)
                self.assertEqual(len(content), profile.size_bytes)
                with Image.open(BytesIO(content)) as image:
                    image.load()
                    self.assertEqual(image.format, "JPEG")
                    self.assertEqual(image.size, (profile.width, profile.height))


if __name__ == "__main__":
    unittest.main()
