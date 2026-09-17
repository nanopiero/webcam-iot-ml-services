from fractions import Fraction
import unittest

from weow_ml.acquisition.policies import automatic_status

from weow_ml.acquisition.streams import partition_for, slice_intervals


class StreamRuleTests(unittest.TestCase):
    def test_automatic_blacklist_for_indoor_and_far_arctic_streams(self):
        metadata = {
            "source_stream": {"tags": [], "latitude": 68.56},
            "site": {"latitude": 70.0},
        }
        self.assertEqual(automatic_status(metadata), "whitelist")
        metadata["source_stream"]["latitude"] = 68.5601
        self.assertEqual(automatic_status(metadata), "blacklist")
        metadata["source_stream"] = {"tags": ["indoor"], "latitude": 45.0}
        self.assertEqual(automatic_status(metadata), "blacklist")

    def test_unsplit_and_centered_intervals(self):
        self.assertEqual(slice_intervals(400, 224), ((0, 400),))
        self.assertEqual(slice_intervals(440, 224), ((18, 421),))

    def test_panorama_intervals_cover_edges_and_respect_maximum_ratio(self):
        width, height = 1200, 288
        intervals = slice_intervals(width, height)
        self.assertGreater(len(intervals), 1)
        self.assertEqual(intervals[0][0], 0)
        self.assertEqual(intervals[-1][1], width)
        for left, right in intervals:
            self.assertLessEqual(Fraction(right - left, height), Fraction(9, 5))

    def test_partition_mapping_is_bounded_and_stable(self):
        stream_id = "fin12345P01T0_P0S0V0"
        self.assertEqual(partition_for(stream_id), partition_for(stream_id))
        self.assertIn(partition_for(stream_id), range(50))


if __name__ == "__main__":
    unittest.main()
