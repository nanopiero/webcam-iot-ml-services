from io import BytesIO
from pathlib import Path
import tempfile
import unittest

import numpy as np

from weow_ml.acquisition.state import (
    InitialStatePublisher,
    LATENT_DIMENSION,
    STATE_SCHEMA,
    initial_state_bytes,
    initial_state_path,
)


class InitialStateTests(unittest.TestCase):
    def test_path_is_stable_across_image_hours(self):
        stream = "test_P0S0V0"
        first = initial_state_path(stream, "images/test/2026/04/15/10/a_P0S0V0.jpg")
        later = initial_state_path(stream, "images/test/2026/04/16/11/b_P0S0V0.jpg")
        self.assertEqual(first, later)
        self.assertEqual(str(first), "images/test/state/test_P0S0V0_state_initial.npz")

    def test_path_preserves_configured_benchmark_prefix(self):
        prefix = "benchmarks/wp1_5/run_001"
        image = prefix + "/images/test/2026/04/15/10/a_P0S0V0.jpg"
        state = initial_state_path("test_P0S0V0", image, prefix)
        self.assertEqual(
            str(state), prefix + "/images/test/state/test_P0S0V0_state_initial.npz"
        )
        with self.assertRaisesRegex(ValueError, "configured prefix"):
            initial_state_path(
                "test_P0S0V0", "images/test/2026/04/15/10/a_P0S0V0.jpg", prefix
            )

    def test_contract_is_pickle_free_and_empty(self):
        with np.load(BytesIO(initial_state_bytes()), allow_pickle=False) as state:
            self.assertEqual(str(state["schema_version"]), STATE_SCHEMA)
            self.assertEqual(state["latent_bank"].shape, (0, LATENT_DIMENSION))
            self.assertEqual(state["latent_bank"].dtype, np.dtype("float32"))
            self.assertEqual(state["latent_counts"].shape, (0,))
            self.assertEqual(state["freshness_signature"].shape, (0,))
            self.assertEqual(state["last_ingestion_timestamp"].shape, (0,))

    def test_publication_is_idempotent_and_does_not_replace(self):
        with tempfile.TemporaryDirectory() as root:
            publisher = InitialStatePublisher(root)
            image = "images/test/2026/04/15/10/image_P0S0V0.jpg"
            first, created = publisher.ensure("test_P0S0V0", image)
            original = first.read_bytes()
            second, created_again = publisher.ensure("test_P0S0V0", image)
            self.assertTrue(created)
            self.assertFalse(created_again)
            self.assertEqual(first, second)
            self.assertEqual(second.read_bytes(), original)

    def test_incompatible_existing_state_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            publisher = InitialStatePublisher(root)
            image = "images/test/2026/04/15/10/image_P0S0V0.jpg"
            path, _ = publisher.ensure("test_P0S0V0", image)
            path.write_bytes(b"not an npz")
            with self.assertRaises(ValueError):
                publisher.ensure("test_P0S0V0", image)


if __name__ == "__main__":
    unittest.main()
