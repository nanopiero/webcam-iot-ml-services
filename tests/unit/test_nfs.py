import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from weow_ml.acquisition.nfs import NFSPublicationError, publish_bytes


class NFSPublicationTests(unittest.TestCase):
    def test_complete_content_replaces_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            first = publish_bytes(directory, "images/stream/hour/image.jpg", b"first")
            second = publish_bytes(directory, "images/stream/hour/image.jpg", b"complete")
            self.assertEqual(first, second)
            self.assertEqual(second.read_bytes(), b"complete")
            self.assertEqual(list(second.parent.glob(".*.tmp")), [])
            self.assertEqual(second.stat().st_mode & 0o777, 0o640)

    def test_unsafe_destinations_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for path in ("/absolute.jpg", "../escape.jpg", "images/../escape.jpg"):
                with self.subTest(path=path), self.assertRaises(NFSPublicationError):
                    publish_bytes(directory, path, b"content")

    def test_failed_rename_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "state" / "initial.npz"
            with patch.object(os, "replace", side_effect=OSError("rename failed")):
                with self.assertRaises(OSError):
                    publish_bytes(directory, "state/initial.npz", b"state")
            self.assertFalse(target.exists())
            self.assertEqual(list(target.parent.glob(".*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
