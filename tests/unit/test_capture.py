from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    from weow_ml import capture
except ModuleNotFoundError as exc:
    if exc.name not in ("paho", "paho.mqtt", "paho.mqtt.client"):
        raise
    capture = None


@unittest.skipIf(capture is None, "install requirements-mqtt.txt for capture tests")
class CaptureTests(unittest.TestCase):
    settings = {"host": "example.invalid", "port": 1883,
                "protocol": "5", "topic": "webcam/T0", "qos": 1}

    def test_live_message_saved_and_validated(self):
        fixture = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"
        payload = fixture.read_bytes()
        with tempfile.TemporaryDirectory() as directory, patch.object(capture.mqtt, "Client") as factory:
            client = factory.return_value
            client.loop.side_effect = lambda **kwargs: (
                client.on_message(client, None, SimpleNamespace(retain=False, payload=payload)) or 0
            )
            result = capture.capture(self.settings, Path(directory), 1, 5)
            self.assertEqual(result["valid"], 1)
            files = list(Path(directory).glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertEqual(files[0].read_bytes(), payload)
            self.assertEqual(files[0].stat().st_mode & 0o777, 0o600)
            client.disconnect.assert_called_once()
            self.assertTrue(factory.call_args.kwargs["client_id"].startswith("weow-contract-"))

    def test_invalid_payload_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(capture.mqtt, "Client") as factory:
            client = factory.return_value
            client.loop.side_effect = lambda **kwargs: (
                client.on_message(client, None, SimpleNamespace(retain=False, payload=b"not JSON")) or 0
            )
            result = capture.capture(self.settings, Path(directory), 1, 5)
            self.assertEqual(result["invalid"], 1)
            self.assertEqual(result["valid"], 0)

    def test_network_failure_disconnects(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(capture.mqtt, "Client") as factory:
            factory.return_value.loop.return_value = capture.mqtt.MQTT_ERR_CONN_LOST
            with self.assertRaises(ConnectionError):
                capture.capture(self.settings, Path(directory), 1, 5)
            factory.return_value.disconnect.assert_called_once()

    def test_expired_deadline_disconnects(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(capture.mqtt, "Client") as factory:
            with patch.object(capture.time, "monotonic", side_effect=[0, 6]):
                with self.assertRaises(TimeoutError):
                    capture.capture(self.settings, Path(directory), 1, 5)
            factory.return_value.disconnect.assert_called_once()
