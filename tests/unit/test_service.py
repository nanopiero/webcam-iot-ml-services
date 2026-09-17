from concurrent.futures import Future
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

import paho.mqtt.client as mqtt

from weow_ml.acquisition.service import MQTTService


FIXTURE = Path(__file__).parents[1] / "fixtures" / "notification_n0v0.json"


class Client:
    def __init__(self):
        self.acks = []
        self.disconnected = 0
        self.subscriptions = []

    def ack(self, mid, qos):
        self.acks.append((mid, qos))
        return mqtt.MQTT_ERR_SUCCESS

    def disconnect(self):
        self.disconnected += 1
        return mqtt.MQTT_ERR_SUCCESS

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        return mqtt.MQTT_ERR_SUCCESS, 1


class Workers:
    def __init__(self, error=None):
        self.error = error
        self.payloads = []

    def submit(self, payload):
        self.payloads.append(payload)
        future = Future()
        if self.error:
            future.set_exception(self.error)
        else:
            future.set_result("handled")
        return future


class MQTTServiceTests(unittest.TestCase):
    settings = {
        "protocol": "5", "qos": 1, "topic": "webcam/T0",
        "client_id": "test", "host": "broker", "port": 1883, "tls": False,
    }

    def message(self, payload=None, **changes):
        values = dict(retain=False, payload=payload or FIXTURE.read_bytes(),
                      mid=17, qos=1, dup=False)
        values.update(changes)
        return SimpleNamespace(**values)

    def test_success_is_acknowledged_after_handling_and_dup_is_not_filtered(self):
        client, workers = Client(), Workers()
        service = MQTTService(self.settings, workers, client=client)
        service._on_message(client, None, self.message(dup=True))
        self.assertEqual(len(workers.payloads), 1)
        self.assertEqual(client.acks, [(17, 1)])
        self.assertEqual(service.counters["completed"], 1)

    def test_failure_disconnects_without_acknowledging(self):
        client = Client()
        service = MQTTService(self.settings, Workers(RuntimeError("failed")), client=client)
        service._on_message(client, None, self.message())
        self.assertFalse(client.acks)
        self.assertEqual(client.disconnected, 1)
        self.assertEqual(service.counters["failed"], 1)

    def test_invalid_and_retained_messages_are_terminal(self):
        client, workers = Client(), Workers()
        service = MQTTService(self.settings, workers, client=client)
        service._on_message(client, None, self.message(payload=b"not json"))
        service._on_message(client, None, self.message(retain=True, mid=18))
        self.assertFalse(workers.payloads)
        self.assertEqual(client.acks, [(17, 1), (18, 1)])
        self.assertEqual(service.counters["invalid"], 1)
        self.assertEqual(service.counters["retained"], 1)


if __name__ == "__main__":
    unittest.main()
