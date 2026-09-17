"""Runnable MQTT Acquisition service composition."""

import argparse
import json
import logging
import os
from pathlib import Path
import socket
import sys
import threading

import paho.mqtt.client as mqtt

from .archive import s3_client
from .contracts import ContractError, parse_notification
from .handler import NotificationHandler
from .kafka import KafkaPublisher
from .registry import Registry
from .state import InitialStatePublisher
from .workers import AcquisitionWorkers
from ..common.config import database_settings, load_config


LOG = logging.getLogger("weow.acquisition")


class MQTTService:
    """Subscribe and couple MQTT acknowledgement to completed handling."""

    def __init__(self, settings, workers, client=None):
        if str(settings.get("protocol")) != "5" or settings.get("qos") not in (1, 2):
            raise ValueError("operational Acquisition requires MQTT v5 with QoS 1 or 2")
        self.settings = dict(settings)
        self.workers = workers
        self.client = client or mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=settings["client_id"],
            protocol=mqtt.MQTTv5,
            reconnect_on_failure=True,
            manual_ack=True,
        )
        self._fatal = None
        self._lock = threading.Lock()
        self._pending = set()
        self.counters = {"received": 0, "retained": 0, "invalid": 0,
                         "completed": 0, "failed": 0}
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message
        if settings.get("tls"):
            self.client.tls_set()
        username_env = settings.get("username_env")
        password_env = settings.get("password_env")
        if username_env:
            self.client.username_pw_set(
                os.environ[username_env],
                os.environ[password_env] if password_env else None,
            )

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self._fail(ConnectionError("MQTT connection rejected: " + str(reason_code)))
            return
        result, _ = client.subscribe(self.settings["topic"], qos=self.settings["qos"])
        if result != mqtt.MQTT_ERR_SUCCESS:
            self._fail(ConnectionError("MQTT subscription could not be sent"))

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            self._fail(ConnectionError("MQTT subscription rejected"))

    def _ack(self, message):
        if self.client.ack(message.mid, message.qos) != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError("MQTT acknowledgement failed")

    def _on_message(self, client, userdata, message):
        self.counters["received"] += 1
        if message.retain:
            self.counters["retained"] += 1
            self._ack(message)
            return
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            parse_notification(payload)
        except (UnicodeError, json.JSONDecodeError, ContractError, TypeError) as exc:
            self.counters["invalid"] += 1
            LOG.warning("Invalid MQTT notification: %s", type(exc).__name__)
            self._ack(message)
            return
        try:
            future = self.workers.submit(payload)
        except Exception as exc:
            self.counters["failed"] += 1
            self._fail(exc)
            return
        with self._lock:
            self._pending.add(future)

        def completed(done):
            with self._lock:
                self._pending.discard(done)
            try:
                done.result()
                self._ack(message)
            except Exception as exc:
                self.counters["failed"] += 1
                self._fail(exc)
            else:
                self.counters["completed"] += 1

        future.add_done_callback(completed)

    def _fail(self, error):
        with self._lock:
            if self._fatal is None:
                self._fatal = error
        self.client.disconnect()

    def run(self):
        self.client.connect(
            self.settings["host"], self.settings["port"], keepalive=30, clean_start=True
        )
        result = self.client.loop_forever(retry_first_connection=False)
        if self._fatal is not None:
            raise RuntimeError("Acquisition stopped after a handling failure") from self._fatal
        if result != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError("MQTT network loop failed: " + mqtt.error_string(result))

    def close(self):
        self.client.disconnect()


def build_service(settings, secrets_dir):
    acquisition = settings["acquisition"]
    solar = acquisition["solar"]
    spool_settings = settings["spool_s3"]
    archive_settings = settings["archive_s3"]
    spool = s3_client(spool_settings["endpoint_url"],
                      spool_settings["access_key_file"], spool_settings["secret_key_file"],
                      settings.get("s3_ca_bundle"))
    archive = s3_client(archive_settings["endpoint_url"],
                        archive_settings["access_key_file"], archive_settings["secret_key_file"],
                        settings.get("s3_ca_bundle"))
    registry = Registry(
        database_settings(Path(secrets_dir) / "database.json"),
        partitions=settings["kafka"]["partitions"],
        generation=acquisition["generation_version"],
        free_water_tags=acquisition.get("free_water_tags", ()),
        maximum_processing_latitude=acquisition["maximum_processing_latitude"],
    )
    publisher = KafkaPublisher(
        settings["kafka"]["bootstrap_servers"], settings["kafka"]["topic"],
        timeout_seconds=acquisition["operation_timeout_seconds"],
    )
    nfs_root = Path(settings["nfs"]["root"])
    handler = NotificationHandler(
        registry, spool, archive, archive_settings["bucket"], nfs_root,
        InitialStatePublisher(nfs_root), publisher,
        solar["timestamp_field_by_network"], solar["default_timestamp_field"],
        solar["transition_margin_seconds"],
    )
    workers = AcquisitionWorkers(
        handler, acquisition["workers"], acquisition["max_pending_notifications"],
        acquisition["retry_count"], acquisition["retry_admissible_age_seconds"],
        acquisition["retry_delay_seconds"],
    )
    return MQTTService(settings["mqtt"], workers), workers, publisher


def preflight(settings, secrets_dir):
    """Check live dependencies without subscribing or writing application data."""
    service = workers = publisher = None
    try:
        service, workers, publisher = build_service(settings, secrets_dir)
        handler = workers.handler
        handler.spool.head_bucket(Bucket=settings["spool_s3"]["bucket"])
        handler.archive.head_bucket(Bucket=settings["archive_s3"]["bucket"])
        nfs_root = Path(settings["nfs"]["root"])
        if not nfs_root.is_dir() or not os.path.ismount(nfs_root) or not os.access(nfs_root, os.W_OK):
            raise OSError("configured NFS root is not a writable mount")
        metadata = publisher.producer.list_topics(
            topic=settings["kafka"]["topic"],
            timeout=settings["acquisition"]["operation_timeout_seconds"],
        )
        topic = metadata.topics.get(settings["kafka"]["topic"])
        if topic is None or topic.error is not None:
            raise ConnectionError("operational Kafka topic is unavailable")
        with socket.create_connection(
            (settings["mqtt"]["host"], settings["mqtt"]["port"]),
            timeout=settings["acquisition"]["operation_timeout_seconds"],
        ):
            pass
        return {
            "postgres": "ready",
            "spool_s3": "ready",
            "archive_s3": "ready",
            "nfs": "ready",
            "kafka": "ready",
            "mqtt_tcp": "ready",
        }
    finally:
        if service is not None:
            service.close()
        if workers is not None:
            workers.close(wait=True)
        if publisher is not None:
            publisher.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/acquisition.example.json"))
    parser.add_argument("--secrets-dir", type=Path, default=Path(".secrets"))
    parser.add_argument("--check", action="store_true",
                        help="check dependencies without subscribing to MQTT")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service = workers = publisher = None
    try:
        settings = load_config(args.config, args.secrets_dir)
        if args.check:
            print(json.dumps(preflight(settings, args.secrets_dir), sort_keys=True))
            return 0
        service, workers, publisher = build_service(settings, args.secrets_dir)
        service.run()
    except KeyboardInterrupt:
        LOG.info("Acquisition interrupted")
    except Exception as exc:
        LOG.error("Acquisition failed: %s", type(exc).__name__)
        return 2
    finally:
        if service is not None:
            service.close()
        if workers is not None:
            workers.close(wait=True)
        if publisher is not None:
            publisher.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
