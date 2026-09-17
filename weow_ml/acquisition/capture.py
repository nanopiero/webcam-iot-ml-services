"""Bounded, independent MQTT v5 subscriber for validating the upstream contract."""

import argparse
import json
import os
from pathlib import Path
import sys
import time
import uuid

import paho.mqtt.client as mqtt

from .contracts import ContractError, parse_notification


def capture(settings, output: Path, count: int, timeout: float) -> dict:
    if count <= 0 or timeout <= 0:
        raise ValueError("count and timeout must be positive")
    if str(settings.get("protocol")) != "5":
        raise ValueError("capture requires MQTT v5")
    if settings.get("qos") not in (0, 1, 2):
        raise ValueError("invalid MQTT QoS")
    output.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    result = {"received": 0, "valid": 0, "invalid": 0, "retained_ignored": 0}
    errors = []
    # Never displace a running acquisition client or retain a diagnostic session.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id="weow-contract-" + run_id, protocol=mqtt.MQTTv5)
    client.connect_timeout = min(timeout, 10)
    if settings.get("tls"):
        client.tls_set()
    username_env = settings.get("username_env")
    password_env = settings.get("password_env")
    if username_env:
        client.username_pw_set(os.environ[username_env],
                               os.environ[password_env] if password_env else None)

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            errors.append("MQTT connection rejected: " + str(reason_code))
            return
        status, _ = client.subscribe(settings["topic"], qos=settings["qos"])
        if status != mqtt.MQTT_ERR_SUCCESS:
            errors.append("MQTT subscription could not be sent")

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        if any(code.is_failure for code in reason_codes):
            errors.append("MQTT subscription rejected")

    def on_message(client, userdata, message):
        if message.retain:
            result["retained_ignored"] += 1
            return
        if result["received"] >= count:
            return
        result["received"] += 1
        path = output / f"{run_id}-{result['received']:04d}.json"
        # Keep original bytes, including invalid samples, outside version control.
        with path.open("xb") as file:
            os.chmod(path, 0o600)
            file.write(message.payload)
        try:
            parse_notification(json.loads(message.payload.decode("utf-8")))
        except (ValueError, UnicodeError, ContractError):
            result["invalid"] += 1
        else:
            result["valid"] += 1

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    deadline = time.monotonic() + timeout
    try:
        client.connect(settings["host"], settings["port"], keepalive=30, clean_start=True)
        while result["received"] < count and not errors:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Capture timed out: {result}")
            status = client.loop(timeout=min(1.0, remaining))
            if status != mqtt.MQTT_ERR_SUCCESS:
                raise ConnectionError("MQTT network loop failed: " + mqtt.error_string(status))
        if errors:
            raise ConnectionError(errors[0])
    finally:
        client.disconnect()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/acquisition.example.json"))
    parser.add_argument("--output", type=Path, default=Path("local-captures"))
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    try:
        settings = json.loads(args.config.read_text())["mqtt"]
        result = capture(settings, args.output, args.count, args.timeout)
    except (OSError, ValueError, KeyError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result))
    return 0 if result["invalid"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
