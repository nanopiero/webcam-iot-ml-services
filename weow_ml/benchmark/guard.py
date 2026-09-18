"""Fail-closed isolation checks for a live WP1.5 Acquisition benchmark."""

from dataclasses import dataclass

from ..acquisition.contracts import ContractError, identifier
from .workload import BENCHMARK_KAFKA_TOPIC, BENCHMARK_MQTT_TOPIC


BENCHMARK_DATABASE = "weow_ml_benchmark"
LIVE_MQTT_TOPIC = "webcam/T0"


@dataclass(frozen=True)
class BenchmarkGuard:
    run_id: str
    spool_bucket: str
    input_mode: str = "synthetic"

    @property
    def output_prefix(self):
        return f"benchmarks/wp1_5/{self.run_id}"

    @property
    def spool_prefix(self):
        return self.output_prefix + "/spool/"

    def __call__(self, notification):
        if notification.bucket != self.spool_bucket:
            raise ContractError("benchmark notification uses another spool bucket")
        if self.input_mode == "live":
            return
        payload = notification.payload
        benchmark = payload.get("benchmark")
        if notification.network_id != "benchmark":
            raise ContractError("benchmark notification must use the benchmark network")
        if not isinstance(benchmark, dict) or benchmark.get("run_id") != self.run_id:
            raise ContractError("benchmark notification run_id differs from the service")
        if not notification.object_key.startswith(self.spool_prefix):
            raise ContractError("benchmark notification is outside its spool prefix")


def validate_benchmark_settings(settings, connection_settings):
    """Return the notification guard only when every writable target is isolated."""
    benchmark = settings.get("benchmark")
    if not isinstance(benchmark, dict) or benchmark.get("enabled") is not True:
        return None
    run_id = identifier(benchmark.get("run_id"), "benchmark.run_id")
    input_mode = benchmark.get("input_mode", "synthetic")
    if input_mode not in ("synthetic", "live"):
        raise ValueError("benchmark input_mode must be 'synthetic' or 'live'")
    spool_bucket = settings["spool_s3"].get("bucket")
    if not isinstance(spool_bucket, str) or not spool_bucket:
        raise ValueError("benchmark spool bucket is required")
    guard = BenchmarkGuard(run_id, spool_bucket, input_mode)
    required_mqtt_topic = LIVE_MQTT_TOPIC if input_mode == "live" else BENCHMARK_MQTT_TOPIC
    expected = (
        (settings["mqtt"].get("topic"), required_mqtt_topic, "benchmark MQTT topic"),
        (settings["kafka"].get("topic"), BENCHMARK_KAFKA_TOPIC, "benchmark Kafka topic"),
        (settings["acquisition"].get("output_prefix"), guard.output_prefix,
         "benchmark output prefix"),
        (connection_settings.get("dbname"), BENCHMARK_DATABASE, "benchmark database"),
    )
    for actual, required, name in expected:
        if actual != required:
            raise ValueError(f"{name} must be {required!r}")
    client_id = settings["mqtt"].get("client_id", "")
    if not client_id.startswith("weow-benchmark-"):
        raise ValueError("benchmark MQTT client_id must start with 'weow-benchmark-'")
    return guard
