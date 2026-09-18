#!/usr/bin/env bash
# Start and visibly follow the 48-hour live benchmark from a Kestra SSH task.
set -euo pipefail

repo_root="$HOME/src/webcam-iot-ml-services"
service="weow-acquisition-benchmark.service"
config="$repo_root/.secrets/acquisition.benchmark.json"
metrics_url="http://192.168.1.135:9102/metrics"
duration_seconds=172800
poll_seconds=300
minimum_free_bytes=21474836480

export XDG_RUNTIME_DIR="/run/user/$(id -u)"
export DBUS_SESSION_BUS_ADDRESS="unix:path=$XDG_RUNTIME_DIR/bus"

cd "$repo_root"
.venv/bin/python - "$config" <<'PY'
import json
from pathlib import Path
import sys

settings = json.loads(Path(sys.argv[1]).read_text())
benchmark = settings.get("benchmark", {})
acquisition = settings.get("acquisition", {})
run_id = benchmark.get("run_id", "")
expected_prefix = f"benchmarks/wp1_5/{run_id}"
checks = {
    "benchmark enabled": benchmark.get("enabled") is True,
    "live input": benchmark.get("input_mode") == "live",
    "48-hour duration": benchmark.get("duration_seconds") == 172800,
    "fresh endurance identity": run_id.startswith("wp15_live_48h_"),
    "24 workers": acquisition.get("workers") == 24,
    "isolated output prefix": acquisition.get("output_prefix") == expected_prefix,
    "20 GiB NFS reserve": acquisition.get("minimum_nfs_free_bytes") == 21474836480,
    "real MQTT topic": settings.get("mqtt", {}).get("topic") == "webcam/T0",
    "real ingestion bucket": settings.get("spool_s3", {}).get("bucket")
        == "webcam-ingestion-buffer",
}
failed = [name for name, accepted in checks.items() if not accepted]
if failed:
    raise SystemExit("invalid 48-hour benchmark configuration: " + ", ".join(failed))
print(f"validated run_id={run_id} duration_seconds=172800 workers=24")
PY

if [[ "${1:-}" == "--validate-only" ]]; then
  exit 0
fi

.venv/bin/python -m weow_ml.acquisition.service \
  --config "$config" --secrets-dir "$repo_root/.secrets" --check

systemctl --user reset-failed "$service" 2>/dev/null || true
systemctl --user start "$service"
started_epoch=$(date +%s)
echo "benchmark_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

stop_if_running() {
  if systemctl --user is-active --quiet "$service"; then
    systemctl --user stop "$service"
  fi
}
trap stop_if_running EXIT INT TERM

while systemctl --user is-active --quiet "$service"; do
  now_epoch=$(date +%s)
  elapsed=$((now_epoch - started_epoch))
  free_bytes=$(df --output=avail -B1 /srv/weow-nfs | tail -n 1 | tr -d ' ')
  metrics=$(curl --fail --silent --max-time 10 "$metrics_url")
  received=$(awk '$1 ~ /^weow_acquisition_notifications_total/ && $1 ~ /outcome="received"/ {print $2}' <<<"$metrics")
  completed=$(awk '$1 ~ /^weow_acquisition_notifications_total/ && $1 ~ /outcome="completed"/ {print $2}' <<<"$metrics")
  queue=$(awk '$1 == "weow_acquisition_worker_queue" {print $2}' <<<"$metrics")
  active=$(awk '$1 == "weow_acquisition_active_workers" {print $2}' <<<"$metrics")
  failures=$(awk '$1 ~ /^weow_acquisition_stage_failures_total/ {sum += $2} END {print sum+0}' <<<"$metrics")
  resources=$(podman stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' \
    weow-acquisition-benchmark 2>/dev/null || echo unavailable)
  printf 'utc=%s elapsed_seconds=%s received=%s completed=%s queue=%s active=%s failures=%s nfs_free_bytes=%s resources="%s"\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$elapsed" "${received:-0}" \
    "${completed:-0}" "${queue:-0}" "${active:-0}" "$failures" \
    "$free_bytes" "$resources"

  if (( free_bytes < minimum_free_bytes )); then
    echo "NFS free space crossed the 20 GiB hard stop" >&2
    exit 20
  fi
  if (( elapsed > duration_seconds + 900 )); then
    echo "benchmark service exceeded its duration and drain allowance" >&2
    exit 21
  fi
  sleep "$poll_seconds"
done

exit_status=$(systemctl --user show "$service" --property=ExecMainStatus --value)
result=$(systemctl --user show "$service" --property=Result --value)
echo "benchmark_finished=$(date -u +%Y-%m-%dT%H:%M:%SZ) result=$result exit_status=$exit_status"
if [[ "$result" != "success" || "$exit_status" != "0" ]]; then
  exit 22
fi
trap - EXIT INT TERM
