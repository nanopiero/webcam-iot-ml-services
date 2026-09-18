#!/usr/bin/env bash
# Install the development Kafka broker as a persistent rootless user service.
set -euo pipefail

image=docker.io/apache/kafka:4.1.2
service_source=deployment/weow-kafka.service
service_target="$HOME/.config/systemd/user/weow-kafka.service"
cluster_directory="$HOME/.config/weow-kafka"
cluster_environment="$cluster_directory/cluster.env"

# Non-interactive sessions may not inherit the persistent user manager's bus.
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"

if [ ! -f "$service_source" ]; then
  echo "Run this script from the repository root" >&2
  exit 2
fi

mkdir -p "$HOME/.config/systemd/user" "$cluster_directory"
chmod 700 "$cluster_directory"

if ! mountpoint -q /srv/weow-kafka || [ ! -w /srv/weow-kafka ]; then
  echo "/srv/weow-kafka must be a writable dedicated mount" >&2
  exit 2
fi

podman pull "$image"
mkdir -p /srv/weow-kafka/data

if [ ! -f "$cluster_environment" ]; then
  cluster_id=$(podman run --rm "$image" /opt/kafka/bin/kafka-storage.sh random-uuid)
  printf 'CLUSTER_ID=%s\n' "$cluster_id" > "$cluster_environment"
  chmod 600 "$cluster_environment"
fi

install -m 600 "$service_source" "$service_target"
systemctl --user daemon-reload
systemctl --user enable --now weow-kafka.service

for attempt in $(seq 1 60); do
  if podman exec weow-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Kafka did not become ready" >&2
    exit 1
  fi
  sleep 1
done

for topic in inference.jobs.live inference.jobs.benchmark; do
  if ! podman exec weow-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server 127.0.0.1:9092 --list | grep -Fxq "$topic"; then
    podman exec weow-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server 127.0.0.1:9092 \
      --create --topic "$topic" --partitions 50 --replication-factor 1 \
      --config cleanup.policy=delete --config retention.ms=31536000000 \
      --config segment.bytes=268435456
  fi
  podman exec weow-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server 127.0.0.1:9092 --describe --topic "$topic"
done
