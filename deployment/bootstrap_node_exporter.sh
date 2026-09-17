#!/usr/bin/env bash
# Install a rootless node exporter bound to this VM's private address.
set -euo pipefail

service_root="$HOME/.config/systemd/user"
environment_file="$HOME/.config/weow-node-exporter.env"
private_ip=$(ip -4 -o address show scope global | awk '$4 ~ /^192\.168\./ {sub(/\/.*/, "", $4); print $4; exit}')

if ! command -v podman >/dev/null; then
  echo "podman is required; install it with: sudo dnf install -y podman" >&2
  exit 2
fi
if [ -z "$private_ip" ]; then
  echo "No 192.168.x.x private address found" >&2
  exit 2
fi

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"
mkdir -p "$service_root"
printf 'WEOW_PRIVATE_IP=%s\n' "$private_ip" > "$environment_file"
chmod 600 "$environment_file"
podman pull docker.io/prom/node-exporter:v1.9.1
install -m 600 deployment/weow-node-exporter.service "$service_root/weow-node-exporter.service"
systemctl --user daemon-reload
systemctl --user enable --now weow-node-exporter.service
