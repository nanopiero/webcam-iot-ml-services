#!/usr/bin/env bash
# Install Prometheus and Grafana as persistent rootless services on weow-o.
set -euo pipefail

source_root=deployment/observability
config_root="$HOME/.config/weow-observability"
service_root="$HOME/.config/systemd/user"

if ! command -v podman >/dev/null; then
  echo "podman is required; install it with: sudo dnf install -y podman" >&2
  exit 2
fi
if [ ! -f "$source_root/prometheus/prometheus.yml" ]; then
  echo "Run this script from the repository root" >&2
  exit 2
fi

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"

mkdir -p "$config_root/grafana" "$service_root"
cp "$source_root/prometheus/prometheus.yml" "$config_root/prometheus.yml"
cp "$source_root/prometheus/alerts.yml" "$config_root/alerts.yml"
cp -R "$source_root/grafana/provisioning" "$config_root/grafana/"
cp -R "$source_root/grafana/dashboards" "$config_root/grafana/"

if [ ! -f "$config_root/grafana.env" ]; then
  umask 077
  {
    echo 'GF_SECURITY_ADMIN_USER=admin'
    printf 'GF_SECURITY_ADMIN_PASSWORD=%s\n' "$(openssl rand -hex 24)"
    echo 'GF_USERS_ALLOW_SIGN_UP=false'
  } > "$config_root/grafana.env"
fi

podman pull docker.io/prom/prometheus:v3.12.0
podman pull docker.io/grafana/grafana:13.2.0
podman network exists weow-observability || podman network create weow-observability
install -m 600 deployment/weow-prometheus.service "$service_root/weow-prometheus.service"
install -m 600 deployment/weow-grafana.service "$service_root/weow-grafana.service"
systemctl --user daemon-reload
systemctl --user enable --now weow-prometheus.service weow-grafana.service

echo "Grafana credentials are in $config_root/grafana.env"
