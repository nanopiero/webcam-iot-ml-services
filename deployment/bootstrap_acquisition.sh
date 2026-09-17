#!/usr/bin/env bash
# Build and install Acquisition without starting the live MQTT subscription.
set -euo pipefail

service_source=deployment/weow-acquisition.service
service_target="$HOME/.config/systemd/user/weow-acquisition.service"

if [ ! -f "$service_source" ] || [ ! -f Containerfile.acquisition ]; then
  echo "Run this script from the repository root" >&2
  exit 2
fi
if ! mountpoint -q /srv/weow-nfs || [ ! -w /srv/weow-nfs ]; then
  echo "/srv/weow-nfs must be a writable mount" >&2
  exit 2
fi
for file in .secrets/database.json .secrets/postgres_acquisition_password \
            .secrets/ingestion_s3_access_key .secrets/ingestion_s3_secret_key \
            .secrets/weows_s3_access_key .secrets/weows_s3_secret_key \
            .secrets/s3_endpoints.json; do
  if [ ! -r "$file" ]; then
    echo "Missing required secret file: $file" >&2
    exit 2
  fi
done

podman build --tag localhost/weow-acquisition:dev --file Containerfile.acquisition .
mkdir -p "$HOME/.config/systemd/user"
install -m 600 "$service_source" "$service_target"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"
systemctl --user daemon-reload
systemctl --user disable weow-acquisition.service >/dev/null 2>&1 || true
echo "Acquisition image built and service installed but not started"
