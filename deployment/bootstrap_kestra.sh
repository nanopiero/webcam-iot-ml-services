#!/usr/bin/env bash
# Install Kestra on weow-o using the pilot PostgreSQL instance.
set -euo pipefail

config_root="$HOME/.config/weow-kestra"
service_root="$HOME/.config/systemd/user"
database_password_file="$config_root/database_password"
admin_password_file="$config_root/admin_password"
encryption_key_file="$config_root/encryption_key"
ssh_key_file="$config_root/weow-a-ssh-key"

if ! command -v podman >/dev/null; then
  echo "podman is required" >&2
  exit 2
fi
if [ ! -s "$database_password_file" ]; then
  echo "$database_password_file must contain the Kestra database password" >&2
  exit 2
fi
if [ ! -f deployment/weow-kestra.service ]; then
  echo "Run this script from the repository root" >&2
  exit 2
fi

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=$XDG_RUNTIME_DIR/bus}"
mkdir -p "$config_root" "$service_root"
chmod 700 "$config_root"
umask 077

[ -s "$admin_password_file" ] || openssl rand -base64 36 > "$admin_password_file"
[ -s "$encryption_key_file" ] || openssl rand -base64 32 > "$encryption_key_file"
if [ ! -s "$ssh_key_file" ]; then
  ssh-keygen -q -t ed25519 -N '' -C weow-kestra -f "$ssh_key_file"
fi

python3 - "$config_root" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
database_password = (root / "database_password").read_text().strip()
admin_password = (root / "admin_password").read_text().strip()
encryption_key = (root / "encryption_key").read_text().strip()
configuration = f"""datasources:
  postgres:
    url: jdbc:postgresql://192.168.1.145:5432/kestra
    driver-class-name: org.postgresql.Driver
    username: kestra
    password: {json.dumps(database_password)}
kestra:
  tutorialFlows:
    enabled: false
  server:
    basic-auth:
      username: admin@weow.local
      password: {json.dumps(admin_password)}
  encryption:
    secret-key: {json.dumps(encryption_key)}
  repository:
    type: postgres
  queue:
    type: postgres
  storage:
    type: local
    local:
      base-path: /app/storage
  tasks:
    tmp-dir:
      path: /tmp/kestra-wd/tmp
  url: http://192.168.1.6:8080/
"""
(root / "application.yml").write_text(configuration)
PY

printf 'SECRET_WEOW_A_SSH_PRIVATE_KEY=%s\n' \
  "$(base64 -w0 < "$ssh_key_file")" > "$config_root/secrets.env"
chmod 600 "$config_root"/*

podman pull docker.io/kestra/kestra:v2.0.2
install -m 600 deployment/weow-kestra.service "$service_root/weow-kestra.service"
systemctl --user daemon-reload
systemctl --user enable weow-kestra.service
systemctl --user restart weow-kestra.service

echo "Kestra admin credentials are in $admin_password_file"
echo "Authorize this public key for codexuser on weow-a:"
cat "$ssh_key_file.pub"
