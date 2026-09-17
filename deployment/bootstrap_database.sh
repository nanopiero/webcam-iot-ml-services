#!/usr/bin/env bash
# Run as codexuser on weow-db. Existing volumes and credentials are preserved.
set -euo pipefail
umask 077
mkdir -p "$HOME/.config/weow-db" "$HOME/.config/systemd/user"
if [ ! -f "$HOME/.config/weow-db/postgres_password" ]; then
  python3 -c 'import secrets; print(secrets.token_urlsafe(48))' > "$HOME/.config/weow-db/postgres_password"
fi
podman volume exists weow-postgres-data || podman volume create weow-postgres-data
if ! podman secret inspect weow-postgres-password >/dev/null 2>&1; then
  podman secret create weow-postgres-password "$HOME/.config/weow-db/postgres_password"
fi
cat > "$HOME/.config/systemd/user/weow-postgres.service" <<'EOF'
[Unit]
Description=WEOW PostgreSQL acquisition database
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/podman run --name weow-postgres --replace --rm -v weow-postgres-data:/var/lib/postgresql/data --publish 192.168.1.145:5432:5432 --env POSTGRES_DB=weow_ml --env POSTGRES_PASSWORD_FILE=/run/secrets/weow-postgres-password --secret weow-postgres-password docker.io/library/postgres:16-bookworm
ExecStop=/usr/bin/podman stop --time 30 weow-postgres
ExecStopPost=-/usr/bin/podman rm --force weow-postgres
Restart=on-failure
RestartSec=5
TimeoutStartSec=180
EOF
if [ -f "$HOME/.config/containers/systemd/weow-postgres.container" ]; then
  mv "$HOME/.config/containers/systemd/weow-postgres.container" \
     "$HOME/.config/containers/systemd/weow-postgres.container.disabled"
fi
systemctl --user daemon-reload
systemctl --user enable --now weow-postgres.service
for attempt in {1..30}; do
  if podman exec weow-postgres pg_isready -U postgres -d weow_ml; then
    exit 0
  fi
  sleep 1
done
exit 1
