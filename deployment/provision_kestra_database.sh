#!/usr/bin/env bash
# Create Kestra's isolated role and database in the pilot PostgreSQL instance.
set -euo pipefail

config_root="$HOME/.config/weow-db"
password_file="$config_root/kestra_password"

mkdir -p "$config_root"
chmod 700 "$config_root"
if [ ! -f "$password_file" ]; then
  umask 077
  openssl rand -base64 36 > "$password_file"
fi
password=$(cat "$password_file")

podman exec -i weow-postgres psql \
  --username postgres --dbname postgres --set ON_ERROR_STOP=1 \
  --set kestra_password="$password" <<'SQL'
SELECT format('CREATE ROLE kestra LOGIN PASSWORD %L', :'kestra_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'kestra') \gexec
ALTER ROLE kestra PASSWORD :'kestra_password';
SELECT 'CREATE DATABASE kestra OWNER kestra'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kestra') \gexec
SQL

podman exec weow-postgres pg_isready --username kestra --dbname kestra
