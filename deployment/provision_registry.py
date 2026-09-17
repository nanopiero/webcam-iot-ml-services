"""Provision acquisition databases through SSH; never print credentials.

Run from the repository root. Uses the existing weow-postgres container.
"""

import json
import os
from pathlib import Path
import secrets
import subprocess


def psql(database, sql):
    return subprocess.run(
        ["ssh", "weow-db", "podman", "exec", "-i", "weow-postgres", "psql",
         "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database, "-Atq"],
        input=sql, text=True, capture_output=True, check=True,
    ).stdout.strip()


def main():
    secret_dir = Path(".secrets")
    secret_dir.mkdir(mode=0o700, exist_ok=True)
    password_file = secret_dir / "postgres_acquisition_password"
    if not password_file.exists():
        with os.fdopen(os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
            file.write(secrets.token_hex(32) + "\n")
    password = password_file.read_text().strip()
    if len(password) != 64 or any(c not in "0123456789abcdef" for c in password):
        raise ValueError("Unexpected generated credential format")
    if psql("postgres", "SELECT 1 FROM pg_roles WHERE rolname='weow_acquisition';") != "1":
        psql("postgres", f"CREATE ROLE weow_acquisition LOGIN PASSWORD '{password}';")
    for database in ("weow_ml", "weow_ml_test"):
        if psql("postgres", f"SELECT 1 FROM pg_database WHERE datname='{database}';") != "1":
            psql("postgres", f"CREATE DATABASE {database};")
        if psql(database, "SELECT to_regclass('public.schema_migration') IS NOT NULL;") == "f":
            psql(database, Path("sql/migrations/001_acquisition.sql").read_text())
        elif psql(database, "SELECT max(version) FROM schema_migration;") != "1":
            raise ValueError("Unexpected database schema version")
        psql(database, f"""
            REVOKE CREATE ON SCHEMA public FROM PUBLIC;
            GRANT CONNECT ON DATABASE {database} TO weow_acquisition;
            GRANT USAGE ON SCHEMA public TO weow_acquisition;
            GRANT SELECT ON schema_migration, registry_settings TO weow_acquisition;
            GRANT SELECT, INSERT, UPDATE, DELETE ON derived_stream, processing_profile,
                processing_stream TO weow_acquisition;
        """)
        print(database + ": schema version 1 and acquisition grants ready")
    settings = {"host": "192.168.1.145", "port": 5432, "dbname": "weow_ml",
                "user": "weow_acquisition", "password_file": str(password_file),
                "connect_timeout": 10}
    config_path = secret_dir / "database.json"
    if not config_path.exists():
        with os.fdopen(os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as file:
            json.dump(settings, file, indent=2)
            file.write("\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError:
        # Failed CREATE ROLE input may contain a password: never echo it.
        raise SystemExit("Database provisioning command failed; credential-bearing SQL suppressed")
