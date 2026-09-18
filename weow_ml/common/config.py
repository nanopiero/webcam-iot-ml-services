"""Load shared public configuration and credential references without logging secrets."""

import copy
import json
import os
from pathlib import Path


def load_config(path="config/acquisition.example.json", secrets_dir=".secrets"):
    settings = json.loads(Path(path).read_text())
    directory = Path(secrets_dir)
    endpoint_file = directory / "s3_endpoints.json"
    if endpoint_file.exists():
        storage = json.loads(endpoint_file.read_text())
        for name in ("spool_s3", "archive_s3"):
            for key, value in storage[name].items():
                if settings[name].get(key) in (None, "REQUIRED"):
                    settings[name][key] = value
        settings["s3_ca_bundle"] = storage.get("ca_bundle")
    for section, default_prefix in (("spool_s3", "ingestion"),
                                    ("archive_s3", "weows")):
        prefix = settings[section].get("credentials_prefix", default_prefix)
        settings[section]["access_key_file"] = str(directory / f"{prefix}_s3_access_key")
        settings[section]["secret_key_file"] = str(directory / f"{prefix}_s3_secret_key")
    return settings


def database_settings(path=".secrets/database.json", test=False):
    """Return connection keyword arguments; callers must not log this dictionary."""
    settings = copy.copy(json.loads(Path(path).read_text()))
    password_file = Path(settings.pop("password_file"))
    settings["password"] = password_file.read_text().strip()
    if test:
        settings["dbname"] = "weow_ml_test"
    return settings
