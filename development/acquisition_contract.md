# Acquisition foundation

This first implementation increment runs offline on Python 3.9 or later:

```sh
python3 -m unittest discover -s tests -v
python3 -m weow_ml tests/fixtures/notification_n0v0.json
```

The fixture is the illustrative N0V0 notification from the upstream architecture,
with its `provider_metadata` placeholders replaced by empty objects. It is not
a capture from the running ingestion system. The first live contract check is
recorded below; other networks still need sampling.

The parser preserves the full notification for the archived sidecar. It
validates source dimensions and colour representation, image identity, timestamps,
S3 references, derived dimensions, and the scalar freshness signature. It accepts
unknown fields and never filters on the MQTT DUP flag. Archive names follow the
ingestion filename and resolved derived-stream identifier.

The example includes derived colour depth but omits source colour depth. Missing
source depth remains unknown; derived depth is not substituted. A later explicit
depth changes the profile fingerprint. Confirm this behavior against live data.

Partition assignment uses SHA-256 of the UTF-8 processing-stream identifier,
interpreted as an unsigned big-endian integer modulo the configured partition
count. Persist this assignment at stream creation. Existing assignments and the
architecture's profile-inheritance exceptions take precedence over recalculation.

`sql/migrations/001_acquisition.sql` is the initial PostgreSQL schema migration.
It has been applied to the development databases and normalizes processing
profiles into a small additional
table to enforce profile reuse across slices. Stream metadata is initially kept
in JSONB; typed fields and indexes can be added as query needs become concrete.
The future registry writer must lock the parent stream while allocating profile
numbers, commit database changes before promoting cache entries, and reload the
cache on startup. That writer is not implemented in this increment.

`config/acquisition.example.json` describes the upcoming service configuration;
the offline inspector does not load it. The capture command loads its MQTT
section, now populated from the upstream connection sheet. REQUIRED values await
S3 and local Kafka endpoints. Credentials belong in environment variables
or external credential profiles, never in this tracked template. Worker counts
and queue limits are initial benchmark candidates, not measured sizing results.

## Infrastructure observed on 2026-09-17

- Acquisition host: Python 3.9, Podman available.
- NFS backing volume: `/srv/weow-nfs`, approximately 200 GB provisioned.
- `weow-p0`: NFSv4.2 mounted at `/mnt/weow-nfs` from the acquisition host.
- `weow-db` and `weow-o`: SSH access confirmed.

The PostgreSQL container is now running on `weow-db` as a rootless Podman
systemd user service. Its persistent volume is `weow-postgres-data`; the
database service is PostgreSQL 16.15, with `weow_ml` and `weow_ml_test`
databases, schema migration 1, and the restricted `weow_acquisition` role.
The container publishes `192.168.1.145:5432` on the private interface only.
Private-network TCP/5432 is allowed, and an authenticated connection from
`weow-a` using the restricted `weow_acquisition` role has been verified for
both `weow_ml` and `weow_ml_test`. The local ignored database configuration
uses the private endpoint directly; the earlier SSH tunnel is no longer used.

The revised architecture takes precedence over earlier correction proposals:
use static consumer membership and publish complete NFS state through a temporary
file and atomic rename. Occasional stale complete state after reassignment is an
accepted pilot risk. Strict ownership fencing is not a development prerequisite.

The persistent registry has been tested against `weow_ml_test` for concurrent
discovery by independent registry instances, profile recurrence, slice-layout
partition inheritance, metadata ordering, operational-status changes,
administrative overrides, and cache reconstruction. Tests use unique stream
identities and remove only their own rows.

Next: proceed with WP1.2 foundations and then wire the WP1.3 live service across
MQTT, archive storage, NFS preparation, and Kafka publication. Benchmark these
stages under WP1.5 before launching sustained acquisition.

## WP1.3 implementation status

The operational single-notification handler now applies registry status and
solar eligibility, downloads an eligible image once, archives the image and
resolved sidecar, publishes all processing images to NFS, requests idempotent
initial-state creation, and only then publishes acknowledged Kafka jobs. A
bounded worker pool applies the configured one short retry while preserving the
original Acquisition availability timestamp.

The pilot automatically blacklists streams above 68.56 degrees north. This
check occurs in registry resolution before solar calculation, so such streams
retain metadata changes without downloading or processing images for dates on
which Astral cannot supply same-day sunrise and sunset values.

The concrete initial-state NPZ writer remains separate from the handler. Schema
`S0V0` uses the 2048-element ReID latent dimension from the existing WEOW model,
an empty float32 latent bank, empty cluster counts, and zero-length arrays for
the null freshness signature and last ingestion timestamp. Each processing
stream has one stable immutable state path under its derived-stream hierarchy.
The handler enforces that state publication completes before the corresponding
Kafka job is emitted.

The runnable entry point is `python -m weow_ml.acquisition.service`. Its
`--check` mode validates PostgreSQL, spool and archive S3 access, the writable
NFS mount, operational Kafka topic metadata, and MQTT TCP reachability without
subscribing or writing application data. `Containerfile.acquisition` packages
the entry point on Python 3.12. `deployment/bootstrap_acquisition.sh` builds the
image and installs a stopped user service; starting the live subscription is a
separate controlled-launch action.

## Live MQTT validation, 2026-09-17

The local reference sheet is
`~/doc/ML_services_architecture/upstream_mqtt_connection_sheet`.
It specifies MQTT v5 at `192.168.1.97:1883`, topic `webcam/T0`, QoS 1,
anonymous access, no TLS, and non-retained UTF-8 N0V0 notifications.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-mqtt.txt
.venv/bin/python -m weow_ml.capture --count 10 --timeout 120
.venv/bin/python -m unittest discover -s tests -v
```

The capture command uses a unique diagnostic client ID and a clean session.
It does not publish messages or use the operational acquisition identity.
Original payloads are saved with owner-only permissions under the ignored
`local-captures/` directory. It rejects failed connections/subscriptions, reports
timeouts, and counts schema failures without printing payload contents.
Paho MQTT v5 client behavior follows the
[official client documentation](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html).

The initial result was 10 notifications received, 10 valid, 0 invalid, and 0
retained. A later bounded sample received 200 notifications, all valid and
non-retained. All live samples were from Windy; the broker did not emit Skaping
or Fintraffic notifications during these sampling windows. The checked-in
fixture remains the canonical illustrative Fintraffic N0V0 payload from the
upstream architecture. Live Skaping and Fintraffic sampling remains an external
coverage item once those workers emit notifications; it does not change the
network-generic N0V0 parser. Source colour depth was absent in every live sample.
Additional fields included site corrected coordinates, source-image provider
metadata, and provider_url_timestamp; these are preserved in the payload.

The sheet does not supply the spool S3 endpoint or credential profile. Bucket
and object keys arrive in notifications, but downloading images remains untested
until S3 connection settings are available. The ingestion .env was not accessed.

## Bounded S3 archival check

Install `requirements-s3.txt`, then run with confirmed endpoint URLs and bucket:

```sh
.venv/bin/python -m pip install -r requirements-s3.txt
.venv/bin/python -m weow_ml.archive \
  --spool-endpoint https://SPOOL-ENDPOINT \
  --archive-endpoint https://ARCHIVE-ENDPOINT \
  --archive-bucket ARCHIVE-BUCKET \
  local-captures/SELECTED-NOTIFICATION.json
```

The command reads the four ingestion/weows access/secret key files directly
from `.secrets/`, without logging their contents. It copies complete unsliced
images into the architecture's `images/{derived_stream_id}/yyyy/mm/dd/hh/`
hierarchy and embeds compressed JSON sidecars in JPEG APP15 segments. Conditional creation prevents replacement of
existing objects; repeated transfers accept matching content. Both uploaded
objects are read back and checked. This uses the S3
[conditional PUT API](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html);
compatibility with the archive endpoint was confirmed by the live test below.

This is an archival integration check, not operational Acquisition. Sidecars
explicitly mark processing context as unresolved and do not invent stream IDs
or the original MQTT receipt timestamp. No Kafka jobs are created. JPEG marker
checks reject obvious invalid payloads but do not replace full image decoding.
Transfers are bounded to 10 MiB per source object. Each archive image and its
sidecar use one conditional PUT and remain recoverable from one valid JPEG.

Local tests cover transfer, repeat transfer, conflicting existing content,
missing source objects, and indoor exclusion.

Live validation on 2026-09-17 succeeded: one 3,246-byte image and its JSON
sidecar were uploaded to `iot-pilot-weow-storage` and both read back with exact
byte equality. The object stem is
`images/win1793913147T0/2026/09/17/12/20260917T122641Z_win1793913147T0`
with `.jpg` and `.json` extensions. No inference job was published.
Both stores use `https://object-store.os-api.cci2.ecmwf.int`; the source bucket
is `webcam-ingestion-buffer`. Validated endpoint settings are saved locally in
the ignored, mode-600 `.secrets/s3_endpoints.json` file.
On this VM set `AWS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt`: verification
with the SDK's bundled trust store failed, while the system CA bundle worked.
TLS verification remains enabled.
