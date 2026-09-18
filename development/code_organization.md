# Code organization

The repository is organized by runtime service. Shared code is kept small and
contains only concepts that are genuinely used by more than one service. This
keeps the Acquisition implementation independent from the later processing,
publication, calibration, and maintenance services without introducing a
framework before those services exist.

## Package layout

```text
weow_ml/
  common/
    config.py             public configuration and local secret references
  acquisition/
    contracts.py          N0V0 validation and notification value objects
    streams.py            pure profile, slicing, fingerprint and partition rules
    registry.py           PostgreSQL-backed registry and startup cache
    archive.py            bounded spool download and S3 archival operations
    handler.py            ordered single-notification workflow
    workers.py            bounded concurrency and short retry policy
    service.py            runnable MQTT composition and acknowledgements
    capture.py            independent MQTT contract diagnostic
    cli.py                offline notification-inspection command

sql/
  migrations/             ordered, append-only database migrations
tests/
  fixtures/               representative upstream contracts
  unit/                   deterministic tests without live infrastructure
  integration/            tests against explicitly configured services
deployment/               host and service provisioning helpers
benchmarks/               workload drivers, run recipes and results
config/                   tracked, non-secret configuration templates
```

The top-level `weow_ml.archive`, `weow_ml.capture`, `weow_ml.config`,
`weow_ml.registry`, and `weow_ml.streams` modules are compatibility entry
points. Existing development commands and imports can continue to use them;
new implementation code imports the service package directly.

The live Acquisition path uses the following focused modules:

- `policies.py` contains status, solar-phase and processing-eligibility decisions;
- `postgres.py` if SQL persistence grows beyond the registry implementation;
- `nfs.py` for atomic processing-image and initial-state publication;
- `images.py` for deterministic V0 slicing and JPEG construction;
- `kafka.py` for job construction and acknowledged publication;
- `service.py` for MQTT acknowledgement and runtime composition;
- `metrics.py` when application metrics are introduced in WP1.4.

## Dependency direction

`contracts.py`, `streams.py`, and `policies.py` contain deterministic domain
logic and do not perform network, filesystem, database, or environment access.
Infrastructure modules depend on those rules. `service.py` coordinates the
infrastructure modules but does not duplicate their implementation.

The operational notification path is:

```text
validate notification
  -> resolve and persist stream identities
  -> download and archive the complete image and embedded sidecar in one PUT
  -> publish processing images and initial state atomically on NFS
  -> publish and acknowledge one Kafka job per eligible processing stream
```

Every transition exposes an explicit success or failure to the coordinator.
Kafka publication cannot occur before the S3 archive and all required NFS
objects are complete. The accepted pilot crash behavior remains unchanged:
in-flight work may be lost or resent, and no durable Acquisition recovery log
is added.

## State and transaction boundaries

The registry owns the in-memory snapshots of `derived_stream` and
`processing_stream`. PostgreSQL remains authoritative. Registry mutations lock
the parent stream row, commit database changes before promoting cache entries,
and retain the persisted Kafka partition assignment when a profile is reused or
inherits its slice layout. The cache is reconstructed from PostgreSQL when the
service starts.

S3, NFS, and Kafka are separate systems and are not presented as one distributed
transaction. Their required publication order and idempotent object identities
provide the pilot's consistency boundary.

## Commands and tests

Diagnostic commands remain separate from the operational service:

```sh
python3 -m weow_ml tests/fixtures/notification_n0v0.json
python3 -m weow_ml.capture --count 10 --timeout 120
python3 -m weow_ml.archive [options] notification.json
python3 -m weow_ml.acquisition.service --check
```

Unit tests use in-memory fakes and fixtures. Integration tests must use explicit
test databases, object prefixes, Kafka topics, and stream identities. They must
never silently target operational resources. Live validation and benchmark
commands remain deliberate operator actions rather than part of the unit suite.
