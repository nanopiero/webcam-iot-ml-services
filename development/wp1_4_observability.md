# WP1.4 observability and orchestration

Acquisition exposes Prometheus data on `192.168.1.135:9101`. Metric labels are
limited to predefined outcomes and processing stages; webcam, source-stream,
and processing-stream identifiers are deliberately absent. The readiness gauge
becomes `1` only after the MQTT broker accepts the subscription.

Prometheus and Grafana run on `weow-o` and listen only on its private address,
`192.168.1.6`. Prometheus retains at most 15 days or 8 GB, whichever limit is
reached first. The initial dashboard covers reception and filtering outcomes,
worker pressure, stage latency and failure counts, archive throughput, Kafka job
publication, and filesystem capacity. Node exporter supplies CPU, memory,
network, and filesystem measurements for `weow-a`, `weow-db`, `weow-o`, and
`weow-p0`.

The Kestra flows use a private-key secret named `WEOW_A_SSH_PRIVATE_KEY` and
the certified SSH task to manage the user service on `weow-a`. Their schedule
triggers remain disabled until controlled launch. Systemd performs immediate
failure restarts; Kestra records the periodic supervision action and the daily
00:00 UTC restart that reconstructs the in-memory dictionaries.

Deployment order:

1. Install Podman on `weow-o`, then run `deployment/bootstrap_observability.sh`.
2. Run `deployment/bootstrap_node_exporter.sh` on each monitored VM.
3. Permit TCP 3000 and 9090 on the private interface of `weow-o`, TCP 9101 on
   the private interface of `weow-a`, and TCP 9100 on each monitored VM.
4. Add the SSH private key to Kestra's secret backend, import both flows, and
enable their triggers only when Acquisition is approved for launch.

Kestra uses an isolated `kestra` role and database in the pilot's single
PostgreSQL 16 instance. Its repository and queue are PostgreSQL-backed; workflow
files use persistent local storage on `weow-o`. The standalone service is pinned
to Kestra 2.0.2 with an eight-thread worker and a 2 GB JVM heap ceiling.
The configured Basic Auth username is `admin@weow.local`; its generated password
remains in the mode-0600 host configuration directory.

The initial configuration does not infer data loss from a restart. It reports
the stop and recovery and retains the accepted architecture behavior: in-flight
notifications can be lost or resent.
