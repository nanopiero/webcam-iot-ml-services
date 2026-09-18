# WP1.5 run recipe

Use a new run ID for every scenario and repetition. Replace every
`REQUIRED_RUN_ID` in `config/acquisition.benchmark.example.json`, save the
result as `.secrets/acquisition.benchmark.json`, and keep it mode 600. The
benchmark subscribes to the real `webcam/T0` feed for the configured duration
and reads each referenced image from `webcam-ingestion-buffer`. It does not
prepare or publish a synthetic workload. Outputs remain isolated in the
benchmark database, Kafka topic, NFS prefix, and WEOW archive prefix.

Provision the dedicated database and Kafka topic with the repository's
idempotent provisioning commands. Build the current Acquisition image, install
`deployment/weow-acquisition-benchmark.service` as a user unit, and update the
Prometheus deployment before collecting a run. Capture baseline host, NFS,
Kafka, PostgreSQL, and S3 metrics, then start the service. It disconnects from
MQTT after `benchmark.duration_seconds` and drains admitted work before exit.
Repeat representative 15-minute runs at different EWC times of day because the
live notification count, image mix, and shared S3 pressure vary.

For published outcomes, query p50, p95, and p99 from
`weow_acquisition_notification_completion_seconds`; this spans MQTT receipt,
all Acquisition stages, Kafka acknowledgement, and the MQTT acknowledgement.
Use `weow_acquisition_stage_seconds`, worker queue and wait metrics, node
exporter metrics, Kafka log size, and before/after S3 and NFS measurements for
the remaining report fields. A burst is complete only after the queue returns
to zero.

Keep the service's `Restart=no` failure state and logs for diagnosis rather
than automatically relaunching it. Do not delete
run objects, database rows, NFS files, or Kafka records until the report's
counts and storage-growth measurements have been recorded.
