# WP1.5 run recipe

Use a new run ID for every scenario and repetition. Replace every
`REQUIRED_RUN_ID` in `config/acquisition.benchmark.example.json`, save the
result as `.secrets/acquisition.benchmark.json`, and keep it mode 600. The
spool and archive both use the WEOW bucket, under disjoint run prefixes; no
write access to the ingestion bucket is required.

Provision the dedicated database and Kafka topic with the repository's
idempotent provisioning commands. Build the current Acquisition image, install
`deployment/weow-acquisition-benchmark.service` as a user unit, and update the
Prometheus deployment before collecting a run. Do not start the service yet.

Choose a UTC start timestamp and retain it unchanged through both commands:

```sh
run_id=wp15_smoke_001
start=2026-09-19T10:00:00Z
receipt=/tmp/${run_id}.spool.json

python -m weow_ml.benchmark.spool \
  --config .secrets/acquisition.benchmark.json \
  --scenario smoke --start "$start" --receipt "$receipt"
```

The spool command performs conditional writes and read-back verification. It
can be rerun with the same arguments; it rejects existing objects whose bytes
differ. Start the benchmark Acquisition service only after this command has
completed and its Prometheus target is up.

Capture baseline host, NFS, Kafka, PostgreSQL, and S3 metrics, then publish:

```sh
python -m weow_ml.benchmark.publish \
  --config .secrets/acquisition.benchmark.json \
  --scenario smoke --start "$start" --receipt "$receipt"
```

Run `smoke` first. Continue with `nominal`, `burst`, and `margin` only after the
preceding run has the expected input, terminal outcome, archive, and job counts
and no unexplained failures. Use a new run ID, configuration, timestamp, and
receipt each time. Repeat representative runs at different EWC times of day.

For published outcomes, query p50, p95, and p99 from
`weow_acquisition_notification_completion_seconds`; this spans MQTT receipt,
all Acquisition stages, Kafka acknowledgement, and the MQTT acknowledgement.
Use `weow_acquisition_stage_seconds`, worker queue and wait metrics, node
exporter metrics, Kafka log size, and before/after S3 and NFS measurements for
the remaining report fields. A burst is complete only after the queue returns
to zero.

Stop the benchmark service after each run. Keep its `Restart=no` failure state
and logs for diagnosis rather than automatically relaunching it. Do not delete
run objects, database rows, NFS files, or Kafka records until the report's
counts and storage-growth measurements have been recorded.
