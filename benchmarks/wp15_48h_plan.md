# WP1.5 48-hour live endurance plan

## Purpose

Run Acquisition continuously against the real `webcam/T0` feed and real
`webcam-ingestion-buffer` objects for 48 hours. Confirm that burst handling,
stream discovery, memory, queue depth, and storage growth remain bounded over a
longer traffic cycle.

## Isolated run

- Run ID: `wp15_live_48h_001`
- Duration: 172,800 seconds from successful MQTT subscription
- Workers: 24
- Input: real MQTT notifications and ingestion spool
- Outputs: benchmark PostgreSQL database, `inference.jobs.benchmark`, and
  `benchmarks/wp1_5/wp15_live_48h_001` prefixes on WEOW S3 and NFS
- Archive: one conditional JPEG-bundle PUT per source image
- NFS hard stop: fail closed when free space falls below 20 GiB
- Orchestration: manual Kestra execution `weow.benchmarks.acquisition_benchmark_48h`

Use a fresh output prefix. Do not prepare a synthetic spool or run the workload
publisher. Leave the ingestion bucket unchanged.

## Capacity expectation

The 15-minute live run observed 20.4 notifications/second and allocated about
422 MB of processing-JPEG content. At that mix, 48 hours should use roughly
70–85 GB plus stream-state and filesystem overhead. Scaling to the nominal
33.3 images/second gives approximately 140–155 GB. The clean NFS has about
199 GB available, so the 20 GiB stop threshold preserves a bounded margin if
traffic or image sizes exceed the estimate.

Kafka has approximately 99.6 GB free and measured close to 1 KiB per job. Its
48-hour nominal growth is only about 5.8 million jobs, or roughly 6 GB; Kafka
disk and heap are not expected to constrain this run.

## Before launch

1. Confirm the benchmark NFS prefix is absent and record filesystem bytes.
2. Set the fresh run ID, output prefix, 172,800-second duration, 24 workers,
   and 20 GiB minimum-free threshold in the local benchmark configuration.
3. Build the exact committed image and record its digest.
4. Run the dependency preflight and verify the Prometheus target and alerts.
5. Record Kafka offsets, benchmark database counts, host memory, and disk
   baselines.
6. Import the Kestra flow, then launch it manually. Its `run_and_follow` task
   remains RUNNING for the test and writes a five-minute progress line to the
   execution log.

## Monitoring and stop conditions

Record counters at least every five minutes and durable counts every six hours.
Stop the run immediately if any of these occurs:

- NFS free space falls below 20 GiB; the service enforces this condition.
- Acquisition records a stage failure or exits unexpectedly.
- Worker queue grows continuously for ten minutes.
- MQTT received/completed divergence does not drain after an arrival burst.
- Container memory exceeds 1.5 GiB or grows monotonically across six-hour
  checkpoints without stabilizing.
- Kafka or NFS becomes unavailable.

At 24 hours, review the data without restarting the service. Preserve one
continuous process lifetime so the run measures memory and registry-cache
behavior.

## Acceptance and closeout

- The service remains subscribed for 48 hours and exits cleanly on its timer.
- Every admitted notification reaches a terminal result after the final drain.
- No unexplained loss, stage failure, or sustained worker backlog occurs.
- Bursts drain and p95/p99 completion latency remains compatible with the
  five-minute service budget.
- NFS remains above the 20 GiB reserve; Kafka and memory growth remain bounded.
- A real archive bundle recovers a valid sidecar and byte-identical source JPEG.
- Kafka jobs equal NFS processing images after drain.

Write a final report before deleting NFS test data. Retain S3, Kafka, and
PostgreSQL evidence unless a later explicit cleanup decision changes that.
