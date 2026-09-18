# WP1.5 smoke result: wp15_smoke_001

## Identity

- Date: 2026-09-18 UTC
- Scenario: `smoke`, seed 1
- Workload start identity: 2026-09-18T08:02:00Z
- Result collection completed: 2026-09-18T08:16:22Z
- Code: `6d6db4b` (Acquisition image content from `94c8c4c`; later commits
  changed only the host-side publisher)
- Container image:
  `7d30fe555c952fb7065d5a720cc12063c11f98cc3b9420f1c976a4bc7f5d3b5b`
- Host: weow-a, kernel 5.14.0-687.48.1.el9_8.x86_64

The dedicated `weow_ml_benchmark` database and
`inference.jobs.benchmark` topic were provisioned. The topic has 50 partitions,
replication factor 1, one-year delete retention, and 256 MiB segments. The full
Acquisition preflight passed for PostgreSQL, both S3 roles, NFS, Kafka, and MQTT.

## Driver findings

The first attempt exposed that the publisher did not wait for MQTT v5 connection
completion. Commit `e2f4ffd` added the connection barrier. The next attempt
exposed a client-ID collision between the subscriber and publisher; the broker
correctly allowed only one connection with that identity. Commit `6d6db4b`
assigned a distinct publisher identity. Metrics confirmed that neither failed
attempt delivered a notification. The clean run then published all 12 messages
in 11.002 seconds with a maximum schedule lag of 1.83 ms.

## Accounting

| Measure | Result |
|---|---:|
| Planned and publisher-acknowledged notifications | 12 |
| Acquisition received/completed/published outcomes | 12 / 12 / 12 |
| Archived image bytes | 593,371 |
| S3 spool images | 12 (593,371 bytes) |
| S3 archived images | 12 (593,371 bytes) |
| S3 sidecars | 12 (29,344 bytes) |
| PostgreSQL derived streams / profiles / processing streams | 5 / 5 / 8 |
| NFS processing images / initial states | 21 / 8 |
| NFS total files / bytes | 29 / 550,548 |
| Kafka records | 21 |
| Stage failures | 0 |
| Final queue / active workers | 0 / 0 |

One selected 1600x288 panorama stream occurred three times and produced four
processing images/jobs per occurrence. The other nine images produced one job
each, hence 21 jobs and processing images. Eight states match four standard
processing streams plus four panorama slices.

## Latency sample

Receipt-to-completion latency totalled 8.389 seconds for 12 images, an average
of 0.699 seconds. Histogram estimates are p50 <= 0.5 seconds, p95 approximately
2.2 seconds, and p99 approximately 2.44 seconds. These coarse estimates come
from Prometheus bucket interpolation and are smoke diagnostics, not acceptance
measurements.

Stage totals across all 12 images were:

| Stage | Total seconds | Mean seconds/image |
|---|---:|---:|
| Registry | 0.393 | 0.033 |
| Solar | 0.002 | <0.001 |
| Download | 0.722 | 0.060 |
| Archive | 4.986 | 0.416 |
| Image preparation | 0.028 | 0.002 |
| NFS publication | 1.971 | 0.164 |
| State initialization | 0.133 | 0.011 |
| Kafka publication | 0.137 | 0.011 |

All queue waits were below 5 ms. Archive and NFS publication dominated this
small sequential sample; the nominal and burst runs are required before drawing
capacity conclusions.

## Host state after the run

- Memory: 7.5 GiB total, 5.6 GiB available, no swap.
- Root filesystem: 15 GiB available of 29 GiB.
- NFS filesystem: 186 GiB available of 196 GiB.
- Kafka filesystem: 93 GiB available of 98 GiB.
- Benchmark service stopped and left inactive after collection.

## Remaining setup before nominal measurement

The local metrics endpoint was healthy at `192.168.1.135:9102`, but weow-o was
blocked by weow-a's firewall. The Prometheus configuration and Grafana latency
panel are installed. TCP 9102 must be allowed from `192.168.1.6/32`, then the
`acquisition-benchmark` target must be confirmed up before the nominal run.
