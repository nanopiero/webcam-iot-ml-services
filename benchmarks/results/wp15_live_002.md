# WP1.5 live-input result: wp15_live_002 (accepted)

## Result

The 24-worker Acquisition configuration kept up with the real upstream MQTT
feed for 15 minutes while reading the corresponding real images from
`webcam-ingestion-buffer`. The worker queue remained empty throughout the run,
including observed 30-second bursts above 40 notifications/second. No
Acquisition stage failure was recorded.

This live run is accepted as the WP1.5 capacity check. A one-week endurance run
will cover longer-term traffic variation and bursts. That run must include
consumers or additional NFS capacity: the current 200 GB NFS holds only about
three nominal days of processing images.

## Identity and configuration

- Run ID: `wp15_live_002`
- UTC interval: 2026-09-18 12:08:19–12:23:23
- Input: real `webcam/T0` notifications and ingestion-spool objects
- Acquisition workers: 24
- Container image: `d5d0f73120e02833d3cc13f8b974567a2ec9914e7d605d0c4ca9992660c41f05`
- Output isolation: benchmark PostgreSQL database, Kafka topic, and
  `benchmarks/wp1_5/wp15_live_002` S3/NFS prefix
- Archive format: one conditional PUT per source image; the compressed JSON
  sidecar is embedded in JPEG APP15 segments

## Accounting

The final Prometheus scrape, a few seconds before the timed disconnect, showed
18,368 received and 18,348 completed notifications, with 20 active and none
queued. Shutdown waited for admitted workers. Durable-output reconciliation
after exit found:

| Measure | Result |
|---|---:|
| Archived source JPEG/sidecar bundles | 18,231 |
| Archive bytes | 440,471,133 |
| Kafka jobs added | 18,317 |
| NFS processing JPEGs | 18,317 |
| NFS initialized stream states | 13,667 |
| NFS bytes | 774,714,722 |
| Stage failures | 0 |
| Peak observed worker queue | 0 |

One real archive object was downloaded and checked before cleanup. It decoded
as JPEG, its embedded sidecar decoded as JSON, its recorded digest matched, and
the recovered source JPEG was byte-identical to the object in the ingestion
spool.

## Performance evidence

- Mean observed arrival rate through the final scrape: 20.4 notifications/s.
- Observed 30-second bursts exceeded 40 notifications/s and drained without a
  queue.
- Published-notification completion latency: p50 0.52 s, p95 2.21 s,
  p99 4.15 s.
- Queue-wait p99: 5.7 ms.
- Last observed container memory: 435 MiB of the 2 GiB limit.
- Last observed CPU: approximately 30%.

Mean stage time per invocation was approximately 317 ms for registry
resolution, 26 ms for spool download, 81 ms for the combined archive PUT,
2 ms for image preparation, 141 ms for NFS publication, 58 ms for state
initialization, and 33 ms for acknowledged Kafka publication. These times
overlap across 24 workers.

The timer-induced MQTT disconnect initially returned Paho's not-connected code
and made the otherwise successful container exit with status 2. The service now
treats its configured timed disconnect as a clean exit and records final
counters after draining workers.

## Capacity and decision

Kafka uses a 1 GiB maximum heap and stores backlog on its dedicated disk. The
measured jobs occupy approximately 1 KiB each. Roughly 99.6 GB was free, giving
about 31 nominal days of Kafka backlog, or about 25 days while retaining 20%
free space.

NFS is the earlier limit. At the measured image size and the nominal rate of
10,000 images per five minutes, its 200 GB capacity holds about three days of
processing images. Acquisition may start before consumers, as planned, but the
consumer launch must precede that limit. The planned one-week test requires
active consumers or more NFS capacity.

