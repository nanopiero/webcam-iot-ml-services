# WP1.5 Acquisition benchmark report

## Run identity

- Run ID:
- Scenario and repetition:
- UTC start/end:
- Git commit and container image ID:
- VM flavours and host kernel versions:
- Acquisition settings:
- EWC time-of-day window:
- Registry eligibility snapshot: whitelist / greylist / blacklist, and expected
  daytime or transition share:

## Workload accounting

| Measure | Warm-up | Measured |
|---|---:|---:|
| Planned notifications | | |
| Publisher-acknowledged notifications | | |
| Acquisition received | | |
| Terminal images | | |
| Archived images/sidecars | | |
| Published Kafka jobs | | |
| Panorama-derived jobs | | |
| Errors or unexplained differences | | |

## Latency and pressure

| Measure | p50 | p95 | p99 | maximum |
|---|---:|---:|---:|---:|
| Receipt to successful completion | | | | |
| Queue wait | | | | |
| Registry | | | | |
| Spool download | | | | |
| Archive | | | | |
| Image preparation | | | | |
| NFS publication | | | | |
| State initialization | | | | |
| Kafka publication | | | | |

- Peak and final queue depth:
- Burst drain time:
- Upstream download/publication delay distribution:

## Resources and storage

| Resource | Before | Peak | After | Growth |
|---|---:|---:|---:|---:|
| Acquisition CPU | | | | |
| Acquisition RSS | | | | |
| Host available memory | | | | |
| Network receive/transmit | | | | |
| Archive images | | | | |
| Archive sidecars | | | | |
| NFS processing images/state | | | | |
| Kafka topic/log | | | | |

- S3 GET/PUT operation rate and latency:
- NFS write operation rate and latency:
- PostgreSQL transaction rate and latency:

## Capacity projection and decision

- Projected archive, sidecar, NFS, and Kafka growth at nominal load:
- Acquisition-only backlog duration supported by current free capacity:
- Allocated Acquisition latency budget within the five-minute service target:
- Nominal acceptance result:
- Burst acceptance result and drain behavior:
- 200 image/s margin result:
- Bottleneck and required configuration or sizing change:
- Decision for WP1.6 controlled launch:
