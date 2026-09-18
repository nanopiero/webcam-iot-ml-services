# WP1.5 nominal result: wp15_nominal_001 (failed)

## Result

The four-worker Acquisition configuration did not sustain the nominal workload.
The publisher delivered and received broker acknowledgement for all 10,600
messages in 359.97 seconds with 5.9 ms maximum scheduling lag. Acquisition
completed approximately 6.0 images/s and received only 2,449 messages. Its MQTT
outbound queue then emptied with 8,151 acknowledged publications never delivered
to Acquisition.

This exposes two related limits:

1. Four Acquisition threads provide substantially less than the nominal
   33.33-image/s measured rate.
2. The broker's bounded subscriber queue cannot serve as a durable Acquisition
   backlog. Acquisition's own queue remained at 16 waiting plus four active
   workers, so its local queue metric alone concealed the upstream loss.

No Acquisition stage failed. Every message that Acquisition received reached a
consistent terminal result.

## Identity and configuration

- Run ID: `wp15_nominal_001`
- Workload: 600 warm-up images at 10/s, then 10,000 images over five minutes
- Acquisition workers: 4
- Code: `03580ff`
- Container image:
  `dc7afc22d371ae29198146925fe5b30cc7928c1e9ab66af47dfa5e6565e2f14e`
- Kafka starting offset total: 21
- Prometheus target `acquisition-benchmark`: up

## Accounting

| Measure | Result |
|---|---:|
| Planned and publisher-acknowledged notifications | 10,600 |
| Acquisition received/completed | 2,449 / 2,449 |
| Missing before Acquisition receipt | 8,151 |
| Archived images / sidecars | 2,449 / 2,449 |
| Archived image / sidecar bytes | 82,079,495 / 6,291,044 |
| Kafka jobs | 3,202 |
| NFS processing images / states | 3,202 / 1,062 |
| NFS bytes | 79,030,664 |
| Derived / processing streams | 810 / 1,062 |
| Final queue / active workers | 0 / 0 |
| Stage failures | 0 |

The S3 test spool contains all 10,600 inputs and 343,769,857 bytes. Kafka ended
at 3,223 records including the 21-record smoke baseline, matching the 3,202 jobs
from this run.

## Performance evidence

- Five-minute Prometheus completion rate: approximately 6.02 images/s.
- Completion time accumulated across received images: 10,080.05 seconds,
  averaging 4.12 seconds per image while the local queue was saturated.
- Container sample: approximately 114 MB resident memory and 8% CPU.
- Four workers remained active and 16 messages remained locally queued during
  load; the bottleneck was I/O concurrency rather than CPU or memory.

## Decision

This run fails nominal acceptance and cannot be used for capacity projection.
A fresh run with distinct stream identities and 24 Acquisition threads will test
whether increased I/O concurrency clears 33.33 images/s without broker loss.
The failed run's database, S3, NFS, Kafka, and Prometheus evidence is retained.
