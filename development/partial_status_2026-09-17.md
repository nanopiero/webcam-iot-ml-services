# Partial development status — 2026-09-17

Development is deliberately paused before starting the live Acquisition
service. The `weow-a` VM must be upgraded first.

## Infrastructure state at the pause

- `weow-kafka.service` is stopped and disabled, so it cannot restart during the
  VM upgrade.
- No Acquisition service is running.
- The VM exposes 1.7 GiB of RAM and no swap. Before Kafka was stopped, only
  about 498 MiB was available. After it stopped, about 971 MiB was available.
- The root filesystem is 29 GiB with about 17 GiB free. The Kafka Podman volume
  currently resides on that root filesystem.
- `/srv/weow-nfs` is a separate 196 GiB filesystem with about 186 GiB free.

## Required upgrade before resuming

1. Increase `weow-a` to at least 4 GiB of RAM; 8 GiB is preferred for Kafka and
   Acquisition together. Add a small swap area for transient pressure.
2. Attach a dedicated Kafka data volume. Size it from the architecture's
   2--25 GB/month single-replica estimate and the desired one-year retention;
   400--500 GB provides headroom for the upper estimate.
3. Relocate `weow-kafka-data` to that volume before re-enabling the service.
4. Keep Acquisition at four worker threads for the controlled start. Change
   this only after the WP1.5 benchmark measures throughput and peak memory.
5. Treat the 200 GB NFS as controlled-launch capacity. Measure processing-image
   growth before widening the workload because checkpoint-based cleanup is not
   available until consumers run.

## Implemented development state

- WP1.1 notification contracts, PostgreSQL registry, schema, stream generation,
  metadata updates, status overrides, and integration coverage are present.
- WP1.2 verified S3 transfer, atomic NFS publication, Kafka deployment and
  acknowledged publication foundations are present.
- WP1.3 includes deterministic slicing, solar/status eligibility, the automatic
  blacklist above 68.56 degrees north, ordered archive/NFS/Kafka handling, and
  bounded worker concurrency with one short retry.
- Initial state uses schema `S0V0`: an empty `(0, 2048)` float32 latent bank,
  empty cluster counts, and empty arrays representing null freshness signature
  and last ingestion timestamp. Publication is immutable and atomic.
- The runnable MQTT service composition has not been started. It is the next
  development task after the VM and Kafka storage upgrades.

## Resume checks

After the upgrade, verify RAM/swap, mounts, ownership, Kafka volume placement,
Kafka topic configuration, PostgreSQL connectivity, S3 credentials, NFS
read/write visibility, and the complete test suite before enabling Kafka or
starting Acquisition.
