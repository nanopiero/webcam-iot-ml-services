# Partial development status — 2026-09-17

This file records the temporary pause before starting the live Acquisition
service and the subsequent infrastructure upgrade.

Update after the pause: the VM now has 7.5 GiB usable RAM, and a dedicated
100 GiB partition is mounted at `/srv/weow-kafka`. Kafka has been migrated to
that mount and validated. Approximately 100 GiB remains unallocated on the same
disk for later expansion.

Kafka is active and enabled again. It retained the 50-partition operational
topic, completed an application-level produce/consume integration test, and
reported zero service restarts. Immediately after startup it used about 472 MB,
while the VM retained about 6.3 GiB available RAM. The VM still has no swap.

## Infrastructure state at the pause

- `weow-kafka.service` is stopped and disabled, so it cannot restart during the
  VM upgrade.
- No Acquisition service is running.
- The VM exposes 1.7 GiB of RAM and no swap. Before Kafka was stopped, only
  about 498 MiB was available. After it stopped, about 971 MiB was available.
- The root filesystem is 29 GiB with about 17 GiB free. The Kafka Podman volume
  currently resides on that root filesystem.
- `/srv/weow-nfs` is a separate 196 GiB filesystem with about 186 GiB free.

## Upgrade decision and operating limits

1. The RAM upgrade is complete. Adding a small swap area remains recommended
   for transient pressure.
2. Kafka initially receives 100 GiB. Since the architecture estimates
   2--25 GB/month, capacity alerts and a controlled stop threshold remain
   mandatory; the unallocated reserve can extend the filesystem later.
3. Keep Acquisition at four worker threads for the controlled start. Change
   this only after the WP1.5 benchmark measures throughput and peak memory.
4. Treat the 200 GB NFS as controlled-launch capacity. Measure processing-image
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
  development task now that the VM and Kafka storage upgrades are complete.

## Resume checks

After the upgrade, verify RAM/swap, mounts, ownership, Kafka volume placement,
Kafka topic configuration, PostgreSQL connectivity, S3 credentials, NFS
read/write visibility, and the complete test suite before enabling Kafka or
starting Acquisition.
