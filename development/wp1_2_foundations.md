# WP1.2 infrastructure foundations

## NFS

The Acquisition host has a 200 GB attached ext4 volume mounted at
`/srv/weow-nfs`. It is exported to `192.168.1.0/24` using NFSv4 with `rw`,
`sync`, `root_squash`, and `no_subtree_check`. `weow-p0` mounts the export at
`/mnt/weow-nfs` using NFSv4.2.

The directory is owned by `root:weow` with mode `2775`. `codexuser` is a member
of the `weow` group on both hosts. A write from `weow-a` under that group and
write access from `weow-p0` have been verified. Long-running processes started
before group membership was added must be restarted to acquire the supplementary
group; new services receive it normally.

Acquisition will publish processing images and initial state using a temporary
file in the destination directory followed by an atomic rename. Consumer VMs
will only use final names.

## Kafka

The pilot broker uses the official JVM image `apache/kafka:4.1.2` as a single
combined KRaft broker/controller. It listens only on the `weow-a` private
address `192.168.1.135:9092`; the controller listener is container-internal.
The operational topic is `inference.jobs.live`, with 50 partitions,
replication factor 1, delete retention, one-year `retention.ms`, and 256 MiB
segments. Automatic topic creation is disabled.

The rootless service is defined in `deployment/weow-kafka.service` and installed
by `deployment/bootstrap_kafka.sh`. Its KRaft cluster identifier is generated
once into an ignored, mode-600 local environment file. Broker data are stored
on the dedicated ext4 filesystem mounted at `/srv/weow-kafka`, which the user
service requires before starting. The container bind-mounts
`/srv/weow-kafka/data` at `/var/lib/kafka/data`.

`codexuser` lingering is enabled on `weow-a`, so the user service persists after
logout.

Deployment validation confirmed that the service remains active with zero
restarts, the broker advertises `192.168.1.135:9092`, `weow-p0` can reach that
listener, and a temporary isolated topic completed one produce/consume round
trip before being deleted. No synthetic record was written to the operational
topic.

NFS validation published one marker through the application atomic-publication
primitive, read the completed bytes from `weow-p0`, confirmed that no temporary
name was visible, and removed the marker afterwards.

## Capacity status before sustained Acquisition

`weow-a` has 7.5 GiB usable RAM. Kafka uses about 472 MB immediately after
startup, leaving about 6.3 GiB available before Acquisition starts. The broker
filesystem provides approximately 93 GiB usable free space. A further 100 GiB
remains unallocated on the same block device for later expansion.

The architecture estimates roughly 2--25 GB of job records per month before
replication, so 100 GB does not guarantee one year at the upper estimate.
Before sustained Acquisition starts, measure actual bytes per published job and
set capacity alerts and a controlled stop threshold. The NFS volume is reserved
for processing images and state and is not counted as Kafka capacity.

References: the Apache Kafka 4.1 Docker image documentation and its official
single-node plaintext example.
