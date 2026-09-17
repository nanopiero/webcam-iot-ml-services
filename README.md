# webcam-iot-ml-services

ML services for the EUMETNET Webcam Service pilot.

The project extracts meteorological information from ground-based webcam images and provides the processing layer between the upstream webcam ingestion service and the downstream sharing system.

The initial implementation builds on the existing WEOW processing chain developed at Météo-France and focuses primarily on:

* on-ground snow detection;
* horizontal visibility estimation.

The new implementation reworks the existing WEOW processing logic for deployment on the European Weather Cloud (EWC) and for operation at a substantially larger scale, with a pilot target of approximately 25,000 webcams across Europe.

## Role of the service

The ML services are responsible for:

* acquiring image notifications and associated metadata from the upstream ingestion service;
* retrieving and preparing images for processing;
* maintaining the processing streams and their associated state;
* scheduling image-processing jobs through Kafka;
* executing the meteorological inference algorithms;
* persisting raw and post-processed outputs;
* maintaining the state required by temporally dependent algorithms;
* publishing eligible observations to the downstream sharing system;
* supporting operational replay, backfill, calibration and qualification, and maintenance workflows;
* exposing metrics required to monitor processing throughput, latency, failures, backlog, and infrastructure usage.

The architecture separates image acquisition from image processing so that temporary processing interruptions do not prevent image archival and job creation. Processing jobs are queued in Kafka and can subsequently be consumed while preserving the ordering required by stateful processing streams.

The service relies on PostgreSQL for persistent application data, object storage for durable image and metadata archives, NFS for transient processing data and state material, Kafka for processing queues, Kestra for workflow orchestration, and Prometheus/Grafana for observability.

The processing architecture is designed to support both near-real-time operation and controlled non-real-time workflows such as catch-up, backfill and replay. Operational processing applies eligibility and freshness rules before outputs are disseminated, while maintenance and replay workflows can use different processing and publication semantics.

## Development status

The acquisition implementation now provides validated notification contracts,
a PostgreSQL stream registry, solar and status policies, verified S3 archival,
atomic NFS image publication, acknowledged Kafka jobs, and bounded worker
concurrency. A versioned initial-state NPZ contract is also defined. The
runnable MQTT service composition remains in progress and is paused until the
Acquisition VM memory and Kafka storage are upgraded.
See [the acquisition development notes](development/acquisition_contract.md)
for usage, tests, infrastructure observations, and remaining integration work.
The service boundaries and dependency rules are recorded in
[the code-organization note](development/code_organization.md).

This repository is currently under active development. The architecture and the pre-existing WEOW implementation should be considered the primary references while the new services are being implemented.

### Temporary development references

The following paths are local development references and are intended to be removed from this README once the implementation is sufficiently mature:

* Intended ML-services architecture:
  `~/doc/ML_services_architecture/architecture`

* Architecture of the upstream ingestion service:
  `~/doc/ML_services_architecture/architecture_upstream_ingestion_service`

* Previous WEOW implementation:
  `~/doc/ML_services_architecture/WEOW`

* Development plan to be followed during implementation:
  `~/doc/ML_services_architecture/plan`
