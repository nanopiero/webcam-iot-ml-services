# WP1.5 Acquisition benchmark

The workload has three deliberate stages: offline planning, isolated spool
preparation, and paced MQTT publication. The planner has no network side
effects. The other commands require the fail-closed benchmark configuration.

| Scenario | Warm-up | Measured load |
|---|---:|---:|
| `smoke` | 2 images at 1/s | 10 images at 1/s |
| `nominal` | 600 images at 10/s | 10,000 images over 5 minutes |
| `burst` | 600 images at 10/s | 10,000 images over 1 minute |
| `margin` | 600 images at 10/s | 12,000 images at 200/s |

Streams use deterministic 5:2:1 rate weights, five percent receive a metadata
change in the second half of the measured phase, and ten percent use the
architecture's 1600x288 panorama case. Standard image dimensions and byte
sizes come from the ten locally captured upstream notifications. Synthetic
pixels avoid retaining provider imagery; every generated JPEG is decodable and
has the exact recorded transfer size. Operational eligibility must be measured
from the registry immediately before the live runs and recorded in the report;
it is not replaced with an invented ratio in the driver.

All run data are isolated as follows:

- network and stream IDs use `benchmark` and `bm...`;
- spool and output objects use `benchmarks/wp1_5/<run-id>/`;
- Kafka uses `inference.jobs.benchmark`;
- PostgreSQL uses `weow_ml_benchmark`;
- metrics use port 9102 and Prometheus job `acquisition-benchmark`.

Benchmark mode refuses startup when any of these writable targets differs.
Notifications from another run or object prefix are also rejected before a
database or storage operation. Spool preparation writes a local receipt, and
publication refuses a receipt for a different run, scenario, timestamp,
bucket, prefix, or object count.

Inspect an offline plan:

```sh
python -m weow_ml.benchmark.workload --scenario smoke --run-id local_001
```

The exact infrastructure and execution sequence is in `run_recipe.md`. Record
results using `report_template.md`.
