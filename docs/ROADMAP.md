# Roadmap

## Prove the wedge

- Publish reproducible same-region Databricks-to-S3 benchmarks.
- Compare direct Spark writes, common serialized anti-patterns, and Dude.
- Measure Parquet, JSON Lines, compressed archives, KMS, and skewed datasets.

## Make tuning adaptive

- Estimate compressed output size from a bounded sample.
- Extend the initial throughput/retry controller with throttling, CPU, and NIC signals.
- Learn safe defaults per cluster and bucket without retaining payload data.
- Report the limiting stage with evidence from Spark and S3 telemetry.

## Expand the data plane

- Compact existing small objects.
- Incrementally transfer only changed partitions.
- Store durable checkpoints outside ephemeral driver storage.
- Add a distributed single-object writer for safely concatenable formats.
- Add OpenTelemetry metrics and structured audit events.

## Harden releases

- Run live integration tests against disposable S3 infrastructure.
- Add fault injection around upload, completion, and lost-response cases.
- Add support and compatibility matrices for Databricks runtimes.
- Commission a security review before declaring a stable release.
