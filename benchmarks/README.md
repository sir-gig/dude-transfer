# Benchmarks

Performance is part of Dude's API. Claims must be reproducible and must not use
sensitive or production data.

The Databricks notebook source in `databricks_s3.py` compares an ordinary Spark
write that preserves the current partition layout with Dude's planned write. It
uses unique destination prefixes, reports exact output bytes, and does not
delete benchmark output.

`compose_s3.py` compares Dude's S3-side composition with the same ordered bytes
downloaded to local driver storage and uploaded again. It alternates execution
order across three repetitions, records client payload bytes, uses unique
destinations, and never deletes objects. Run it only with synthetic,
byte-concatenable sources:

```bash
python benchmarks/compose_s3.py s3://bucket/dude-benchmark \
  s3://bucket/stage/0000.bin s3://bucket/stage/0001.bin
```

The baseline deliberately uses local disk and is capped at 20 GiB unless
`--max-local-size` is explicitly raised. This harness measures the staging-to-one-
object wedge; it does not imply that Parquet shards can be concatenated.

`databricks_single_jsonl.py` is the direct benchmark for Dude's high-level
DataFrame path. It alternates an intentionally serialized `coalesce(1)` JSON
Lines write with parallel executor staging plus S3-side composition. Upload it
as a Databricks notebook, configure an authorized synthetic source and unique
benchmark prefix, and publish the complete JSON report rather than a screenshot.

`databricks_skew_repair.py` creates a synthetic, uncompressed JSON Lines dataset
with a deliberately tiny first partition. It verifies that the first staging
layout is rejected, Dude chooses a measured Spark reshuffle, and the successful
result still reports zero client-downloaded payload bytes. Use it to publish the
repair overhead and every `staging_attempts` record, not only the final runtime.

Record these dimensions with every result:

- Databricks runtime, access mode, worker and driver types, and worker count.
- Workspace and S3 regions and whether an S3 VPC endpoint is present.
- Source format, row count, logical size, partition count, and skew.
- Destination format, compression, encryption, and object count.
- Warm or cold source state and benchmark execution order.

Run each path at least three times and report the median plus the complete raw
results. A single favorable run is not a benchmark.
