# Benchmarks

Performance is part of Dude's API. Claims must be reproducible and must not use
sensitive or production data.

The Databricks notebook source in `databricks_s3.py` compares an ordinary Spark
write that preserves the current partition layout with Dude's planned write. It
uses unique destination prefixes, reports exact output bytes, and does not
delete benchmark output.

Record these dimensions with every result:

- Databricks runtime, access mode, worker and driver types, and worker count.
- Workspace and S3 regions and whether an S3 VPC endpoint is present.
- Source format, row count, logical size, partition count, and skew.
- Destination format, compression, encryption, and object count.
- Warm or cold source state and benchmark execution order.

Run each path at least three times and report the median plus the complete raw
results. A single favorable run is not a benchmark.
