# Dude

[![CI](https://github.com/sir-gig/dude-transfer/actions/workflows/ci.yml/badge.svg)](https://github.com/sir-gig/dude-transfer/actions/workflows/ci.yml)
[![CodeQL](https://github.com/sir-gig/dude-transfer/actions/workflows/codeql.yml/badge.svg)](https://github.com/sir-gig/dude-transfer/actions/workflows/codeql.yml)

Dude makes the fast path from Databricks to Amazon S3 the default. It plans
distributed DataFrame writes, detects serialization bottlenecks, and provides a
resumable concurrent multipart uploader when the destination must be one S3
object.

## Project status

Dude is an open-source alpha under Apache-2.0. It is a working engineering
prototype, not yet a proven replacement for production transfer systems. Do not
publish performance claims until they are reproduced with the benchmark harness
and workload details in [`benchmarks/`](benchmarks/).

The command remains `dude`; the Python import is `dude_transfer` to avoid a
collision with an unrelated package already using the `dude` import namespace.

## Why

A multi-gigabyte transfer should not silently run through one driver thread.
Dude separates the two workloads that are often accidentally mixed together:

- **Spark datasets** are written directly by executors as multiple Parquet,
  Delta, JSON, or CSV objects.
- **Single files** are split into checksummed S3 multipart uploads with bounded
  memory, adaptive concurrency, retries, durable checkpoints, and independent
  completion verification.

## Install on Databricks

From the repository root:

```bash
pip install -e ".[s3]"
```

For a wheel suitable for a cluster library:

```bash
pip install build
python -m build
```

Install the resulting file from `dist/` as a Databricks workspace or cluster
library. Spark is supplied by Databricks and is intentionally not a package
dependency.

## Send a DataFrame

```python
from dude_transfer import diagnose_dataframe, send_dataframe

for finding in diagnose_dataframe(df):
    print(finding.severity, finding.code, finding.message)

result = send_dataframe(
    df,
    "s3://my-lab-data/diagnostics/run_id=2026-08-05/",
    file_format="parquet",
    compression="snappy",
    target_file_size="256MiB",
    mode="overwrite",
)

print(result.as_dict())
```

Dude reads Spark plan metadata without calling `count()` or collecting rows to
the driver. If the DataFrame is under-partitioned, it increases parallelism. If
it has far too many partitions, it coalesces them before the write. The result's
throughput is marked as estimated because Spark's logical byte statistics are
not the exact compressed S3 object size.

Use an external location or external volume backed by the target S3 prefix so
Unity Catalog governs the write. Do not place AWS access keys in notebook code.

## Send one file

```python
from dude_transfer import MultipartConfig, send_file

result = send_file(
    "/local_disk0/diagnostics.tar.zst",
    "s3://my-lab-data/diagnostics/diagnostics.tar.zst",
    config=MultipartConfig(
        part_size="64MiB",
        max_concurrency=8,
        initial_concurrency=2,
        adaptive=True,
        max_in_flight_bytes="512MiB",
        checksum_algorithm="CRC32",
        resume=True,
        verify_after_upload=True,
    ),
    create_args={
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "alias/lab-data",
        "ExpectedBucketOwner": "123456789012",
    },
)
```

Dude uses boto3's standard credential chain, such as the compute instance role.
It never accepts AWS access keys. Failed multipart transfers retain both their
remote upload and a local `.dude-upload.json` checkpoint. Running the same call
again reconciles completed parts with S3 and resumes the remainder. Successful
uploads remove the checkpoint.

Adaptive uploads start with two connections, probe upward while aggregate
throughput improves, and back off when requests require retries. The configured
memory ceiling remains authoritative. Every part completion and concurrency
change can also be consumed as a structured `TransferEvent`.

`verify_after_upload=True` performs a `HeadObject` check against the exact byte
length and Dude's random transfer identity. It requires `s3:GetObject` in
addition to the write permissions required by multipart upload.

### CLI

```bash
dude plan-file /local_disk0/diagnostics.bin s3://bucket/diagnostics.bin
dude send-file /local_disk0/diagnostics.bin s3://bucket/diagnostics.bin \
  --part-size 64MiB --initial-concurrency 2 --concurrency 8 \
  --max-memory 512MiB --verify --json-events
```

## Current scope

Version 0.1 deliberately focuses on the two critical paths. Its first adaptive
controller reacts to measured part throughput and retries, but does not yet use
cluster CPU, NIC, or S3 request metrics. It also does not calculate exact
DataFrame output sizes, compact existing S3 small files, or distribute parts of
a single byte stream across Spark executors.

## Contributing and security

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development and benchmark rules,
[`SECURITY.md`](SECURITY.md) for data-handling and vulnerability guidance, and
[`docs/ROADMAP.md`](docs/ROADMAP.md) for the path from useful prototype to a
distinctive adaptive transfer engine.

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
