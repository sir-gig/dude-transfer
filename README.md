# Dude

[![CI](https://github.com/sir-gig/dude-transfer/actions/workflows/ci.yml/badge.svg)](https://github.com/sir-gig/dude-transfer/actions/workflows/ci.yml)
[![CodeQL](https://github.com/sir-gig/dude-transfer/actions/workflows/codeql.yml/badge.svg)](https://github.com/sir-gig/dude-transfer/actions/workflows/codeql.yml)

Dude makes the fast path from Databricks to Amazon S3 the default. It plans
distributed DataFrame writes, detects serialization bottlenecks, and provides a
resumable concurrent multipart uploader when the destination must be one S3
object. It can also compose ordered staging objects entirely inside S3, so
payload bytes do not make a wasteful round trip through the Databricks driver.

## Project status

Dude is an open-source alpha under Apache-2.0. It is a working engineering
prototype, not yet a proven replacement for production transfer systems. Do not
publish performance claims until they are reproduced with the benchmark harness
and workload details in [`benchmarks/`](benchmarks/).

The command remains `dude`; the Python import is `dude_transfer` to avoid a
collision with an unrelated package already using the `dude` import namespace.

## Why

A multi-gigabyte transfer should not silently run through one driver thread.
Dude separates the transfer shapes that are often accidentally mixed together:

- **Spark datasets** are written directly by executors as multiple Parquet,
  Delta, JSON, or CSV objects.
- **Single files** are split into checksummed S3 multipart uploads with bounded
  memory, adaptive concurrency, retries, durable checkpoints, and independent
  completion verification.
- **Concatenable S3 chunks** are assembled with concurrent server-side multipart
  copies, source-version pinning, resume, and no payload download to the driver.

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

### Produce one JSON Lines object without serializing Spark

When a downstream system requires one object rather than a Spark dataset, let
executors write JSON Lines chunks in parallel and have S3 assemble them:

```python
from dude_transfer import ComposeConfig, send_dataframe_as_single_object

result = send_dataframe_as_single_object(
    df,
    "s3://my-lab-data/exports/run-42.jsonl",
    target_file_size="256MiB",
    compose_config=ComposeConfig(
        initial_concurrency=4,
        max_concurrency=16,
        expected_source_bucket_owner="123456789012",
    ),
    create_args={"ExpectedBucketOwner": "123456789012"},
)
```

This path supports uncompressed UTF-8 JSON Lines and text only. It writes to a
random staging prefix, orders Spark `part-*` files by partition and chunk number,
verifies the completed destination, and then deletes only that run's staging
objects. Set `cleanup_staging=False` to retain them. A failed write or compose
keeps staging for diagnosis and retry; a cleanup failure is reported in result
metadata without hiding a successfully committed destination.

Spark DataFrames have no implicit global row order. Dude preserves Spark's
partition/chunk order but does not invent a row-order guarantee. Each nonfinal
payload object must meet S3's 5 MiB multipart minimum; if skew creates a smaller
object, Dude fails closed and retains the staging run. `mode="errorifexists"`
performs a destination preflight check, but S3 clients without conditional
multipart completion cannot make that check atomic against another writer.

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

## Compose staged S3 chunks without downloading them

If executors have already produced ordered chunks of a byte-concatenable format,
Dude can make one S3 object without downloading and re-uploading those bytes:

```python
from dude_transfer import ComposeConfig, compose_s3_objects

result = compose_s3_objects(
    [
        "s3://my-lab-data/stage/run-42/0000.jsonl",
        "s3://my-lab-data/stage/run-42/0001.jsonl",
        "s3://my-lab-data/stage/run-42/0002.jsonl",
    ],
    "s3://my-lab-data/final/run-42.jsonl",
    config=ComposeConfig(
        copy_part_size="512MiB",
        initial_concurrency=4,
        max_concurrency=16,
        resume=True,
        verify_after_compose=True,
        expected_source_bucket_owner="123456789012",
    ),
    create_args={
        "ExpectedBucketOwner": "123456789012",
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "alias/lab-data",
    },
)
```

The source order is the byte order in the destination. Dude inspects every
source, pins its ETag and version ID when available, splits large sources into
legal copy ranges, and rejects a plan if any nonfinal output part would be under
S3's 5 MiB minimum. Sources are never modified or deleted.

This is correct for JSON Lines, text, and binary streams that were deliberately
split on safe boundaries. It does **not** turn independent Parquet files into one
valid Parquet file; keep those as a Spark dataset. Likewise, format-level headers,
trailers, delimiters, and compression semantics remain the caller's responsibility.

The runtime role needs `s3:GetObject` on sources plus the destination multipart
permissions (`s3:PutObject`, `s3:ListMultipartUploadParts`, and
`s3:AbortMultipartUpload`). KMS-encrypted sources and destinations also require
the corresponding KMS permissions. Configure an S3 lifecycle rule to expire
abandoned multipart uploads as a final safety net.

The high-level DataFrame single-object path additionally needs `s3:ListBucket`
on its staging prefix and `s3:DeleteObject` when automatic staging cleanup is
enabled. It never deletes user-supplied composition sources.

Spark and the boto3 control-plane client must both be authorized for the staging
location. A Unity Catalog external location can authorize the Spark write without
automatically supplying credentials to an arbitrary boto3 client; use an approved
compute role or pass a configured S3 client when those trust paths differ.

### CLI

```bash
dude plan-file /local_disk0/diagnostics.bin s3://bucket/diagnostics.bin
dude send-file /local_disk0/diagnostics.bin s3://bucket/diagnostics.bin \
  --part-size 64MiB --initial-concurrency 2 --concurrency 8 \
  --max-memory 512MiB --verify --json-events
dude plan-compose s3://bucket/final/run.jsonl \
  s3://bucket/stage/0000.jsonl s3://bucket/stage/0001.jsonl
dude compose-s3 s3://bucket/final/run.jsonl \
  s3://bucket/stage/0000.jsonl s3://bucket/stage/0001.jsonl \
  --initial-concurrency 4 --concurrency 16 --verify --json-events
```

## Current scope

Version 0.1 deliberately focuses on three critical paths. Its first adaptive
controller reacts to measured part throughput and retries, but does not yet use
cluster CPU, NIC, or S3 request metrics. It does not calculate exact DataFrame
output sizes, compact arbitrary existing S3 small files, or distribute ranges of
one local file across Spark executors. Server-side composition requires sources
that are already ordered and byte-concatenable.

## Contributing and security

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development and benchmark rules,
[`SECURITY.md`](SECURITY.md) for data-handling and vulnerability guidance, and
[`docs/ROADMAP.md`](docs/ROADMAP.md) for the path from useful prototype to a
distinctive adaptive transfer engine.

Licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
