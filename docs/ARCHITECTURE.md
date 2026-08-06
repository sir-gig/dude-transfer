# Architecture

Dude is a transfer planner, not a replacement for Spark or Amazon S3.

## DataFrame path

```text
Spark DataFrame
      |
      +-- inspect logical byte statistics and partition count
      +-- diagnose single-partition and explicit-collapse plans
      +-- preserve, repartition, or coalesce
      `-- DataFrameWriter -> concurrent executor writes -> S3 prefix
```

No row collection or count action is used during planning. Spark's logical byte
statistics are advisory; compressed output can differ substantially.

## Local-file path

```text
local or Volume file
      |
      +-- plan part size, count, concurrency, and buffer ceiling
      +-- reconcile checkpoint with S3 ListParts
      +-- upload missing checksummed parts with adaptive concurrency
      `-- atomically CompleteMultipartUpload
```

Each worker holds at most one part in memory. The configured in-flight byte
ceiling limits effective concurrency. Failed resumable transfers retain their
remote parts and checkpoint. Non-resumable failures abort the remote upload.

## S3 composition path

```text
ordered S3 staging objects
      |
      +-- HeadObject sources concurrently; pin ETags and version IDs
      +-- plan legal 5 MiB..5 GiB copy parts in source order
      +-- reconcile checkpoint with destination ListParts
      +-- UploadPartCopy missing ranges concurrently inside S3
      `-- CompleteMultipartUpload -> one destination object
```

Only control requests and metadata pass through the client; payload bytes remain
on S3's data plane. Source objects are preserved. The operation is byte
concatenation, so it is valid only when the staged format is itself safely
concatenable. Independent Parquet objects are not a valid input for this path.

## DataFrame-to-one-object orchestration

```text
Spark DataFrame
      |
      +-- executors write uncompressed JSONL/text to a random staging prefix
      +-- list committed part objects and sort numeric partition/chunk IDs
      +-- measure S3 part sizes; reshuffle skewed output with lower parallelism
      +-- run the verified S3 composition path
      `-- optionally delete only the owned staging-run keys
```

The orchestrator never collects rows and never transfers payload bytes through
the client. An undersized nonfinal object triggers a bounded sequence of forced
Spark shuffles chosen from actual staged bytes. A serial repair is allowed only
when the complete output is no larger than the configured target object size.
Earlier invalid attempt prefixes are cleaned before the next write when cleanup
is enabled; the final failed prefix is retained. Cleanup results across every
attempt remain visible, including orphaned prefixes when deletion is denied.
Failed staging or composition leaves its current run prefix intact. Final cleanup
is attempted only after destination verification; cleanup errors are returned as
metadata because the destination has already committed successfully.

The adaptive controller begins conservatively, probes upward when aggregate
throughput improves, and applies multiplicative backoff when a part requires a
retry. Every adjustment is returned as structured result metadata and can be
streamed as a `TransferEvent`.

Each newly created object carries a random `dude-transfer-id` metadata value.
That identity lets Dude distinguish a successfully committed object from an old
same-sized object if the completion response is lost. Remote parts are reusable
only when their byte length and configured checksum metadata match the plan.
Create-only and compare-and-swap modes are enforced on the final S3 write with
`If-None-Match` or `If-Match`; a preflight read is never treated as a lock.

## Trust boundaries

Dude delegates authentication to Spark, Unity Catalog, or boto3's credential
chain. Checkpoints are local metadata and can expose source paths, S3 object
names, ETags, version IDs, and multipart upload IDs. Protect them like
operational logs.
