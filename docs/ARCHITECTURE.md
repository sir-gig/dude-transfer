# Architecture

Dude is a transfer planner, not a replacement for Spark or Amazon S3.

## DataFrame path

```text
Spark DataFrame
      │
      ├─ inspect logical byte statistics and partition count
      ├─ diagnose single-partition and explicit-collapse plans
      ├─ preserve, repartition, or coalesce
      └─ DataFrameWriter → concurrent executor writes → S3 prefix
```

No row collection or count action is used during planning. Spark's logical byte
statistics are advisory; compressed output can differ substantially.

## File path

```text
local or Volume file
      │
      ├─ plan part size, count, concurrency, and buffer ceiling
      ├─ reconcile checkpoint with S3 ListParts
      ├─ upload missing checksummed parts concurrently
      └─ atomically CompleteMultipartUpload
```

Each worker holds at most one part in memory. The configured in-flight byte
ceiling limits effective concurrency. Failed resumable transfers retain their
remote parts and checkpoint. Non-resumable failures abort the remote upload.

## Trust boundaries

Dude delegates authentication to Spark, Unity Catalog, or boto3's credential
chain. Checkpoints are local metadata and can expose source paths and S3 object
names. They should be protected like operational logs.
