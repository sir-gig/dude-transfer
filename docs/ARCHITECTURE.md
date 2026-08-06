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
      ├─ upload missing checksummed parts with adaptive concurrency
      └─ atomically CompleteMultipartUpload
```

Each worker holds at most one part in memory. The configured in-flight byte
ceiling limits effective concurrency. Failed resumable transfers retain their
remote parts and checkpoint. Non-resumable failures abort the remote upload.

The adaptive controller begins conservatively, probes upward when aggregate
throughput improves, and applies multiplicative backoff when a part requires a
retry. Concurrency never exceeds the configured byte ceiling. Every adjustment
is returned as structured result metadata and can be streamed as a `TransferEvent`.

Each newly created object carries a random `dude-transfer-id` metadata value.
That identity lets Dude distinguish a successfully committed object from an old
same-sized object if the completion response is lost. Remote parts are reusable
only when their byte length and configured checksum metadata match the plan.

## Trust boundaries

Dude delegates authentication to Spark, Unity Catalog, or boto3's credential
chain. Checkpoints are local metadata and can expose source paths and S3 object
names. They should be protected like operational logs.
