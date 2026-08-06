# Databricks notebook source
"""Exercise measured skew repair with deterministic synthetic JSONL data.

The first range partition is intentionally smaller than S3's 5 MiB nonfinal
multipart minimum. Dude must discard that layout, force a balanced shuffle, and
compose the replacement without downloading payload bytes through the driver.
"""

# COMMAND ----------

import json
import time
import uuid

from pyspark import StorageLevel
from pyspark.sql import functions as F

from dude_transfer import ComposeConfig, send_dataframe_as_single_object

# COMMAND ----------

dbutils.widgets.text("destination", "s3://replace-me/dude-skew-repair-benchmark")
dbutils.widgets.text("partitions", "8")
dbutils.widgets.text("total_mib", "512")
dbutils.widgets.text("tiny_partition_mib", "1")
dbutils.widgets.text("payload_bytes", "1024")
dbutils.widgets.text("target_file_size", "64MiB")
dbutils.widgets.text("max_skew_retries", "2")
dbutils.widgets.text("expected_bucket_owner", "")

destination = dbutils.widgets.get("destination").rstrip("/")
partition_count = int(dbutils.widgets.get("partitions"))
total_mib = int(dbutils.widgets.get("total_mib"))
tiny_partition_mib = int(dbutils.widgets.get("tiny_partition_mib"))
payload_bytes = int(dbutils.widgets.get("payload_bytes"))
target_file_size = dbutils.widgets.get("target_file_size")
max_skew_retries = int(dbutils.widgets.get("max_skew_retries"))
expected_bucket_owner = dbutils.widgets.get("expected_bucket_owner").strip() or None

if destination.startswith("s3://replace-me/"):
    raise ValueError("set destination to an authorized synthetic benchmark prefix")
if partition_count < 2:
    raise ValueError("partitions must be at least two")
if not 0 < tiny_partition_mib < 5:
    raise ValueError("tiny_partition_mib must be between one and four")
if total_mib <= tiny_partition_mib + 5 * (partition_count - 1):
    raise ValueError("total_mib must leave at least 5 MiB for every large partition")
if payload_bytes < 64:
    raise ValueError("payload_bytes must be at least 64")
if max_skew_retries < 1:
    raise ValueError("this benchmark requires at least one skew-repair retry")

# COMMAND ----------

mib = 1024**2
total_rows = total_mib * mib // payload_bytes
tiny_rows = tiny_partition_mib * mib // payload_bytes

raw = (
    spark.range(total_rows)
    .withColumn(
        "__dude_partition",
        F.when(F.col("id") < tiny_rows, F.lit(0)).otherwise(
            F.lit(1)
            + F.pmod(
                F.col("id") - F.lit(tiny_rows),
                F.lit(partition_count - 1),
            )
        ),
    )
    .withColumn("payload", F.repeat(F.lit("x"), payload_bytes))
)
df = (
    raw.repartitionByRange(partition_count, "__dude_partition")
    .drop("__dude_partition")
    .persist(StorageLevel.DISK_ONLY)
)

# Materialize the repeatable fixture and collect only one row count per partition.
partition_rows = df.rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)]).collect()

# COMMAND ----------

run_id = uuid.uuid4().hex
output = f"{destination}/run={run_id}/repaired.jsonl"
create_args = {}
if expected_bucket_owner:
    create_args["ExpectedBucketOwner"] = expected_bucket_owner

started = time.monotonic()
try:
    result = send_dataframe_as_single_object(
        df,
        output,
        target_file_size=target_file_size,
        partitions=partition_count,
        max_skew_retries=max_skew_retries,
        compose_config=ComposeConfig(
            initial_concurrency=4,
            max_concurrency=16,
            expected_source_bucket_owner=expected_bucket_owner,
        ),
        create_args=create_args,
    )
finally:
    df.unpersist()
elapsed = time.monotonic() - started

if not result.metadata["skew_repair"]["repaired"]:
    raise RuntimeError(
        "the synthetic layout did not trigger repair; inspect partition_rows and "
        "increase total_mib or reduce tiny_partition_mib"
    )
if result.metadata["payload_bytes_downloaded_by_client"] != 0:
    raise RuntimeError("skew repair unexpectedly downloaded payload through the client")

report = {
    "run_id": run_id,
    "destination": output,
    "elapsed_seconds": elapsed,
    "throughput_mib_s": result.bytes_transferred / mib / elapsed,
    "fixture": {
        "partitions": partition_count,
        "partition_rows": partition_rows,
        "total_mib": total_mib,
        "tiny_partition_mib": tiny_partition_mib,
        "payload_bytes": payload_bytes,
        "target_file_size": target_file_size,
        "max_skew_retries": max_skew_retries,
    },
    "result": result.as_dict(),
    "notes": [
        "The fixture is synthetic and persisted on executor disks before timing.",
        "The timed result includes the rejected staging write and repair rewrite.",
        "Publish every staging attempt and the full cluster/runtime configuration.",
    ],
}

print(json.dumps(report, indent=2, sort_keys=True))
