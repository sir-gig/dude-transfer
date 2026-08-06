# Databricks notebook source
"""Compare serialized Spark JSONL output with Dude's parallel single-object path.

All destinations are unique and retained. Use synthetic data only.
"""

# COMMAND ----------

import json
import time
import uuid

from dude_transfer import ComposeConfig, diagnose_dataframe, send_dataframe_as_single_object

# COMMAND ----------

dbutils.widgets.text("source_table", "samples.wanderbricks.reviews")
dbutils.widgets.text("destination", "s3://replace-me/dude-single-object-benchmark")
dbutils.widgets.text("target_file_size", "256MiB")
dbutils.widgets.text("expected_bucket_owner", "")
dbutils.widgets.text("repetitions", "3")
dbutils.widgets.text("max_skew_retries", "2")

source_table = dbutils.widgets.get("source_table")
destination = dbutils.widgets.get("destination").rstrip("/")
target_file_size = dbutils.widgets.get("target_file_size")
expected_bucket_owner = dbutils.widgets.get("expected_bucket_owner").strip() or None
repetitions = int(dbutils.widgets.get("repetitions"))
max_skew_retries = int(dbutils.widgets.get("max_skew_retries"))

if destination.startswith("s3://replace-me/"):
    raise ValueError("set the destination widget to an authorized benchmark prefix")
if repetitions < 1:
    raise ValueError("repetitions must be positive")
if max_skew_retries < 0:
    raise ValueError("max_skew_retries must be nonnegative")

run_id = uuid.uuid4().hex
df = spark.table(source_table)

# COMMAND ----------


def storage_stats(path):
    pending = [path]
    files = 0
    payload_files = 0
    size_bytes = 0
    while pending:
        current = pending.pop()
        for item in dbutils.fs.ls(current):
            if item.isDir():
                pending.append(item.path)
            else:
                files += 1
                size_bytes += item.size
                if item.name.startswith("part-") and item.size:
                    payload_files += 1
    return {"files": files, "payload_files": payload_files, "size_bytes": size_bytes}


def serialized_write(repetition):
    output = f"{destination}/run={run_id}/rep={repetition}/serialized-jsonl"
    started = time.monotonic()
    df.coalesce(1).write.format("json").mode("errorifexists").save(output)
    elapsed = time.monotonic() - started
    stats = storage_stats(output)
    if stats["payload_files"] != 1:
        raise RuntimeError("serialized baseline did not produce exactly one payload object")
    return {
        "engine": "spark-coalesce-1",
        "destination": output,
        "elapsed_seconds": elapsed,
        "throughput_mib_s": stats["size_bytes"] / 1024**2 / elapsed,
        **stats,
    }


def dude_write(repetition):
    output = f"{destination}/run={run_id}/rep={repetition}/dude.jsonl"
    create_args = {}
    if expected_bucket_owner:
        create_args["ExpectedBucketOwner"] = expected_bucket_owner
    started = time.monotonic()
    result = send_dataframe_as_single_object(
        df,
        output,
        target_file_size=target_file_size,
        max_skew_retries=max_skew_retries,
        compose_config=ComposeConfig(
            initial_concurrency=4,
            max_concurrency=16,
            expected_source_bucket_owner=expected_bucket_owner,
        ),
        create_args=create_args,
    )
    elapsed = time.monotonic() - started
    return {
        "engine": "dude-parallel-compose",
        "destination": output,
        "elapsed_seconds": elapsed,
        "throughput_mib_s": result.bytes_transferred / 1024**2 / elapsed,
        "size_bytes": result.bytes_transferred,
        "parts": result.parts,
        "staging_cleanup": result.metadata["staging_cleanup"],
        "result": result.as_dict(),
    }


# COMMAND ----------

results = []
for repetition in range(1, repetitions + 1):
    if repetition % 2:
        results.append(serialized_write(repetition))
        results.append(dude_write(repetition))
    else:
        results.append(dude_write(repetition))
        results.append(serialized_write(repetition))

report = {
    "run_id": run_id,
    "source_table": source_table,
    "target_file_size": target_file_size,
    "max_skew_retries": max_skew_retries,
    "diagnostics": [item.as_dict() for item in diagnose_dataframe(df)],
    "results": results,
    "notes": [
        "Execution order alternates between repetitions.",
        "The baseline intentionally serializes Spark with coalesce(1).",
        "Dude writes executor chunks in parallel and composes inside S3.",
        "All output is retained except Dude-owned successful staging runs.",
        "Publish complete raw JSON plus cluster, runtime, region, and endpoint details.",
    ],
}

print(json.dumps(report, indent=2, sort_keys=True))
