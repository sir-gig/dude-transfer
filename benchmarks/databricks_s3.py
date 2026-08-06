# Databricks notebook source
"""Reproducible native-vs-Dude DataFrame write benchmark.

Upload this file as a Databricks notebook after installing Dude. The notebook
creates new objects under the supplied destination and never deletes data.
"""

# COMMAND ----------

import json
import time
import uuid

from dude_transfer import diagnose_dataframe, send_dataframe

# COMMAND ----------

dbutils.widgets.text("source_table", "samples.wanderbricks.reviews")
dbutils.widgets.text("destination", "s3://replace-me/dude-benchmark")
dbutils.widgets.text("target_file_size", "256MiB")
dbutils.widgets.dropdown("run_native", "true", ["true", "false"])

source_table = dbutils.widgets.get("source_table")
destination = dbutils.widgets.get("destination").rstrip("/")
target_file_size = dbutils.widgets.get("target_file_size")
run_native = dbutils.widgets.get("run_native") == "true"

if destination.startswith("s3://replace-me/"):
    raise ValueError("set the destination widget to an authorized benchmark prefix")

run_id = uuid.uuid4().hex
native_destination = f"{destination}/run={run_id}/engine=native"
dude_destination = f"{destination}/run={run_id}/engine=dude"
df = spark.table(source_table)

# COMMAND ----------


def storage_stats(path):
    pending = [path]
    files = 0
    size_bytes = 0
    while pending:
        current = pending.pop()
        for item in dbutils.fs.ls(current):
            if item.isDir():
                pending.append(item.path)
            else:
                files += 1
                size_bytes += item.size
    return {"files": files, "size_bytes": size_bytes}


def native_write():
    started = time.monotonic()
    (
        df.write.format("parquet")
        .mode("errorifexists")
        .option("compression", "snappy")
        .save(native_destination)
    )
    elapsed = time.monotonic() - started
    stats = storage_stats(native_destination)
    return {
        "engine": "native",
        "destination": native_destination,
        "elapsed_seconds": elapsed,
        "throughput_mib_s": stats["size_bytes"] / 1024**2 / elapsed,
        **stats,
    }


def dude_write():
    result = send_dataframe(
        df,
        dude_destination,
        file_format="parquet",
        compression="snappy",
        target_file_size=target_file_size,
        mode="errorifexists",
    )
    stats = storage_stats(dude_destination)
    return {
        "engine": "dude",
        "destination": dude_destination,
        "elapsed_seconds": result.elapsed_seconds,
        "throughput_mib_s": stats["size_bytes"] / 1024**2 / result.elapsed_seconds,
        "plan": result.metadata["plan"],
        **stats,
    }


# COMMAND ----------

report = {
    "run_id": run_id,
    "source_table": source_table,
    "diagnostics": [item.as_dict() for item in diagnose_dataframe(df)],
    "results": [],
    "notes": [
        "This is an end-to-end write benchmark; source computation is included.",
        "Native runs before Dude, so repeat runs and alternate order before publishing claims.",
        "Benchmark output is retained and must be removed according to your storage policy.",
    ],
}

if run_native:
    report["results"].append(native_write())
report["results"].append(dude_write())

print(json.dumps(report, indent=2, sort_keys=True))
