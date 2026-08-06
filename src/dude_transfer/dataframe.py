"""Plan and execute distributed Spark DataFrame writes."""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from typing import Any

from .models import DataFramePlan, Diagnostic, TransferResult
from .utils import MIB, parse_size

_UNKNOWN_SPARK_SIZE = (1 << 63) - 1


def _estimated_size(df: Any) -> int | None:
    try:
        value = int(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return None
    if value <= 0 or value >= _UNKNOWN_SPARK_SIZE:
        return None
    return value


def _partition_count(df: Any) -> int | None:
    try:
        return int(df.rdd.getNumPartitions())
    except (AttributeError, TypeError, ValueError, RuntimeError):
        try:
            return int(df._jdf.rdd().getNumPartitions())
        except (AttributeError, TypeError, ValueError, RuntimeError):
            return None


def _plan_text(df: Any) -> str:
    try:
        return str(df._jdf.queryExecution().optimizedPlan().toString())
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return ""


def diagnose_dataframe(df: Any) -> tuple[Diagnostic, ...]:
    """Inspect a DataFrame without running a Spark action."""

    diagnostics: list[Diagnostic] = []
    partitions = _partition_count(df)
    plan = _plan_text(df).lower()

    if partitions == 1:
        diagnostics.append(
            Diagnostic(
                "critical",
                "single_partition",
                "The DataFrame has one partition, so only one Spark task can write it.",
            )
        )
    elif partitions is None:
        diagnostics.append(
            Diagnostic(
                "info",
                "partition_count_unavailable",
                "The partition count is unavailable in this compute mode; "
                "Dude will use plan stats.",
            )
        )

    if "coalesce 1" in plan or "repartition 1" in plan:
        diagnostics.append(
            Diagnostic(
                "critical",
                "serialized_plan",
                "The optimized plan explicitly collapses the data to one partition.",
            )
        )
    if _estimated_size(df) is None:
        diagnostics.append(
            Diagnostic(
                "info",
                "size_estimate_unavailable",
                "Spark has no reliable byte estimate; partition sizing will be conservative.",
            )
        )
    return tuple(diagnostics)


def plan_dataframe(
    df: Any,
    destination: str,
    *,
    file_format: str = "parquet",
    target_file_size: int | str = 256 * MIB,
    min_partitions: int = 4,
    max_partitions: int = 4096,
    partitions: int | None = None,
) -> DataFramePlan:
    """Choose a write shape using Spark metadata, without evaluating the DataFrame."""

    target_bytes = parse_size(target_file_size)
    if min_partitions < 1 or max_partitions < min_partitions:
        raise ValueError("partition bounds are invalid")
    if partitions is not None and partitions < 1:
        raise ValueError("partitions must be greater than zero")

    before = _partition_count(df)
    estimated = _estimated_size(df)
    diagnostics = list(diagnose_dataframe(df))

    if partitions is not None:
        after = partitions
    elif estimated is not None:
        after = math.ceil(estimated / target_bytes)
        after = max(min_partitions, min(max_partitions, after))
    elif before is not None:
        after = max(min_partitions, min(max_partitions, before))
    else:
        after = None

    if partitions is not None:
        action = "preserve" if before == after else "repartition"
    elif before is None:
        action = "repartition" if after is not None else "preserve"
    elif after > before:
        action = "repartition"
    elif after * 2 < before:
        action = "coalesce"
    else:
        action = "preserve"
        after = before

    if estimated is not None:
        diagnostics.append(
            Diagnostic(
                "info",
                "logical_size_estimate",
                "Partition sizing uses Spark logical statistics; compressed output can differ.",
            )
        )

    return DataFramePlan(
        destination=destination,
        file_format=file_format.lower(),
        target_file_size=target_bytes,
        estimated_bytes=estimated,
        partitions_before=before,
        partitions_after=after,
        action=action,
        diagnostics=tuple(diagnostics),
    )


def send_dataframe(
    df: Any,
    destination: str,
    *,
    file_format: str = "parquet",
    mode: str = "errorifexists",
    compression: str | None = "snappy",
    target_file_size: int | str = 256 * MIB,
    min_partitions: int = 4,
    max_partitions: int = 4096,
    partitions: int | None = None,
    partition_by: str | Sequence[str] | None = None,
    options: Mapping[str, Any] | None = None,
) -> TransferResult:
    """Write a DataFrame directly from Spark executors to the destination."""

    plan = plan_dataframe(
        df,
        destination,
        file_format=file_format,
        target_file_size=target_file_size,
        min_partitions=min_partitions,
        max_partitions=max_partitions,
        partitions=partitions,
    )

    working = df
    if plan.action == "repartition" and plan.partitions_after is not None:
        working = df.repartition(plan.partitions_after)
    elif plan.action == "coalesce" and plan.partitions_after is not None:
        working = df.coalesce(plan.partitions_after)

    writer = working.write.format(plan.file_format).mode(mode)
    if compression and plan.file_format in {"parquet", "json", "csv", "orc", "text"}:
        writer = writer.option("compression", compression)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)
    if partition_by:
        columns = [partition_by] if isinstance(partition_by, str) else list(partition_by)
        writer = writer.partitionBy(*columns)

    started = time.monotonic()
    writer.save(destination)
    elapsed = time.monotonic() - started
    throughput = None
    if plan.estimated_bytes is not None and elapsed > 0:
        throughput = plan.estimated_bytes / MIB / elapsed

    return TransferResult(
        kind="dataframe",
        destination=destination,
        elapsed_seconds=elapsed,
        bytes_transferred=None,
        throughput_mib_s=throughput,
        parts=plan.partitions_after or 0,
        metadata={"plan": plan.as_dict(), "throughput_is_estimated": throughput is not None},
    )
