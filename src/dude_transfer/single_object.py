"""Distributed Spark staging followed by S3-side single-object composition."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import quote

from .compose import ComposeConfig, _effective_config, compose_s3_objects
from .dataframe import send_dataframe
from .exceptions import ConfigurationError
from .models import TransferEvent, TransferResult
from .s3 import (
    S3_MIN_PART_SIZE,
    _call_with_retry,
    _conditional_write_args,
    _creation_args,
    _error_code,
    _get_client,
    _object_matches,
    _owner_args,
    _validated_create_args,
    _verify_object,
    parse_s3_uri,
)
from .utils import MIB, parse_size

_PARTITION_PATTERN = re.compile(r"^part-(\d+)(?:-|$)")
_CHUNK_PATTERN = re.compile(r"-c(\d+)(?:\.|$)")


@dataclass(frozen=True)
class _StageListing:
    sources: tuple[str, ...]
    keys: tuple[str, ...]
    payload_sizes: tuple[int, ...]
    size_bytes: int

    @property
    def undersized_nonfinal(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            (uri, size)
            for uri, size in zip(
                self.sources[:-1],
                self.payload_sizes[:-1],
                strict=True,
            )
            if size < S3_MIN_PART_SIZE
        )


def _stage_prefix(destination: str, staging_prefix: str | None, run_id: str) -> str:
    destination_bucket, destination_key = parse_s3_uri(destination)
    if staging_prefix:
        bucket, base_key = parse_s3_uri(staging_prefix)
    else:
        bucket = destination_bucket
        base_key = f"{destination_key}.dude-stage"
    key = f"{base_key.rstrip('/')}/{run_id}"
    return f"s3://{bucket}/{quote(key, safe='/')}/"


def _request_args(owner: str | None, request_payer: str | None) -> dict[str, str]:
    arguments: dict[str, str] = {}
    if owner:
        arguments["ExpectedBucketOwner"] = owner
    if request_payer:
        arguments["RequestPayer"] = request_payer
    return arguments


def _ensure_destination_mode(
    client: Any,
    destination: str,
    mode: str,
    request_args: Mapping[str, Any],
    max_attempts: int,
) -> None:
    normalized = mode.lower()
    if normalized not in {"error", "errorifexists", "overwrite"}:
        raise ConfigurationError("single-object mode must be errorifexists or overwrite")
    if normalized == "overwrite":
        return
    bucket, key = parse_s3_uri(destination)
    try:
        _call_with_retry(
            lambda: client.head_object(
                Bucket=bucket,
                Key=key,
                **dict(request_args),
            ),
            max_attempts,
        )
    except Exception as error:
        if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
            return
        raise
    raise FileExistsError(f"destination already exists: {destination}")


def _stage_sort_key(key: str) -> tuple[int, int, str]:
    name = key.rsplit("/", 1)[-1]
    partition = _PARTITION_PATTERN.match(name)
    if not partition:
        raise ConfigurationError(f"unexpected Spark output object: {key}")
    chunk = _CHUNK_PATTERN.search(name)
    return int(partition.group(1)), int(chunk.group(1)) if chunk else 0, key


def _inspect_stage(
    client: Any,
    stage_uri: str,
    request_args: Mapping[str, Any],
    max_attempts: int,
) -> _StageListing:
    bucket, prefix = parse_s3_uri(stage_uri)
    all_keys: list[str] = []
    payloads: list[tuple[str, int]] = []
    token: str | None = None
    while True:
        arguments: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            **dict(request_args),
        }
        if token:
            arguments["ContinuationToken"] = token
        response = _call_with_retry(
            lambda arguments=arguments: client.list_objects_v2(**arguments),
            max_attempts,
        ).response
        for item in response.get("Contents", []):
            key = str(item["Key"])
            size = int(item.get("Size", 0))
            all_keys.append(key)
            name = key.rsplit("/", 1)[-1]
            if name.startswith("part-"):
                if size:
                    payloads.append((key, size))
            elif size and not name.startswith(("_", ".")):
                raise ConfigurationError(f"unexpected nonempty staging object: {key}")
        if not response.get("IsTruncated"):
            break
        token = str(response["NextContinuationToken"])

    payloads.sort(key=lambda item: _stage_sort_key(item[0]))
    return _StageListing(
        sources=tuple(
            f"s3://{bucket}/{quote(key, safe='/')}" for key, _ in payloads
        ),
        keys=tuple(all_keys),
        payload_sizes=tuple(size for _, size in payloads),
        size_bytes=sum(size for _, size in payloads),
    )


def _list_stage(
    client: Any,
    stage_uri: str,
    request_args: Mapping[str, Any],
    max_attempts: int,
) -> tuple[list[str], list[str], int]:
    """Return the historical tuple view used by callers of this internal helper."""

    listing = _inspect_stage(client, stage_uri, request_args, max_attempts)
    return list(listing.sources), list(listing.keys), listing.size_bytes


def _repair_partition_count(
    listing: _StageListing,
    staged: TransferResult,
    previous_repair_partitions: int | None,
    target_file_size: int,
) -> int | None:
    """Choose a smaller, shuffled layout from measured output sizes.

    The first repair may keep the observed payload count because the forced
    Spark shuffle itself removes inherited partition skew. Later repairs halve
    the count. A one-partition fallback is permitted only when the complete
    payload already fits within the requested target object size.
    """

    observed = len(listing.sources)
    if observed <= 1:
        return None
    planned = staged.parts if staged.parts > 0 else observed
    current = max(1, min(planned, observed))
    maximum_by_measured_bytes = max(
        1,
        listing.size_bytes // (2 * S3_MIN_PART_SIZE),
    )
    if previous_repair_partitions is None:
        candidate = min(current, maximum_by_measured_bytes)
    else:
        if previous_repair_partitions <= 1:
            return None
        candidate = min(
            max(1, previous_repair_partitions // 2),
            maximum_by_measured_bytes,
        )
    if candidate == 1 and listing.size_bytes > target_file_size:
        return None
    return candidate


def _aggregate_cleanup(
    requested: bool,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not requested:
        return {
            "requested": False,
            "completed": False,
            "deleted_objects": 0,
            "attempts": [],
        }
    result: dict[str, Any] = {
        "requested": True,
        "completed": bool(records) and all(item["completed"] for item in records),
        "deleted_objects": sum(int(item["deleted_objects"]) for item in records),
        "attempts": records,
    }
    failed = [item for item in records if not item["completed"]]
    if len(records) == 1 and failed:
        if "error" in failed[0]:
            result["error"] = failed[0]["error"]
        if "errors" in failed[0]:
            result["errors"] = failed[0]["errors"]
    elif failed:
        result["errors"] = [
            {
                "staging_prefix": item["staging_prefix"],
                "error": item.get("error", item.get("errors")),
            }
            for item in failed
        ]
    return result


def _cleanup_stage(
    client: Any,
    stage_uri: str,
    keys: list[str],
    request_args: Mapping[str, Any],
    max_attempts: int,
) -> dict[str, Any]:
    bucket, prefix = parse_s3_uri(stage_uri)
    if any(not key.startswith(prefix) for key in keys):
        raise ConfigurationError("refusing to clean objects outside Dude's staging run")
    deleted = 0
    errors: list[dict[str, Any]] = []
    try:
        for offset in range(0, len(keys), 1000):
            batch = keys[offset : offset + 1000]
            response = _call_with_retry(
                lambda batch=batch: client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
                    **dict(request_args),
                ),
                max_attempts,
            ).response
            errors.extend(response.get("Errors", []))
            deleted += len(batch) - len(response.get("Errors", []))
    except Exception as error:
        return {
            "requested": True,
            "completed": False,
            "deleted_objects": deleted,
            "error": f"{type(error).__name__}: {error}",
        }
    return {
        "requested": True,
        "completed": not errors,
        "deleted_objects": deleted,
        "errors": errors,
    }


def _write_empty_object(
    client: Any,
    destination: str,
    config: ComposeConfig,
    create_args: Mapping[str, Any],
) -> tuple[str, bool]:
    bucket, key = parse_s3_uri(destination)
    transfer_id = uuid.uuid4().hex
    owner_args = _owner_args(create_args)

    def put() -> dict[str, Any]:
        arguments = _creation_args(
            bucket,
            key,
            config.checksum_algorithm,
            create_args,
            transfer_id,
        )
        arguments.update(
            _conditional_write_args(config.if_none_match, config.if_match)
        )
        arguments["Body"] = b""
        return client.put_object(**arguments)

    recovered = False
    try:
        _call_with_retry(put, config.max_attempts)
    except Exception:
        recovered = _object_matches(
            client,
            bucket,
            key,
            0,
            transfer_id,
            owner_args,
            config.max_attempts,
        )
        if not recovered:
            raise
    if config.verify_after_compose:
        _verify_object(
            client,
            bucket,
            key,
            0,
            transfer_id,
            owner_args,
            config.max_attempts,
        )
    return transfer_id, recovered


def send_dataframe_as_single_object(
    df: Any,
    destination: str,
    *,
    file_format: str = "json",
    mode: str = "errorifexists",
    target_file_size: int | str = 256 * MIB,
    min_partitions: int = 1,
    max_partitions: int = 4096,
    partitions: int | None = None,
    options: Mapping[str, Any] | None = None,
    staging_prefix: str | None = None,
    cleanup_staging: bool = True,
    max_skew_retries: int = 2,
    compose_config: ComposeConfig | None = None,
    client: Any | None = None,
    create_args: Mapping[str, Any] | None = None,
    progress: Callable[[int, int], None] | None = None,
    events: Callable[[TransferEvent], None] | None = None,
) -> TransferResult:
    """Write a DataFrame in parallel, then compose one uncompressed S3 object.

    JSON output is newline-delimited JSON. Text output uses Spark's one-string-
    column text data source. DataFrames have no implicit global row ordering;
    Dude preserves Spark partition and chunk order in the resulting byte stream.
    When measured staging objects violate S3's nonfinal 5 MiB minimum, Dude can
    force up to ``max_skew_retries`` progressively smaller Spark shuffles.
    """

    normalized_format = file_format.lower()
    if normalized_format not in {"json", "text"}:
        raise ConfigurationError("single-object DataFrames support only json or text")
    target_bytes = parse_size(target_file_size)
    if target_bytes < S3_MIN_PART_SIZE:
        raise ConfigurationError("target_file_size must be at least 5 MiB for S3 composition")
    if type(max_skew_retries) is not int or max_skew_retries < 0:
        raise ConfigurationError("max_skew_retries must be a nonnegative integer")

    output_options = dict(options or {})
    normalized_options = {str(key).lower(): value for key, value in output_options.items()}
    compression = str(normalized_options.get("compression", "none")).lower()
    if compression not in {"none", "uncompressed"}:
        raise ConfigurationError("single-object DataFrame output must be uncompressed")
    line_separator = normalized_options.get("linesep", "\n")
    if line_separator != "\n":
        raise ConfigurationError("single-object DataFrame output requires a newline separator")
    encoding = str(normalized_options.get("encoding", "utf-8")).replace("_", "-").lower()
    if encoding not in {"utf-8", "utf8"}:
        raise ConfigurationError("single-object DataFrame output requires UTF-8")
    if "maxrecordsperfile" in normalized_options:
        raise ConfigurationError(
            "single-object DataFrame output cannot use maxRecordsPerFile because it "
            "can create undersized chunks after skew repair"
        )
    for key in list(output_options):
        if str(key).lower() in {"compression", "linesep", "encoding"}:
            output_options.pop(key)
    output_options["lineSep"] = "\n"
    if normalized_format == "json":
        output_options["encoding"] = "UTF-8"

    cfg = _effective_config(compose_config or ComposeConfig())
    normalized_mode = mode.lower()
    if normalized_mode in {"error", "errorifexists"}:
        if cfg.if_match is not None:
            raise ConfigurationError("errorifexists cannot be combined with if_match")
        if not cfg.if_none_match:
            cfg = replace(cfg, if_none_match=True)
    if cleanup_staging and not cfg.verify_after_compose:
        cfg = replace(cfg, verify_after_compose=True)
    extra = _validated_create_args(create_args)
    s3 = _get_client(client)
    destination_owner_args = _owner_args(extra)
    _ensure_destination_mode(
        s3,
        destination,
        mode,
        destination_owner_args,
        cfg.max_attempts,
    )

    run_id = uuid.uuid4().hex
    first_stage_uri = _stage_prefix(
        destination,
        staging_prefix,
        f"{run_id}/attempt-00",
    )
    stage_bucket, _ = parse_s3_uri(first_stage_uri)
    destination_bucket, _ = parse_s3_uri(destination)
    if (
        stage_bucket == destination_bucket
        and cfg.expected_source_bucket_owner is None
        and extra.get("ExpectedBucketOwner")
    ):
        cfg = replace(
            cfg,
            expected_source_bucket_owner=str(extra["ExpectedBucketOwner"]),
        )
    stage_request_args = _request_args(
        cfg.expected_source_bucket_owner,
        cfg.request_payer,
    )
    started = time.monotonic()

    def emit(phase: str, done: int, total: int, detail: str | None = None) -> None:
        if events:
            events(
                TransferEvent(
                    phase=phase,
                    elapsed_seconds=time.monotonic() - started,
                    bytes_transferred=done,
                    total_bytes=total,
                    detail=detail,
                )
            )

    staging_attempts: list[dict[str, Any]] = []
    cleanup_records: list[dict[str, Any]] = []
    repair_rewrites = 0
    previous_repair_partitions: int | None = None
    requested_partitions = partitions
    working_df = df

    while True:
        stage_uri = _stage_prefix(
            destination,
            staging_prefix,
            f"{run_id}/attempt-{len(staging_attempts):02d}",
        )
        emit("dataframe_staging_started", 0, 0, stage_uri)
        try:
            staged = send_dataframe(
                working_df,
                stage_uri,
                file_format=normalized_format,
                mode="errorifexists",
                compression=None,
                target_file_size=target_bytes,
                min_partitions=min_partitions,
                max_partitions=max_partitions,
                partitions=requested_partitions,
                options=output_options,
            )
            listing = _inspect_stage(
                s3,
                stage_uri,
                stage_request_args,
                cfg.max_attempts,
            )
        except BaseException as error:
            emit(
                "dataframe_staging_failed",
                0,
                0,
                f"{stage_uri}; {type(error).__name__}",
            )
            raise

        undersized = listing.undersized_nonfinal
        attempt_record: dict[str, Any] = {
            "attempt": len(staging_attempts),
            "staging_prefix": stage_uri,
            "requested_partitions": requested_partitions,
            "payload_objects": len(listing.sources),
            "staging_objects": len(listing.keys),
            "size_bytes": listing.size_bytes,
            "undersized_nonfinal_objects": [
                {"uri": uri, "size_bytes": size} for uri, size in undersized
            ],
            "valid_for_compose": not undersized,
            "staging_write": staged.as_dict(),
        }
        staging_attempts.append(attempt_record)
        emit(
            "dataframe_staging_completed",
            listing.size_bytes,
            listing.size_bytes,
            f"attempt={attempt_record['attempt']}; payload_objects={len(listing.sources)}",
        )
        if not undersized:
            break

        emit(
            "dataframe_staging_skew_detected",
            listing.size_bytes,
            listing.size_bytes,
            f"attempt={attempt_record['attempt']}; undersized_nonfinal={len(undersized)}",
        )
        next_partitions = _repair_partition_count(
            listing,
            staged,
            previous_repair_partitions,
            target_bytes,
        )
        if repair_rewrites >= max_skew_retries or next_partitions is None:
            reason = (
                f"staging produced {len(undersized)} undersized nonfinal object(s); "
                f"automatic skew repair stopped after {repair_rewrites} rewrite(s); "
                f"retained staging prefix: {stage_uri}"
            )
            emit(
                "dataframe_staging_skew_exhausted",
                listing.size_bytes,
                listing.size_bytes,
                reason,
            )
            raise ConfigurationError(reason)

        try:
            repair_df = df.repartition(next_partitions)
        except BaseException as error:
            emit(
                "dataframe_staging_failed",
                listing.size_bytes,
                listing.size_bytes,
                f"{stage_uri}; repartition; {type(error).__name__}",
            )
            raise

        if cleanup_staging:
            cleanup_record = _cleanup_stage(
                s3,
                stage_uri,
                list(listing.keys),
                stage_request_args,
                cfg.max_attempts,
            )
            cleanup_record["staging_prefix"] = stage_uri
            cleanup_records.append(cleanup_record)
            attempt_record["cleanup"] = cleanup_record
            emit(
                "staging_cleanup_completed"
                if cleanup_record["completed"]
                else "staging_cleanup_failed",
                listing.size_bytes,
                listing.size_bytes,
                None
                if cleanup_record["completed"]
                else str(cleanup_record.get("error", cleanup_record.get("errors"))),
            )

        repair_rewrites += 1
        previous_repair_partitions = next_partitions
        requested_partitions = next_partitions
        working_df = repair_df
        emit(
            "dataframe_staging_repartitioned",
            0,
            listing.size_bytes,
            f"rewrite={repair_rewrites}; partitions={next_partitions}",
        )

    sources = list(listing.sources)
    stage_keys = list(listing.keys)
    staged_bytes = listing.size_bytes

    def forward_event(item: TransferEvent) -> None:
        if events:
            events(replace(item, elapsed_seconds=time.monotonic() - started))

    try:
        if sources:
            composed = compose_s3_objects(
                sources,
                destination,
                config=cfg,
                client=s3,
                create_args=extra,
                progress=progress,
                events=forward_event if events else None,
            )
            result_metadata = composed.as_dict()
            transfer_id = composed.metadata["transfer_id"]
            completion_recovered = composed.metadata["completion_recovered"]
            parts = composed.parts
        else:
            transfer_id, completion_recovered = _write_empty_object(
                s3,
                destination,
                cfg,
                extra,
            )
            result_metadata = None
            parts = 1
    except BaseException as error:
        emit(
            "single_object_failed",
            0,
            staged_bytes,
            f"{stage_uri}; {type(error).__name__}",
        )
        raise

    if cleanup_staging:
        cleanup_record = _cleanup_stage(
            s3,
            stage_uri,
            stage_keys,
            stage_request_args,
            cfg.max_attempts,
        )
        cleanup_record["staging_prefix"] = stage_uri
        cleanup_records.append(cleanup_record)
        staging_attempts[-1]["cleanup"] = cleanup_record
        emit(
            "staging_cleanup_completed"
            if cleanup_record["completed"]
            else "staging_cleanup_failed",
            staged_bytes,
            staged_bytes,
            None
            if cleanup_record["completed"]
            else str(cleanup_record.get("error", cleanup_record.get("errors"))),
        )
    cleanup = _aggregate_cleanup(cleanup_staging, cleanup_records)

    elapsed = time.monotonic() - started
    emit("single_object_completed", staged_bytes, staged_bytes)
    return TransferResult(
        kind="dataframe-single-object",
        destination=destination,
        elapsed_seconds=elapsed,
        bytes_transferred=staged_bytes,
        throughput_mib_s=staged_bytes / MIB / elapsed if elapsed else None,
        parts=parts,
        metadata={
            "file_format": normalized_format,
            "staging_prefix": stage_uri,
            "staging_objects": len(stage_keys),
            "staged_payload_objects": len(sources),
            "staging_cleanup": cleanup,
            "staging_write": staged.as_dict(),
            "staging_attempts": staging_attempts,
            "skew_repair": {
                "enabled": max_skew_retries > 0,
                "max_retries": max_skew_retries,
                "rewrites": repair_rewrites,
                "repaired": repair_rewrites > 0,
                "final_requested_partitions": requested_partitions,
            },
            "composition": result_metadata,
            "transfer_id": transfer_id,
            "completion_recovered": completion_recovered,
            "payload_bytes_downloaded_by_client": 0,
            "global_row_order_guaranteed": False,
        },
    )
