"""Distributed Spark staging followed by S3-side single-object composition."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any
from urllib.parse import quote

from .compose import ComposeConfig, _effective_config, compose_s3_objects
from .dataframe import send_dataframe
from .exceptions import ConfigurationError
from .models import TransferEvent, TransferResult
from .s3 import (
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


def _list_stage(
    client: Any,
    stage_uri: str,
    request_args: Mapping[str, Any],
    max_attempts: int,
) -> tuple[list[str], list[str], int]:
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
    sources = [f"s3://{bucket}/{quote(key, safe='/')}" for key, _ in payloads]
    return sources, all_keys, sum(size for _, size in payloads)


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
    """

    normalized_format = file_format.lower()
    if normalized_format not in {"json", "text"}:
        raise ConfigurationError("single-object DataFrames support only json or text")
    if parse_size(target_file_size) < 5 * MIB:
        raise ConfigurationError("target_file_size must be at least 5 MiB for S3 composition")

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
    stage_uri = _stage_prefix(destination, staging_prefix, run_id)
    stage_bucket, _ = parse_s3_uri(stage_uri)
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

    emit("dataframe_staging_started", 0, 0, stage_uri)
    try:
        staged = send_dataframe(
            df,
            stage_uri,
            file_format=normalized_format,
            mode="errorifexists",
            compression=None,
            target_file_size=target_file_size,
            min_partitions=min_partitions,
            max_partitions=max_partitions,
            partitions=partitions,
            options=output_options,
        )
        sources, stage_keys, staged_bytes = _list_stage(
            s3,
            stage_uri,
            stage_request_args,
            cfg.max_attempts,
        )
    except BaseException as error:
        emit("dataframe_staging_failed", 0, 0, f"{stage_uri}; {type(error).__name__}")
        raise
    emit("dataframe_staging_completed", staged_bytes, staged_bytes)

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

    cleanup = {"requested": False, "completed": False, "deleted_objects": 0}
    if cleanup_staging:
        cleanup = _cleanup_stage(
            s3,
            stage_uri,
            stage_keys,
            stage_request_args,
            cfg.max_attempts,
        )
        emit(
            "staging_cleanup_completed" if cleanup["completed"] else "staging_cleanup_failed",
            staged_bytes,
            staged_bytes,
            None if cleanup["completed"] else str(cleanup.get("error", cleanup.get("errors"))),
        )

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
            "composition": result_metadata,
            "transfer_id": transfer_id,
            "completion_recovered": completion_recovered,
            "payload_bytes_downloaded_by_client": 0,
            "global_row_order_guaranteed": False,
        },
    )
