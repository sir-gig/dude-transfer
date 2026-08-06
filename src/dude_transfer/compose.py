"""Server-side composition of ordered S3 objects with multipart copy."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .adaptive import AdaptiveConcurrency
from .exceptions import CheckpointError, ConfigurationError, UploadVerificationError
from .models import ComposePlan, ComposeSource, TransferEvent, TransferResult
from .s3 import (
    S3_MAX_PART_SIZE,
    S3_MAX_PARTS,
    S3_MIN_PART_SIZE,
    _abort_upload,
    _call_with_retry,
    _completion_part,
    _conditional_write_args,
    _creation_args,
    _discard_upload,
    _error_code,
    _get_client,
    _http_status,
    _object_matches,
    _owner_args,
    _read_checkpoint,
    _remote_parts,
    _validated_create_args,
    _verify_object,
    _write_checkpoint,
    parse_s3_uri,
)
from .utils import MIB, ceil_div, parse_size


@dataclass(frozen=True)
class ComposeConfig:
    copy_part_size: int | str = 512 * MIB
    max_concurrency: int = 16
    initial_concurrency: int = 4
    adaptive: bool = True
    adjustment_window: int = 2
    checksum_algorithm: str = "CRC32"
    resume: bool = True
    checkpoint_path: str | os.PathLike[str] | None = None
    max_attempts: int = 4
    verify_after_compose: bool = False
    expected_source_bucket_owner: str | None = None
    request_payer: str | None = None
    if_none_match: bool = False
    if_match: str | None = None


@dataclass(frozen=True)
class _CopyPart:
    part_number: int
    source_index: int
    bucket: str
    key: str
    version_id: str | None
    etag: str
    first_byte: int
    last_byte: int
    size_bytes: int
    whole_source: bool


def _effective_config(config: ComposeConfig) -> ComposeConfig:
    part_size = parse_size(config.copy_part_size)
    if not S3_MIN_PART_SIZE <= part_size <= S3_MAX_PART_SIZE:
        raise ConfigurationError("copy_part_size must be between 5 MiB and 5 GiB")
    if config.max_concurrency < 1:
        raise ConfigurationError("max_concurrency must be greater than zero")
    if not 1 <= config.initial_concurrency <= config.max_concurrency:
        raise ConfigurationError(
            "initial_concurrency must be between one and max_concurrency"
        )
    if config.adjustment_window < 1:
        raise ConfigurationError("adjustment_window must be greater than zero")
    if config.max_attempts < 1:
        raise ConfigurationError("max_attempts must be greater than zero")
    checksum = config.checksum_algorithm.upper()
    if checksum not in {"CRC32", "SHA256"}:
        raise ConfigurationError("compose checksum_algorithm must be CRC32 or SHA256")
    if config.request_payer not in {None, "requester"}:
        raise ConfigurationError("request_payer must be 'requester' or None")
    _conditional_write_args(config.if_none_match, config.if_match)
    return ComposeConfig(
        copy_part_size=part_size,
        max_concurrency=config.max_concurrency,
        initial_concurrency=config.initial_concurrency,
        adaptive=config.adaptive,
        adjustment_window=config.adjustment_window,
        checksum_algorithm=checksum,
        resume=config.resume,
        checkpoint_path=config.checkpoint_path,
        max_attempts=config.max_attempts,
        verify_after_compose=config.verify_after_compose,
        expected_source_bucket_owner=config.expected_source_bucket_owner,
        request_payer=config.request_payer,
        if_none_match=config.if_none_match,
        if_match=config.if_match,
    )


def _source_request_args(config: ComposeConfig) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    if config.expected_source_bucket_owner:
        arguments["ExpectedBucketOwner"] = config.expected_source_bucket_owner
    if config.request_payer:
        arguments["RequestPayer"] = config.request_payer
    return arguments


def _copy_source_request_args(config: ComposeConfig) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    if config.expected_source_bucket_owner:
        arguments["ExpectedSourceBucketOwner"] = config.expected_source_bucket_owner
    if config.request_payer:
        arguments["RequestPayer"] = config.request_payer
    return arguments


def _head_source(client: Any, uri: str, config: ComposeConfig) -> ComposeSource:
    bucket, key = parse_s3_uri(uri)
    response = _call_with_retry(
        lambda: client.head_object(
            Bucket=bucket,
            Key=key,
            **_source_request_args(config),
        ),
        config.max_attempts,
    ).response
    size = int(response["ContentLength"])
    etag = str(response.get("ETag", ""))
    if not etag:
        raise UploadVerificationError(f"S3 returned no ETag while inspecting source {uri}")
    return ComposeSource(
        uri=uri,
        size_bytes=size,
        etag=etag,
        version_id=response.get("VersionId"),
        part_count=0,
    )


def _parts_for_source(
    source: ComposeSource,
    source_index: int,
    first_part_number: int,
    target_size: int,
) -> list[_CopyPart]:
    if source.size_bytes == 0:
        raise ConfigurationError(f"zero-length source objects cannot be composed: {source.uri}")
    bucket, key = parse_s3_uri(source.uri)
    minimum_count = ceil_div(source.size_bytes, S3_MAX_PART_SIZE)
    desired_count = ceil_div(source.size_bytes, target_size)
    maximum_count = max(1, source.size_bytes // S3_MIN_PART_SIZE)
    count = max(minimum_count, min(desired_count, maximum_count))
    balanced_size = ceil_div(source.size_bytes, count)
    parts: list[_CopyPart] = []
    for index in range(count):
        first = index * balanced_size
        last = min(source.size_bytes, first + balanced_size) - 1
        length = last - first + 1
        parts.append(
            _CopyPart(
                part_number=first_part_number + index,
                source_index=source_index,
                bucket=bucket,
                key=key,
                version_id=source.version_id,
                etag=source.etag,
                first_byte=first,
                last_byte=last,
                size_bytes=length,
                whole_source=count == 1,
            )
        )
    return parts


def _inspect_and_plan(
    sources: Sequence[str],
    destination: str,
    client: Any,
    config: ComposeConfig,
) -> tuple[ComposePlan, list[_CopyPart]]:
    if not sources:
        raise ConfigurationError("compose requires at least one ordered source object")
    parse_s3_uri(destination)
    inspected: list[ComposeSource | None] = [None] * len(sources)
    workers = min(32, config.max_concurrency, len(sources))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dude-head") as pool:
        futures = {
            pool.submit(_head_source, client, uri, config): index
            for index, uri in enumerate(sources)
        }
        for future in as_completed(futures):
            inspected[futures[future]] = future.result()
    resolved = [source for source in inspected if source is not None]

    copy_parts: list[_CopyPart] = []
    planned_sources: list[ComposeSource] = []
    next_part = 1
    for index, source in enumerate(resolved):
        parts = _parts_for_source(
            source,
            index,
            next_part,
            int(config.copy_part_size),
        )
        copy_parts.extend(parts)
        planned_sources.append(
            ComposeSource(
                uri=source.uri,
                size_bytes=source.size_bytes,
                etag=source.etag,
                version_id=source.version_id,
                part_count=len(parts),
            )
        )
        next_part += len(parts)

    if len(copy_parts) > S3_MAX_PARTS:
        raise ConfigurationError(
            f"compose requires {len(copy_parts)} parts; S3 allows at most {S3_MAX_PARTS}"
        )
    for part in copy_parts[:-1]:
        if part.size_bytes < S3_MIN_PART_SIZE:
            source = planned_sources[part.source_index]
            raise ConfigurationError(
                "every compose source except the final output part must contribute at least "
                f"5 MiB; {source.uri} contributes {part.size_bytes} bytes"
            )

    max_concurrency = min(config.max_concurrency, len(copy_parts))
    initial = (
        min(config.initial_concurrency, max_concurrency)
        if config.adaptive
        else max_concurrency
    )
    plan = ComposePlan(
        destination=destination,
        sources=tuple(planned_sources),
        size_bytes=sum(source.size_bytes for source in planned_sources),
        part_count=len(copy_parts),
        initial_concurrency=initial,
        concurrency=max_concurrency,
        adaptive=config.adaptive and initial < max_concurrency,
    )
    return plan, copy_parts


def plan_compose(
    sources: Sequence[str],
    destination: str,
    *,
    config: ComposeConfig | None = None,
    client: Any | None = None,
) -> ComposePlan:
    """Inspect ordered source objects and return a no-write composition plan."""

    cfg = _effective_config(config or ComposeConfig())
    s3 = _get_client(client)
    plan, _ = _inspect_and_plan(sources, destination, s3, cfg)
    return plan


def _identity(
    plan: ComposePlan,
    config: ComposeConfig,
    create_args: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = {
        "destination": plan.destination,
        "sources": [source.as_dict() for source in plan.sources],
        "copy_part_size": config.copy_part_size,
        "checksum_algorithm": config.checksum_algorithm,
        "conditional_args": _conditional_write_args(
            config.if_none_match,
            config.if_match,
        ),
        "create_args": dict(create_args),
    }
    canonical = json.dumps(manifest, sort_keys=True, default=str).encode("utf-8")
    return {
        "destination": plan.destination,
        "size": plan.size_bytes,
        "part_count": plan.part_count,
        "manifest_sha256": hashlib.sha256(canonical).hexdigest(),
    }


def _checkpoint_path(identity: Mapping[str, Any], config: ComposeConfig) -> Path:
    if config.checkpoint_path:
        return Path(config.checkpoint_path)
    digest = str(identity["manifest_sha256"])[:16]
    return Path(tempfile.gettempdir()) / f".dude-compose-{digest}.json"


def _reusable_parts(
    remote: Mapping[int, dict[str, Any]],
    copy_parts: Sequence[_CopyPart],
    checksum_algorithm: str,
) -> dict[int, dict[str, Any]]:
    expected = {part.part_number: part for part in copy_parts}
    checksum_name = f"Checksum{checksum_algorithm}"
    reusable: dict[int, dict[str, Any]] = {}
    for number, part in remote.items():
        planned = expected.get(number)
        if planned is None or part.get("_Size") != planned.size_bytes:
            continue
        if not part.get(checksum_name):
            continue
        reusable[number] = part
    return reusable


def compose_s3_objects(
    sources: Sequence[str],
    destination: str,
    *,
    config: ComposeConfig | None = None,
    client: Any | None = None,
    create_args: Mapping[str, Any] | None = None,
    progress: Callable[[int, int], None] | None = None,
    events: Callable[[TransferEvent], None] | None = None,
) -> TransferResult:
    """Compose ordered, byte-concatenable S3 objects without downloading payload data.

    Source objects are never modified or deleted. Their ETags and, when present,
    version IDs are pinned into every server-side copy request. Formats such as
    line-delimited text and pre-split binary streams can be composed; independent
    Parquet files cannot be concatenated into one valid Parquet file.
    """

    cfg = _effective_config(config or ComposeConfig())
    extra = _validated_create_args(create_args)
    s3 = _get_client(client)
    plan, copy_parts = _inspect_and_plan(sources, destination, s3, cfg)
    destination_bucket, destination_key = parse_s3_uri(destination)
    destination_requests = _owner_args(extra)
    conditional = _conditional_write_args(cfg.if_none_match, cfg.if_match)
    identity = _identity(plan, cfg, extra)
    checkpoint = _checkpoint_path(identity, cfg)
    started = time.monotonic()

    def emit(
        phase: str,
        done: int,
        *,
        part_number: int | None = None,
        concurrency: int | None = None,
        attempts: int | None = None,
        detail: str | None = None,
    ) -> None:
        if events:
            events(
                TransferEvent(
                    phase=phase,
                    elapsed_seconds=time.monotonic() - started,
                    bytes_transferred=done,
                    total_bytes=plan.size_bytes,
                    part_number=part_number,
                    concurrency=concurrency,
                    attempts=attempts,
                    detail=detail,
                )
            )

    emit("planned", 0, concurrency=plan.initial_concurrency)
    saved = _read_checkpoint(checkpoint) if cfg.resume else None
    upload_id: str | None = None
    transfer_id: str | None = None
    completed: dict[int, dict[str, Any]] = {}
    resumed = False
    reused_parts = 0

    if saved and saved.get("identity") == identity:
        upload_id = saved.get("upload_id")
        transfer_id = saved.get("transfer_id")
        if upload_id:
            if not isinstance(upload_id, str) or not isinstance(transfer_id, str):
                raise CheckpointError(
                    f"compose checkpoint {checkpoint} is missing its upload identity"
                )
            try:
                remote = _call_with_retry(
                    lambda: _remote_parts(
                        s3,
                        destination_bucket,
                        destination_key,
                        upload_id,
                        destination_requests,
                    ),
                    cfg.max_attempts,
                ).response
                completed = _reusable_parts(remote, copy_parts, cfg.checksum_algorithm)
                reused_parts = len(completed)
                resumed = True
            except Exception as error:
                if _error_code(error) not in {"NoSuchUpload", "404"}:
                    raise
                if _object_matches(
                    s3,
                    destination_bucket,
                    destination_key,
                    plan.size_bytes,
                    transfer_id,
                    destination_requests,
                    cfg.max_attempts,
                ):
                    checkpoint.unlink(missing_ok=True)
                    if progress:
                        progress(plan.size_bytes, plan.size_bytes)
                    elapsed = time.monotonic() - started
                    emit(
                        "resumed",
                        plan.size_bytes,
                        detail="recovered a commit completed before restart",
                    )
                    emit("completed", plan.size_bytes, concurrency=0)
                    return TransferResult(
                        kind="compose",
                        destination=destination,
                        elapsed_seconds=elapsed,
                        bytes_transferred=plan.size_bytes,
                        throughput_mib_s=None,
                        parts=plan.part_count,
                        resumed=True,
                        metadata={
                            "plan": asdict(plan),
                            "checksum": cfg.checksum_algorithm,
                            "retries": 0,
                            "reused_parts": 0,
                            "verified": True,
                            "completion_recovered": True,
                            "recovery_phase": "restart",
                            "transfer_id": transfer_id,
                            "destination_condition": conditional or None,
                            "payload_bytes_downloaded_by_client": 0,
                            "sources_preserved": True,
                            "adaptive": {
                                "enabled": False,
                                "initial_concurrency": 0,
                                "final_concurrency": 0,
                                "peak_concurrency": 0,
                                "changes": [],
                            },
                        },
                    )
                upload_id = None
                transfer_id = None
                completed = {}
    elif saved and saved.get("upload_id"):
        try:
            _abort_upload(
                s3,
                destination_bucket,
                destination_key,
                saved["upload_id"],
                destination_requests,
            )
        except Exception as error:
            if _error_code(error) not in {"NoSuchUpload", "404"}:
                raise CheckpointError(
                    "the compose manifest changed, but Dude could not abort its stale upload"
                ) from error

    if upload_id is None:
        transfer_id = uuid.uuid4().hex
        response = s3.create_multipart_upload(
            **_creation_args(
                destination_bucket,
                destination_key,
                cfg.checksum_algorithm,
                extra,
                transfer_id,
            )
        )
        upload_id = str(response["UploadId"])
    assert transfer_id is not None

    state: dict[str, Any] = {
        "version": 1,
        "identity": identity,
        "upload_id": upload_id,
        "transfer_id": transfer_id,
        "completed": {str(number): part for number, part in completed.items()},
    }
    expected_parts = {part.part_number: part for part in copy_parts}
    copied_bytes = sum(expected_parts[number].size_bytes for number in completed)
    if cfg.resume:
        _write_checkpoint(checkpoint, state)
    if progress:
        progress(copied_bytes, plan.size_bytes)
    if resumed:
        emit("resumed", copied_bytes, detail=f"reused {reused_parts} server-side parts")

    controller = AdaptiveConcurrency(
        initial=plan.initial_concurrency,
        maximum=plan.concurrency,
        window_parts=cfg.adjustment_window,
        started_at=time.monotonic(),
    )
    total_retries = 0

    def copy_part(part: _CopyPart) -> tuple[int, dict[str, Any], int, int]:
        copy_source = f"{part.bucket}/{quote(part.key, safe='/')}"
        if part.version_id:
            copy_source = f"{copy_source}?versionId={quote(part.version_id, safe='')}"

        def operation() -> dict[str, Any]:
            arguments: dict[str, Any] = {
                "Bucket": destination_bucket,
                "Key": destination_key,
                "UploadId": upload_id,
                "PartNumber": part.part_number,
                "CopySource": copy_source,
                "CopySourceIfMatch": part.etag,
                **destination_requests,
                **_copy_source_request_args(cfg),
            }
            if not part.whole_source:
                arguments["CopySourceRange"] = (
                    f"bytes={part.first_byte}-{part.last_byte}"
                )
            return s3.upload_part_copy(**arguments)

        outcome = _call_with_retry(operation, cfg.max_attempts)
        copied = outcome.response["CopyPartResult"]
        checksum_name = f"Checksum{cfg.checksum_algorithm}"
        checksum = copied.get(checksum_name)
        if not checksum:
            raise UploadVerificationError(
                f"S3 returned no {cfg.checksum_algorithm} checksum for copy part "
                f"{part.part_number}"
            )
        result = {
            "PartNumber": part.part_number,
            "ETag": copied["ETag"],
            checksum_name: checksum,
            "_Size": part.size_bytes,
        }
        return part.part_number, result, part.size_bytes, outcome.attempts

    missing = iter(part for part in copy_parts if part.part_number not in completed)
    source_exhausted = False
    committed = False
    completion_recovered = False
    try:
        with ThreadPoolExecutor(
            max_workers=plan.concurrency,
            thread_name_prefix="dude-copy",
        ) as pool:
            in_flight: dict[Future[tuple[int, dict[str, Any], int, int]], _CopyPart] = {}

            def fill_slots() -> None:
                nonlocal source_exhausted
                while not source_exhausted and len(in_flight) < controller.current:
                    try:
                        part = next(missing)
                    except StopIteration:
                        source_exhausted = True
                        return
                    in_flight[pool.submit(copy_part, part)] = part

            fill_slots()
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.pop(future)
                    number, result, size, attempts = future.result()
                    completed[number] = result
                    copied_bytes += size
                    total_retries += attempts - 1
                    state["completed"] = {
                        str(part_number): value for part_number, value in completed.items()
                    }
                    if cfg.resume:
                        _write_checkpoint(checkpoint, state)
                    if progress:
                        progress(copied_bytes, plan.size_bytes)
                    emit(
                        "part_completed",
                        copied_bytes,
                        part_number=number,
                        concurrency=controller.current,
                        attempts=attempts,
                    )
                    if plan.adaptive:
                        adaptation = controller.observe(
                            byte_count=size,
                            completed_at=time.monotonic(),
                            attempts=attempts,
                        )
                        if adaptation:
                            emit(
                                "concurrency_changed",
                                copied_bytes,
                                concurrency=adaptation.current,
                                detail=adaptation.reason,
                            )
                fill_slots()

        ordered = [
            _completion_part(completed[number])
            for number in range(1, plan.part_count + 1)
        ]

        def complete() -> dict[str, Any]:
            return s3.complete_multipart_upload(
                Bucket=destination_bucket,
                Key=destination_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": ordered},
                **destination_requests,
                **conditional,
            )

        try:
            _call_with_retry(complete, cfg.max_attempts)
            committed = True
        except Exception as error:
            completion_recovered = _object_matches(
                s3,
                destination_bucket,
                destination_key,
                plan.size_bytes,
                transfer_id,
                destination_requests,
                cfg.max_attempts,
            )
            if not completion_recovered:
                if conditional and (
                    _http_status(error) in {404, 409, 412}
                    or _error_code(error)
                    in {
                        "ConditionalRequestConflict",
                        "NoSuchKey",
                        "NoSuchUpload",
                        "PreconditionFailed",
                    }
                ):
                    _discard_upload(
                        s3,
                        destination_bucket,
                        destination_key,
                        upload_id,
                        destination_requests,
                        checkpoint,
                    )
                raise
            committed = True

        if cfg.verify_after_compose:
            _verify_object(
                s3,
                destination_bucket,
                destination_key,
                plan.size_bytes,
                transfer_id,
                destination_requests,
                cfg.max_attempts,
            )
    except BaseException as error:
        if committed:
            checkpoint.unlink(missing_ok=True)
        elif not cfg.resume:
            try:
                _abort_upload(
                    s3,
                    destination_bucket,
                    destination_key,
                    upload_id,
                    destination_requests,
                )
            except Exception:
                pass
        emit("failed", copied_bytes, detail=type(error).__name__)
        raise
    else:
        checkpoint.unlink(missing_ok=True)

    elapsed = time.monotonic() - started
    emit("completed", plan.size_bytes, concurrency=controller.current)
    return TransferResult(
        kind="compose",
        destination=destination,
        elapsed_seconds=elapsed,
        bytes_transferred=plan.size_bytes,
        throughput_mib_s=plan.size_bytes / MIB / elapsed if elapsed else None,
        parts=plan.part_count,
        resumed=resumed,
        metadata={
            "plan": asdict(plan),
            "checksum": cfg.checksum_algorithm,
            "retries": total_retries,
            "reused_parts": reused_parts,
            "verified": cfg.verify_after_compose,
            "completion_recovered": completion_recovered,
            "transfer_id": transfer_id,
            "destination_condition": conditional or None,
            "payload_bytes_downloaded_by_client": 0,
            "sources_preserved": True,
            "adaptive": {
                "enabled": plan.adaptive,
                "initial_concurrency": controller.initial,
                "final_concurrency": controller.current,
                "peak_concurrency": controller.peak,
                "changes": [item.as_dict() for item in controller.history],
            },
        },
    )
