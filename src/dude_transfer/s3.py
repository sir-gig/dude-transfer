"""Resumable, checksummed, adaptive multipart uploads to Amazon S3."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import struct
import time
import uuid
import zlib
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .adaptive import AdaptiveConcurrency
from .exceptions import (
    CheckpointError,
    ConfigurationError,
    SourceChangedError,
    UploadVerificationError,
)
from .models import FilePlan, TransferEvent, TransferResult
from .utils import GIB, MIB, ceil_div, parse_size

S3_MIN_PART_SIZE = 5 * MIB
S3_MAX_PART_SIZE = 5 * GIB
S3_MAX_PARTS = 10_000

_RESERVED_CREATE_ARGS = {
    "Body",
    "Bucket",
    "ChecksumAlgorithm",
    "ChecksumCRC32",
    "ChecksumSHA256",
    "Key",
    "MultipartUpload",
    "PartNumber",
    "UploadId",
}
_RETRYABLE_CODES = {
    "500",
    "502",
    "503",
    "504",
    "InternalError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
}


@dataclass(frozen=True)
class MultipartConfig:
    multipart_threshold: int | str = 100 * MIB
    part_size: int | str = 64 * MIB
    max_concurrency: int = 8
    initial_concurrency: int = 2
    adaptive: bool = True
    adjustment_window: int = 2
    max_in_flight_bytes: int | str = 512 * MIB
    checksum_algorithm: str | None = "CRC32"
    resume: bool = True
    checkpoint_path: str | os.PathLike[str] | None = None
    max_attempts: int = 4
    verify_after_upload: bool = False


@dataclass(frozen=True)
class _RetryOutcome:
    response: dict[str, Any]
    attempts: int


def _effective_config(config: MultipartConfig) -> MultipartConfig:
    threshold = parse_size(config.multipart_threshold)
    part_size = parse_size(config.part_size)
    in_flight = parse_size(config.max_in_flight_bytes)
    if part_size < S3_MIN_PART_SIZE:
        raise ConfigurationError("multipart part_size must be at least 5 MiB")
    if part_size > S3_MAX_PART_SIZE:
        raise ConfigurationError("multipart part_size cannot exceed 5 GiB")
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
    checksum = config.checksum_algorithm.upper() if config.checksum_algorithm else None
    if checksum not in {None, "CRC32", "SHA256"}:
        raise ConfigurationError("checksum_algorithm must be CRC32, SHA256, or None")
    return MultipartConfig(
        multipart_threshold=threshold,
        part_size=part_size,
        max_concurrency=config.max_concurrency,
        initial_concurrency=config.initial_concurrency,
        adaptive=config.adaptive,
        adjustment_window=config.adjustment_window,
        max_in_flight_bytes=in_flight,
        checksum_algorithm=checksum,
        resume=config.resume,
        checkpoint_path=config.checkpoint_path,
        max_attempts=config.max_attempts,
        verify_after_upload=config.verify_after_upload,
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ConfigurationError("destination must be an s3://bucket/key URI")
    if parsed.params or parsed.query or parsed.fragment:
        raise ConfigurationError("S3 URIs cannot contain parameters, queries, or fragments")
    key = unquote(parsed.path.lstrip("/"))
    if not key:
        raise ConfigurationError("destination must include an S3 object key")
    return parsed.netloc, key


def _validated_create_args(create_args: Mapping[str, Any] | None) -> dict[str, Any]:
    extra = dict(create_args or {})
    reserved = sorted(_RESERVED_CREATE_ARGS.intersection(extra))
    if reserved:
        names = ", ".join(reserved)
        raise ConfigurationError(f"create_args cannot override Dude-managed fields: {names}")
    sse_customer_fields = sorted(name for name in extra if name.startswith("SSECustomer"))
    if sse_customer_fields:
        raise ConfigurationError(
            "SSE-C is not supported; use bucket encryption, AES256, or aws:kms"
        )
    metadata = extra.get("Metadata", {})
    if not isinstance(metadata, Mapping):
        raise ConfigurationError("create_args Metadata must be a mapping")
    if any(str(key).lower().startswith("dude-") for key in metadata):
        raise ConfigurationError("S3 metadata keys beginning with 'dude-' are reserved")
    return extra


def _checkpoint_path(source: Path, config: MultipartConfig) -> Path:
    if config.checkpoint_path:
        return Path(config.checkpoint_path)
    return source.with_name(f".{source.name}.dude-upload.json")


def _choose_part_size(size: int, requested: int) -> int:
    minimum_for_part_limit = ceil_div(size, S3_MAX_PARTS)
    effective = max(requested, minimum_for_part_limit, S3_MIN_PART_SIZE)
    effective = ceil_div(effective, MIB) * MIB
    if effective > S3_MAX_PART_SIZE:
        raise ConfigurationError("file is too large for an S3 multipart upload")
    return effective


def plan_file(
    source: str | os.PathLike[str],
    destination: str,
    *,
    config: MultipartConfig | None = None,
) -> FilePlan:
    path = Path(source)
    if not path.is_file():
        raise FileNotFoundError(path)
    parse_s3_uri(destination)
    cfg = _effective_config(config or MultipartConfig())
    size = path.stat().st_size
    multipart = size >= int(cfg.multipart_threshold)
    memory_limit = int(cfg.max_in_flight_bytes)
    requested_part_size = min(int(cfg.part_size), memory_limit)
    part_size = _choose_part_size(size, requested_part_size) if multipart else max(size, 1)
    if multipart and part_size > memory_limit:
        raise ConfigurationError(
            "max_in_flight_bytes is too small for the required S3 multipart part size"
        )
    part_count = ceil_div(size, part_size) if size else 1
    memory_workers = max(1, memory_limit // max(part_size, 1))
    concurrency = min(cfg.max_concurrency, memory_workers, part_count)
    initial = min(concurrency, cfg.initial_concurrency) if cfg.adaptive else concurrency
    return FilePlan(
        source=str(path),
        destination=destination,
        size_bytes=size,
        multipart=multipart,
        part_size=part_size,
        part_count=part_count,
        initial_concurrency=initial,
        concurrency=concurrency,
        peak_buffer_bytes=concurrency * part_size if multipart else 0,
        adaptive=cfg.adaptive and multipart and initial < concurrency,
    )


def _write_checkpoint(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    try:
        temporary.chmod(0o600)
    except OSError:
        pass
    temporary.replace(path)


def _read_checkpoint(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as error:
        raise CheckpointError(f"cannot read upload checkpoint {path}: {error}") from error
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as error:
        raise CheckpointError(
            f"upload checkpoint {path} is corrupt; preserve it for cleanup before retrying"
        ) from error
    if not isinstance(state, dict):
        raise CheckpointError(f"upload checkpoint {path} does not contain an object")
    return state


def _file_identity(
    path: Path,
    bucket: str,
    key: str,
    part_size: int,
    checksum: str | None,
    create_args: Mapping[str, Any],
) -> dict[str, Any]:
    stat = path.stat()
    settings = json.dumps(
        {"checksum": checksum, "create_args": dict(create_args)},
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return {
        "source": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "bucket": bucket,
        "key": key,
        "part_size": part_size,
        "settings_sha256": hashlib.sha256(settings).hexdigest(),
    }


def _assert_source_unchanged(path: Path, identity: Mapping[str, Any]) -> None:
    stat = path.stat()
    if stat.st_size != identity["size"] or stat.st_mtime_ns != identity["mtime_ns"]:
        raise SourceChangedError(
            f"source file changed during upload: {path}; the multipart upload was not committed"
        )


def _error_code(error: Exception) -> str | None:
    try:
        return str(error.response["Error"]["Code"])
    except (AttributeError, KeyError, TypeError):
        return None


def _http_status(error: Exception) -> int | None:
    try:
        return int(error.response["ResponseMetadata"]["HTTPStatusCode"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def _is_retryable(error: Exception) -> bool:
    code = _error_code(error)
    status = _http_status(error)
    if code in _RETRYABLE_CODES or status in {408, 429, 500, 502, 503, 504}:
        return True
    if status is not None and 400 <= status < 500:
        return False
    return True


def _call_with_retry(
    operation: Callable[[], dict[str, Any]],
    max_attempts: int,
) -> _RetryOutcome:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return _RetryOutcome(operation(), attempt)
        except Exception as error:  # boto exceptions are an optional runtime dependency
            last_error = error
            if attempt == max_attempts or not _is_retryable(error):
                break
            delay = min(5.0, 0.25 * (2 ** (attempt - 1))) + random.random() * 0.1
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _checksum(payload: bytes, algorithm: str | None) -> tuple[str | None, str | None]:
    if algorithm == "CRC32":
        raw = struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)
        return "ChecksumCRC32", base64.b64encode(raw).decode("ascii")
    if algorithm == "SHA256":
        raw = hashlib.sha256(payload).digest()
        return "ChecksumSHA256", base64.b64encode(raw).decode("ascii")
    return None, None


def _creation_args(
    bucket: str,
    key: str,
    checksum: str | None,
    create_args: Mapping[str, Any],
    transfer_id: str,
) -> dict[str, Any]:
    arguments = {"Bucket": bucket, "Key": key, **dict(create_args)}
    metadata = dict(create_args.get("Metadata", {}))
    metadata["dude-transfer-id"] = transfer_id
    arguments["Metadata"] = metadata
    if checksum:
        arguments["ChecksumAlgorithm"] = checksum
    return arguments


def _owner_args(create_args: Mapping[str, Any]) -> dict[str, Any]:
    return {
        name: create_args[name]
        for name in ("ExpectedBucketOwner", "RequestPayer")
        if name in create_args
    }


def _new_upload(
    client: Any,
    bucket: str,
    key: str,
    checksum: str | None,
    create_args: Mapping[str, Any],
    transfer_id: str,
) -> str:
    response = client.create_multipart_upload(
        **_creation_args(bucket, key, checksum, create_args, transfer_id)
    )
    return str(response["UploadId"])


def _remote_parts(
    client: Any,
    bucket: str,
    key: str,
    upload_id: str,
    owner_args: Mapping[str, Any],
) -> dict[int, dict[str, Any]]:
    parts: dict[int, dict[str, Any]] = {}
    marker: int | None = None
    while True:
        arguments: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "UploadId": upload_id,
            **dict(owner_args),
        }
        if marker is not None:
            arguments["PartNumberMarker"] = marker
        response = client.list_parts(**arguments)
        for item in response.get("Parts", []):
            part = {
                "PartNumber": int(item["PartNumber"]),
                "ETag": item["ETag"],
                "_Size": int(item.get("Size", -1)),
            }
            for name in ("ChecksumCRC32", "ChecksumSHA256"):
                if name in item:
                    part[name] = item[name]
            parts[part["PartNumber"]] = part
        if not response.get("IsTruncated"):
            return parts
        marker = int(response["NextPartNumberMarker"])


def _part_length(plan: FilePlan, part_number: int) -> int:
    return min(plan.part_size, plan.size_bytes - ((part_number - 1) * plan.part_size))


def _reusable_parts(
    remote: Mapping[int, dict[str, Any]],
    plan: FilePlan,
    checksum_algorithm: str | None,
) -> dict[int, dict[str, Any]]:
    checksum_name = f"Checksum{checksum_algorithm}" if checksum_algorithm else None
    reusable: dict[int, dict[str, Any]] = {}
    for number, part in remote.items():
        if number < 1 or number > plan.part_count:
            continue
        if part.get("_Size") != _part_length(plan, number):
            continue
        if checksum_name and not part.get(checksum_name):
            continue
        reusable[number] = part
    return reusable


def _completion_part(part: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in part.items() if not key.startswith("_")}


def _get_client(client: Any | None) -> Any:
    if client is not None:
        return client
    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("install Dude with `pip install dude-transfer[s3]`") from error
    return boto3.client("s3")


def _put_small_file(
    client: Any,
    path: Path,
    bucket: str,
    key: str,
    checksum: str | None,
    create_args: Mapping[str, Any],
    transfer_id: str,
    max_attempts: int,
) -> int:
    def operation() -> dict[str, Any]:
        arguments = _creation_args(bucket, key, checksum, create_args, transfer_id)
        with path.open("rb") as stream:
            arguments["Body"] = stream
            return client.put_object(**arguments)

    return _call_with_retry(operation, max_attempts).attempts


def _object_matches(
    client: Any,
    bucket: str,
    key: str,
    size: int,
    transfer_id: str | None,
    owner_args: Mapping[str, Any],
    max_attempts: int,
) -> bool:
    if not transfer_id:
        return False
    try:
        response = _call_with_retry(
            lambda: client.head_object(Bucket=bucket, Key=key, **dict(owner_args)),
            max_attempts,
        ).response
    except Exception:
        return False
    metadata = {str(name).lower(): value for name, value in response.get("Metadata", {}).items()}
    return (
        int(response.get("ContentLength", -1)) == size
        and metadata.get("dude-transfer-id") == transfer_id
    )


def _verify_object(
    client: Any,
    bucket: str,
    key: str,
    size: int,
    transfer_id: str,
    owner_args: Mapping[str, Any],
    max_attempts: int,
) -> None:
    if not _object_matches(
        client,
        bucket,
        key,
        size,
        transfer_id,
        owner_args,
        max_attempts,
    ):
        raise UploadVerificationError(
            "S3 object size or Dude transfer identity did not match after upload"
        )


def _abort_upload(
    client: Any,
    bucket: str,
    key: str,
    upload_id: str,
    owner_args: Mapping[str, Any],
) -> None:
    client.abort_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        **dict(owner_args),
    )


def send_file(
    source: str | os.PathLike[str],
    destination: str,
    *,
    config: MultipartConfig | None = None,
    client: Any | None = None,
    create_args: Mapping[str, Any] | None = None,
    progress: Callable[[int, int], None] | None = None,
    events: Callable[[TransferEvent], None] | None = None,
) -> TransferResult:
    """Upload a file with bounded-memory adaptation and durable resume state.

    ``create_args`` can contain S3 creation options such as ``ContentType``,
    ``ServerSideEncryption``, ``SSEKMSKeyId``, or ``ExpectedBucketOwner``.
    Authentication is delegated to boto3's credential chain; Dude never accepts
    access keys.
    """

    path = Path(source)
    cfg = _effective_config(config or MultipartConfig())
    extra = _validated_create_args(create_args)
    plan = plan_file(path, destination, config=cfg)
    bucket, key = parse_s3_uri(destination)
    owner = _owner_args(extra)
    s3 = _get_client(client)
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
    identity = _file_identity(
        path,
        bucket,
        key,
        plan.part_size,
        cfg.checksum_algorithm,
        extra,
    )

    if not plan.multipart:
        transfer_id = uuid.uuid4().hex
        attempts = _put_small_file(
            s3,
            path,
            bucket,
            key,
            cfg.checksum_algorithm,
            extra,
            transfer_id,
            cfg.max_attempts,
        )
        _assert_source_unchanged(path, identity)
        if cfg.verify_after_upload:
            _verify_object(
                s3,
                bucket,
                key,
                plan.size_bytes,
                transfer_id,
                owner,
                cfg.max_attempts,
            )
        elapsed = time.monotonic() - started
        if progress:
            progress(plan.size_bytes, plan.size_bytes)
        emit("completed", plan.size_bytes, attempts=attempts, concurrency=1)
        return TransferResult(
            kind="file",
            destination=destination,
            elapsed_seconds=elapsed,
            bytes_transferred=plan.size_bytes,
            throughput_mib_s=plan.size_bytes / MIB / elapsed if elapsed else None,
            parts=1,
            metadata={
                "plan": plan.as_dict(),
                "checksum": cfg.checksum_algorithm,
                "retries": attempts - 1,
                "verified": cfg.verify_after_upload,
                "transfer_id": transfer_id,
            },
        )

    checkpoint = _checkpoint_path(path, cfg)
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
            try:
                remote = _call_with_retry(
                    lambda: _remote_parts(s3, bucket, key, upload_id, owner),
                    cfg.max_attempts,
                ).response
                completed = _reusable_parts(remote, plan, cfg.checksum_algorithm)
                reused_parts = len(completed)
                resumed = True
            except Exception as error:
                if _error_code(error) not in {"NoSuchUpload", "404"}:
                    raise
                upload_id = None
                transfer_id = None
                completed = {}
    elif saved and saved.get("upload_id"):
        old_identity = saved.get("identity", {})
        if old_identity.get("bucket") == bucket and old_identity.get("key") == key:
            try:
                _abort_upload(s3, bucket, key, saved["upload_id"], owner)
            except Exception as error:
                if _error_code(error) not in {"NoSuchUpload", "404"}:
                    raise CheckpointError(
                        "the source or upload settings changed, but Dude could not abort the "
                        "stale multipart upload"
                    ) from error

    if upload_id is None:
        transfer_id = uuid.uuid4().hex
        upload_id = _new_upload(
            s3,
            bucket,
            key,
            cfg.checksum_algorithm,
            extra,
            transfer_id,
        )
    assert transfer_id is not None

    state: dict[str, Any] = {
        "version": 2,
        "identity": identity,
        "upload_id": upload_id,
        "transfer_id": transfer_id,
        "completed": {str(number): part for number, part in completed.items()},
    }
    uploaded_bytes = sum(_part_length(plan, number) for number in completed)
    if cfg.resume:
        _write_checkpoint(checkpoint, state)
    if progress:
        progress(uploaded_bytes, plan.size_bytes)
    if resumed:
        emit("resumed", uploaded_bytes, detail=f"reused {reused_parts} remote parts")

    total_retries = 0
    controller = AdaptiveConcurrency(
        initial=plan.initial_concurrency,
        maximum=plan.concurrency,
        window_parts=cfg.adjustment_window,
        started_at=time.monotonic(),
    )

    def upload_part(part_number: int) -> tuple[int, dict[str, Any], int, int]:
        offset = (part_number - 1) * plan.part_size
        length = _part_length(plan, part_number)
        with path.open("rb") as stream:
            stream.seek(offset)
            payload = stream.read(length)
        if len(payload) != length:
            raise OSError(f"short read for part {part_number}")
        checksum_name, checksum_value = _checksum(payload, cfg.checksum_algorithm)

        def operation() -> dict[str, Any]:
            arguments: dict[str, Any] = {
                "Bucket": bucket,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
                "Body": payload,
                **owner,
            }
            if cfg.checksum_algorithm:
                arguments["ChecksumAlgorithm"] = cfg.checksum_algorithm
            if checksum_name and checksum_value:
                arguments[checksum_name] = checksum_value
            return s3.upload_part(**arguments)

        outcome = _call_with_retry(operation, cfg.max_attempts)
        part = {
            "PartNumber": part_number,
            "ETag": outcome.response["ETag"],
            "_Size": length,
        }
        if checksum_name and checksum_value:
            part[checksum_name] = outcome.response.get(checksum_name, checksum_value)
        return part_number, part, length, outcome.attempts

    missing = iter(
        number for number in range(1, plan.part_count + 1) if number not in completed
    )
    source_exhausted = False
    committed = False
    completion_recovered = False

    try:
        with ThreadPoolExecutor(
            max_workers=plan.concurrency,
            thread_name_prefix="dude-s3",
        ) as pool:
            in_flight: dict[Future[tuple[int, dict[str, Any], int, int]], int] = {}

            def fill_slots() -> None:
                nonlocal source_exhausted
                while not source_exhausted and len(in_flight) < controller.current:
                    try:
                        number = next(missing)
                    except StopIteration:
                        source_exhausted = True
                        return
                    in_flight[pool.submit(upload_part, number)] = number

            fill_slots()
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.pop(future)
                    number, part, length, attempts = future.result()
                    completed[number] = part
                    uploaded_bytes += length
                    total_retries += attempts - 1
                    state["completed"] = {
                        str(part_number): value for part_number, value in completed.items()
                    }
                    if cfg.resume:
                        _write_checkpoint(checkpoint, state)
                    if progress:
                        progress(uploaded_bytes, plan.size_bytes)
                    emit(
                        "part_completed",
                        uploaded_bytes,
                        part_number=number,
                        concurrency=controller.current,
                        attempts=attempts,
                    )
                    if cfg.adaptive:
                        adaptation = controller.observe(
                            byte_count=length,
                            completed_at=time.monotonic(),
                            attempts=attempts,
                        )
                        if adaptation:
                            emit(
                                "concurrency_changed",
                                uploaded_bytes,
                                concurrency=adaptation.current,
                                detail=adaptation.reason,
                            )
                fill_slots()

        _assert_source_unchanged(path, identity)
        ordered = [
            _completion_part(completed[number])
            for number in range(1, plan.part_count + 1)
        ]

        def complete() -> dict[str, Any]:
            return s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": ordered},
                **owner,
            )

        try:
            _call_with_retry(complete, cfg.max_attempts)
            committed = True
        except Exception:
            completion_recovered = _object_matches(
                s3,
                bucket,
                key,
                plan.size_bytes,
                transfer_id,
                owner,
                cfg.max_attempts,
            )
            if not completion_recovered:
                raise
            committed = True

        if cfg.verify_after_upload:
            _verify_object(
                s3,
                bucket,
                key,
                plan.size_bytes,
                transfer_id,
                owner,
                cfg.max_attempts,
            )
    except SourceChangedError:
        try:
            _abort_upload(s3, bucket, key, upload_id, owner)
        finally:
            checkpoint.unlink(missing_ok=True)
        emit("failed", uploaded_bytes, detail="source_changed")
        raise
    except BaseException as error:
        if committed:
            checkpoint.unlink(missing_ok=True)
        elif not cfg.resume:
            try:
                _abort_upload(s3, bucket, key, upload_id, owner)
            except Exception:
                pass
        emit("failed", uploaded_bytes, detail=type(error).__name__)
        raise
    else:
        checkpoint.unlink(missing_ok=True)

    elapsed = time.monotonic() - started
    emit("completed", plan.size_bytes, concurrency=controller.current)
    return TransferResult(
        kind="file",
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
            "verified": cfg.verify_after_upload,
            "completion_recovered": completion_recovered,
            "transfer_id": transfer_id,
            "adaptive": {
                "enabled": cfg.adaptive,
                "initial_concurrency": controller.initial,
                "final_concurrency": controller.current,
                "peak_concurrency": controller.peak,
                "changes": [item.as_dict() for item in controller.history],
            },
        },
    )
