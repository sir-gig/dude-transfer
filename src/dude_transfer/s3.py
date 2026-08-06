"""Resumable, checksummed, concurrent multipart uploads to Amazon S3."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import struct
import threading
import time
import zlib
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from .models import FilePlan, TransferResult
from .utils import GIB, MIB, ceil_div, parse_size

S3_MIN_PART_SIZE = 5 * MIB
S3_MAX_PART_SIZE = 5 * GIB
S3_MAX_PARTS = 10_000


@dataclass(frozen=True)
class MultipartConfig:
    multipart_threshold: int | str = 100 * MIB
    part_size: int | str = 64 * MIB
    max_concurrency: int = 8
    max_in_flight_bytes: int | str = 512 * MIB
    checksum_algorithm: str | None = "CRC32"
    resume: bool = True
    checkpoint_path: str | os.PathLike[str] | None = None
    max_attempts: int = 4


def _effective_config(config: MultipartConfig) -> MultipartConfig:
    threshold = parse_size(config.multipart_threshold)
    part_size = parse_size(config.part_size)
    in_flight = parse_size(config.max_in_flight_bytes)
    if part_size < S3_MIN_PART_SIZE:
        raise ValueError("multipart part_size must be at least 5 MiB")
    if part_size > S3_MAX_PART_SIZE:
        raise ValueError("multipart part_size cannot exceed 5 GiB")
    if config.max_concurrency < 1:
        raise ValueError("max_concurrency must be greater than zero")
    if config.max_attempts < 1:
        raise ValueError("max_attempts must be greater than zero")
    checksum = config.checksum_algorithm.upper() if config.checksum_algorithm else None
    if checksum not in {None, "CRC32", "SHA256"}:
        raise ValueError("checksum_algorithm must be CRC32, SHA256, or None")
    return MultipartConfig(
        multipart_threshold=threshold,
        part_size=part_size,
        max_concurrency=config.max_concurrency,
        max_in_flight_bytes=in_flight,
        checksum_algorithm=checksum,
        resume=config.resume,
        checkpoint_path=config.checkpoint_path,
        max_attempts=config.max_attempts,
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("destination must be an s3://bucket/key URI")
    key = unquote(parsed.path.lstrip("/"))
    if not key:
        raise ValueError("destination must include an S3 object key")
    return parsed.netloc, key


def _checkpoint_path(source: Path, config: MultipartConfig) -> Path:
    if config.checkpoint_path:
        return Path(config.checkpoint_path)
    return source.with_name(f".{source.name}.dude-upload.json")


def _choose_part_size(size: int, requested: int) -> int:
    minimum_for_part_limit = ceil_div(size, S3_MAX_PARTS)
    effective = max(requested, minimum_for_part_limit, S3_MIN_PART_SIZE)
    effective = ceil_div(effective, MIB) * MIB
    if effective > S3_MAX_PART_SIZE:
        raise ValueError("file is too large for an S3 multipart upload")
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
    part_size = _choose_part_size(size, int(cfg.part_size)) if multipart else max(size, 1)
    part_count = ceil_div(size, part_size) if size else 1
    memory_workers = max(1, int(cfg.max_in_flight_bytes) // max(part_size, 1))
    concurrency = min(cfg.max_concurrency, memory_workers, part_count)
    return FilePlan(
        source=str(path),
        destination=destination,
        size_bytes=size,
        multipart=multipart,
        part_size=part_size,
        part_count=part_count,
        concurrency=concurrency,
        peak_buffer_bytes=concurrency * part_size if multipart else 0,
    )


def _write_checkpoint(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def _read_checkpoint(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (json.JSONDecodeError, OSError):
        return None


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


def _error_code(error: Exception) -> str | None:
    try:
        return str(error.response["Error"]["Code"])
    except (AttributeError, KeyError, TypeError):
        return None


def _checksum(payload: bytes, algorithm: str | None) -> tuple[str | None, str | None]:
    if algorithm == "CRC32":
        raw = struct.pack(">I", zlib.crc32(payload) & 0xFFFFFFFF)
        return "ChecksumCRC32", base64.b64encode(raw).decode("ascii")
    if algorithm == "SHA256":
        raw = hashlib.sha256(payload).digest()
        return "ChecksumSHA256", base64.b64encode(raw).decode("ascii")
    return None, None


def _retry(operation: Callable[[], dict[str, Any]], max_attempts: int) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as error:  # boto exceptions are optional runtime dependencies
            last_error = error
            if attempt + 1 == max_attempts:
                break
            delay = min(5.0, 0.25 * (2**attempt)) + random.random() * 0.1
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _new_upload(
    client: Any,
    bucket: str,
    key: str,
    checksum: str | None,
    create_args: Mapping[str, Any],
) -> str:
    arguments = {"Bucket": bucket, "Key": key, **dict(create_args)}
    if checksum:
        arguments["ChecksumAlgorithm"] = checksum
    response = client.create_multipart_upload(**arguments)
    return str(response["UploadId"])


def _remote_parts(client: Any, bucket: str, key: str, upload_id: str) -> dict[int, dict[str, Any]]:
    parts: dict[int, dict[str, Any]] = {}
    marker: int | None = None
    while True:
        arguments: dict[str, Any] = {"Bucket": bucket, "Key": key, "UploadId": upload_id}
        if marker is not None:
            arguments["PartNumberMarker"] = marker
        response = client.list_parts(**arguments)
        for item in response.get("Parts", []):
            part = {"PartNumber": int(item["PartNumber"]), "ETag": item["ETag"]}
            for name in ("ChecksumCRC32", "ChecksumSHA256"):
                if name in item:
                    part[name] = item[name]
            parts[part["PartNumber"]] = part
        if not response.get("IsTruncated"):
            return parts
        marker = int(response["NextPartNumberMarker"])


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
) -> None:
    arguments = {"Bucket": bucket, "Key": key, **dict(create_args)}
    with path.open("rb") as stream:
        arguments["Body"] = stream
        if checksum:
            arguments["ChecksumAlgorithm"] = checksum
        client.put_object(**arguments)


def send_file(
    source: str | os.PathLike[str],
    destination: str,
    *,
    config: MultipartConfig | None = None,
    client: Any | None = None,
    create_args: Mapping[str, Any] | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> TransferResult:
    """Upload a file with bounded-memory concurrency and durable resume state.

    ``create_args`` can contain S3 creation options such as ``ContentType``,
    ``ServerSideEncryption``, or ``SSEKMSKeyId``. Authentication is delegated to
    boto3's standard credential chain; Dude never accepts access keys.
    """

    path = Path(source)
    cfg = _effective_config(config or MultipartConfig())
    plan = plan_file(path, destination, config=cfg)
    bucket, key = parse_s3_uri(destination)
    s3 = _get_client(client)
    extra = dict(create_args or {})
    started = time.monotonic()

    if not plan.multipart:
        _put_small_file(s3, path, bucket, key, cfg.checksum_algorithm, extra)
        elapsed = time.monotonic() - started
        return TransferResult(
            kind="file",
            destination=destination,
            elapsed_seconds=elapsed,
            bytes_transferred=plan.size_bytes,
            throughput_mib_s=plan.size_bytes / MIB / elapsed if elapsed else None,
            parts=1,
            metadata={"plan": plan.as_dict(), "checksum": cfg.checksum_algorithm},
        )

    checkpoint = _checkpoint_path(path, cfg)
    identity = _file_identity(
        path,
        bucket,
        key,
        plan.part_size,
        cfg.checksum_algorithm,
        extra,
    )
    saved = _read_checkpoint(checkpoint) if cfg.resume else None
    upload_id: str | None = None
    completed: dict[int, dict[str, Any]] = {}
    resumed = False

    if saved and saved.get("identity") == identity:
        upload_id = saved.get("upload_id")
        if upload_id:
            try:
                completed = _retry(
                    lambda: _remote_parts(s3, bucket, key, upload_id),
                    cfg.max_attempts,
                )
                resumed = True
            except Exception as error:
                if _error_code(error) not in {"NoSuchUpload", "404"}:
                    raise
                upload_id = None
                completed = {}
    elif saved and saved.get("upload_id"):
        old_identity = saved.get("identity", {})
        if old_identity.get("bucket") == bucket and old_identity.get("key") == key:
            try:
                s3.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=saved["upload_id"],
                )
            except Exception:
                pass

    if upload_id is None:
        upload_id = _new_upload(s3, bucket, key, cfg.checksum_algorithm, extra)

    state: dict[str, Any] = {
        "version": 1,
        "identity": identity,
        "upload_id": upload_id,
        "completed": {str(number): part for number, part in completed.items()},
    }
    lock = threading.Lock()
    uploaded_bytes = sum(
        min(plan.part_size, plan.size_bytes - ((number - 1) * plan.part_size))
        for number in completed
    )
    if cfg.resume:
        _write_checkpoint(checkpoint, state)
    if progress:
        progress(uploaded_bytes, plan.size_bytes)

    def upload_part(part_number: int) -> tuple[int, dict[str, Any], int]:
        offset = (part_number - 1) * plan.part_size
        length = min(plan.part_size, plan.size_bytes - offset)
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
            }
            if cfg.checksum_algorithm:
                arguments["ChecksumAlgorithm"] = cfg.checksum_algorithm
            if checksum_name and checksum_value:
                arguments[checksum_name] = checksum_value
            return s3.upload_part(**arguments)

        response = _retry(operation, cfg.max_attempts)
        part = {"PartNumber": part_number, "ETag": response["ETag"]}
        if checksum_name and checksum_value:
            part[checksum_name] = response.get(checksum_name, checksum_value)
        return part_number, part, length

    missing = [number for number in range(1, plan.part_count + 1) if number not in completed]
    try:
        with ThreadPoolExecutor(max_workers=plan.concurrency, thread_name_prefix="dude-s3") as pool:
            futures = {pool.submit(upload_part, number): number for number in missing}
            for future in as_completed(futures):
                number, part, length = future.result()
                with lock:
                    completed[number] = part
                    uploaded_bytes += length
                    state["completed"] = {
                        str(part_number): value for part_number, value in completed.items()
                    }
                    if cfg.resume:
                        _write_checkpoint(checkpoint, state)
                    if progress:
                        progress(uploaded_bytes, plan.size_bytes)

        ordered = [completed[number] for number in range(1, plan.part_count + 1)]
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": ordered},
        )
    except BaseException:
        if not cfg.resume:
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            except Exception:
                pass
        raise
    else:
        if cfg.resume:
            checkpoint.unlink(missing_ok=True)

    elapsed = time.monotonic() - started
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
            "upload_id": upload_id,
        },
    )
