"""Serializable models returned by Dude."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class Diagnostic:
    severity: str
    code: str
    message: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DataFramePlan:
    destination: str
    file_format: str
    target_file_size: int
    estimated_bytes: int | None
    partitions_before: int | None
    partitions_after: int | None
    action: str
    diagnostics: tuple[Diagnostic, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FilePlan:
    source: str
    destination: str
    size_bytes: int
    multipart: bool
    part_size: int
    part_count: int
    concurrency: int
    peak_buffer_bytes: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TransferResult:
    kind: str
    destination: str
    elapsed_seconds: float
    bytes_transferred: int | None
    throughput_mib_s: float | None
    parts: int
    resumed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
