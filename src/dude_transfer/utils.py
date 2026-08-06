"""Small dependency-free helpers."""

from __future__ import annotations

import math
import re

MIB = 1024 * 1024
GIB = 1024 * MIB

_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": MIB,
    "gib": GIB,
    "tib": 1024**4,
}


def parse_size(value: int | str) -> int:
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("size must be greater than zero")
        return value
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b)?\s*", value, re.IGNORECASE)
    if not match:
        raise ValueError(f"invalid size: {value!r}")
    number = float(match.group(1))
    unit = (match.group(2) or "b").lower()
    size = int(number * _SIZE_UNITS[unit])
    if size <= 0:
        raise ValueError("size must be greater than zero")
    return size


def format_size(size: int | None) -> str:
    if size is None:
        return "unknown"
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def ceil_div(numerator: int, denominator: int) -> int:
    return math.ceil(numerator / denominator)
