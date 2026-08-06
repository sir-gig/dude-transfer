"""Command-line interface for file transfers."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from .s3 import MultipartConfig, plan_file, send_file
from .utils import format_size


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dude", description="Fast, observable bulk transfers")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect = commands.add_parser("plan-file", help="show the upload plan without sending data")
    inspect.add_argument("source")
    inspect.add_argument("destination")
    _add_transfer_options(inspect)

    send = commands.add_parser("send-file", help="upload a file to S3")
    send.add_argument("source")
    send.add_argument("destination")
    _add_transfer_options(send)
    send.add_argument("--json", action="store_true", help="print the result as JSON")
    return parser


def _add_transfer_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--part-size", default="64MiB")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--max-memory", default="512MiB")
    parser.add_argument("--threshold", default="100MiB")
    parser.add_argument("--checksum", choices=("CRC32", "SHA256", "none"), default="CRC32")
    parser.add_argument("--no-resume", action="store_true")


def _config(args: argparse.Namespace) -> MultipartConfig:
    return MultipartConfig(
        multipart_threshold=args.threshold,
        part_size=args.part_size,
        max_concurrency=args.concurrency,
        max_in_flight_bytes=args.max_memory,
        checksum_algorithm=None if args.checksum == "none" else args.checksum,
        resume=not args.no_resume,
    )


def _print_plan(plan: Any) -> None:
    print(f"source:       {plan.source}")
    print(f"destination:  {plan.destination}")
    print(f"size:         {format_size(plan.size_bytes)}")
    print(f"mode:         {'multipart' if plan.multipart else 'single request'}")
    print(f"parts:        {plan.part_count} x up to {format_size(plan.part_size)}")
    print(f"concurrency:  {plan.concurrency}")
    print(f"peak buffers: {format_size(plan.peak_buffer_bytes)}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = _config(args)
    if args.command == "plan-file":
        _print_plan(plan_file(args.source, args.destination, config=config))
        return 0

    last_update = 0.0

    def progress(done: int, total: int) -> None:
        nonlocal last_update
        now = time.monotonic()
        if now - last_update < 0.2 and done != total:
            return
        last_update = now
        percent = (done / total * 100) if total else 100.0
        print(
            f"\r{percent:6.2f}%  {format_size(done)} / {format_size(total)}",
            end="",
            file=sys.stderr,
            flush=True,
        )

    result = send_file(
        args.source,
        args.destination,
        config=config,
        progress=None if args.json else progress,
    )
    if not args.json:
        print(file=sys.stderr)
        print(
            f"sent {format_size(result.bytes_transferred)} in {result.elapsed_seconds:.2f}s "
            f"({result.throughput_mib_s:.1f} MiB/s)"
        )
    else:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
