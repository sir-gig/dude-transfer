"""Command-line interface for file transfers."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from .models import TransferEvent
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
    send.add_argument(
        "--json-events",
        action="store_true",
        help="stream structured transfer events to stderr",
    )
    send.add_argument("--expected-bucket-owner", help="reject a bucket owned by another account")
    send.add_argument("--sse-kms-key-id", help="encrypt the object with this KMS key")
    return parser


def _add_transfer_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--part-size", default="64MiB")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--initial-concurrency", type=int, default=2)
    parser.add_argument("--fixed-concurrency", action="store_true")
    parser.add_argument("--adjustment-window", type=int, default=2)
    parser.add_argument("--max-memory", default="512MiB")
    parser.add_argument("--threshold", default="100MiB")
    parser.add_argument("--checksum", choices=("CRC32", "SHA256", "none"), default="CRC32")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--checkpoint")
    parser.add_argument("--verify", action="store_true")


def _config(args: argparse.Namespace) -> MultipartConfig:
    return MultipartConfig(
        multipart_threshold=args.threshold,
        part_size=args.part_size,
        max_concurrency=args.concurrency,
        initial_concurrency=args.initial_concurrency,
        adaptive=not args.fixed_concurrency,
        adjustment_window=args.adjustment_window,
        max_in_flight_bytes=args.max_memory,
        checksum_algorithm=None if args.checksum == "none" else args.checksum,
        resume=not args.no_resume,
        checkpoint_path=args.checkpoint,
        verify_after_upload=args.verify,
    )


def _print_plan(plan: Any) -> None:
    print(f"source:       {plan.source}")
    print(f"destination:  {plan.destination}")
    print(f"size:         {format_size(plan.size_bytes)}")
    print(f"mode:         {'multipart' if plan.multipart else 'single request'}")
    print(f"parts:        {plan.part_count} x up to {format_size(plan.part_size)}")
    if plan.adaptive:
        print(f"concurrency:  adaptive {plan.initial_concurrency}..{plan.concurrency}")
    else:
        print(f"concurrency:  fixed {plan.concurrency}")
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

    def event(item: TransferEvent) -> None:
        if args.json_events:
            print(json.dumps(item.as_dict(), sort_keys=True), file=sys.stderr, flush=True)
        elif item.phase == "concurrency_changed":
            print(
                f"\nDude tuned concurrency to {item.concurrency} ({item.detail})",
                file=sys.stderr,
                flush=True,
            )

    create_args = {}
    if args.expected_bucket_owner:
        create_args["ExpectedBucketOwner"] = args.expected_bucket_owner
    if args.sse_kms_key_id:
        create_args.update(
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=args.sse_kms_key_id,
        )

    result = send_file(
        args.source,
        args.destination,
        config=config,
        create_args=create_args,
        progress=None if args.json or args.json_events else progress,
        events=event,
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
