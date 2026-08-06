"""Command-line interface for file transfers."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from .compose import ComposeConfig, compose_s3_objects, plan_compose
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

    inspect_compose = commands.add_parser(
        "plan-compose",
        help="inspect an ordered server-side S3 composition without writing",
    )
    inspect_compose.add_argument("destination")
    inspect_compose.add_argument("sources", nargs="+")
    _add_compose_options(inspect_compose)
    inspect_compose.add_argument("--json", action="store_true", help="print the plan as JSON")

    compose = commands.add_parser(
        "compose-s3",
        help="concatenate ordered S3 objects with server-side multipart copy",
    )
    compose.add_argument("destination")
    compose.add_argument("sources", nargs="+")
    _add_compose_options(compose)
    compose.add_argument("--json", action="store_true", help="print the result as JSON")
    compose.add_argument(
        "--json-events",
        action="store_true",
        help="stream structured transfer events to stderr",
    )
    compose.add_argument("--expected-bucket-owner", help="pin the destination AWS account")
    compose.add_argument("--sse-kms-key-id", help="encrypt the result with this KMS key")
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
    condition = parser.add_mutually_exclusive_group()
    condition.add_argument(
        "--if-none-match",
        action="store_true",
        help="commit only if the destination key does not exist",
    )
    condition.add_argument("--if-match", help="commit only if the destination ETag matches")


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
        if_none_match=args.if_none_match,
        if_match=args.if_match,
    )


def _add_compose_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--copy-part-size", default="512MiB")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--initial-concurrency", type=int, default=4)
    parser.add_argument("--fixed-concurrency", action="store_true")
    parser.add_argument("--adjustment-window", type=int, default=2)
    parser.add_argument("--checksum", choices=("CRC32", "SHA256"), default="CRC32")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--checkpoint")
    parser.add_argument("--verify", action="store_true")
    condition = parser.add_mutually_exclusive_group()
    condition.add_argument(
        "--if-none-match",
        action="store_true",
        help="commit only if the destination key does not exist",
    )
    condition.add_argument("--if-match", help="commit only if the destination ETag matches")
    parser.add_argument("--expected-source-bucket-owner", help="pin the source AWS account")
    parser.add_argument(
        "--request-payer",
        choices=("requester",),
        help="acknowledge requester-pays source buckets",
    )


def _compose_config(args: argparse.Namespace) -> ComposeConfig:
    return ComposeConfig(
        copy_part_size=args.copy_part_size,
        max_concurrency=args.concurrency,
        initial_concurrency=args.initial_concurrency,
        adaptive=not args.fixed_concurrency,
        adjustment_window=args.adjustment_window,
        checksum_algorithm=args.checksum,
        resume=not args.no_resume,
        checkpoint_path=args.checkpoint,
        verify_after_compose=args.verify,
        expected_source_bucket_owner=getattr(args, "expected_source_bucket_owner", None),
        request_payer=getattr(args, "request_payer", None),
        if_none_match=args.if_none_match,
        if_match=args.if_match,
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


def _print_compose_plan(plan: Any) -> None:
    print(f"destination:  {plan.destination}")
    print(f"sources:      {len(plan.sources)} ordered objects")
    print(f"size:         {format_size(plan.size_bytes)}")
    print(f"copy parts:   {plan.part_count}")
    if plan.adaptive:
        print(f"concurrency:  adaptive {plan.initial_concurrency}..{plan.concurrency}")
    else:
        print(f"concurrency:  fixed {plan.concurrency}")
    print("data path:    S3 to S3 (no payload download)")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "plan-file":
        _print_plan(plan_file(args.source, args.destination, config=_config(args)))
        return 0
    if args.command == "plan-compose":
        plan = plan_compose(
            args.sources,
            args.destination,
            config=_compose_config(args),
        )
        if args.json:
            print(json.dumps(plan.as_dict(), indent=2, sort_keys=True))
        else:
            _print_compose_plan(plan)
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
    if getattr(args, "request_payer", None):
        create_args["RequestPayer"] = args.request_payer
    if args.sse_kms_key_id:
        create_args.update(
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=args.sse_kms_key_id,
        )

    if args.command == "send-file":
        result = send_file(
            args.source,
            args.destination,
            config=_config(args),
            create_args=create_args,
            progress=None if args.json or args.json_events else progress,
            events=event,
        )
        action = "sent"
    else:
        result = compose_s3_objects(
            args.sources,
            args.destination,
            config=_compose_config(args),
            create_args=create_args,
            progress=None if args.json or args.json_events else progress,
            events=event,
        )
        action = "composed"
    if not args.json:
        print(file=sys.stderr)
        print(
            f"{action} {format_size(result.bytes_transferred)} in {result.elapsed_seconds:.2f}s "
            f"({result.throughput_mib_s:.1f} MiB/s)"
        )
    else:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
