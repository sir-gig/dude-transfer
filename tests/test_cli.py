from dude_transfer.cli import _compose_config, _config, _parser, main


def test_cli_builds_adaptive_verified_config():
    args = _parser().parse_args(
        [
            "send-file",
            "source.bin",
            "s3://bucket/key",
            "--initial-concurrency",
            "3",
            "--concurrency",
            "12",
            "--adjustment-window",
            "4",
            "--verify",
        ]
    )
    config = _config(args)
    assert config.initial_concurrency == 3
    assert config.max_concurrency == 12
    assert config.adjustment_window == 4
    assert config.verify_after_upload is True


def test_plan_file_cli_reports_adaptive_range(tmp_path, capsys):
    source = tmp_path / "data.bin"
    with source.open("wb") as stream:
        stream.truncate(21 * 1024**2)
    status = main(
        [
            "plan-file",
            str(source),
            "s3://bucket/key",
            "--threshold",
            "1MiB",
            "--part-size",
            "5MiB",
            "--max-memory",
            "25MiB",
            "--initial-concurrency",
            "1",
            "--concurrency",
            "5",
        ]
    )
    assert status == 0
    assert "adaptive 1..5" in capsys.readouterr().out


def test_cli_builds_server_side_compose_config():
    args = _parser().parse_args(
        [
            "compose-s3",
            "s3://destination/result.jsonl",
            "s3://source/0000.jsonl",
            "s3://source/0001.jsonl",
            "--copy-part-size",
            "1GiB",
            "--initial-concurrency",
            "2",
            "--concurrency",
            "12",
            "--expected-source-bucket-owner",
            "123456789012",
            "--request-payer",
            "requester",
        ]
    )
    config = _compose_config(args)
    assert args.sources == ["s3://source/0000.jsonl", "s3://source/0001.jsonl"]
    assert config.copy_part_size == "1GiB"
    assert config.initial_concurrency == 2
    assert config.max_concurrency == 12
    assert config.expected_source_bucket_owner == "123456789012"
    assert config.request_payer == "requester"
