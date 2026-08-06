import json
from pathlib import Path

import pytest

from dude_transfer import CheckpointError, ConfigurationError, SourceChangedError
from dude_transfer.s3 import MultipartConfig, parse_s3_uri, plan_file, send_file


class FakeS3:
    def __init__(self):
        self.uploaded = {}
        self.completed = None
        self.put = None
        self.aborted = None
        self.object_size = None
        self.object_metadata = None

    def put_object(self, **kwargs):
        payload = kwargs["Body"].read()
        self.put = {**kwargs, "Body": payload}
        self.object_size = len(payload)
        self.object_metadata = kwargs.get("Metadata", {})
        return {"ETag": '"small"'}

    def create_multipart_upload(self, **kwargs):
        self.create_args = kwargs
        return {"UploadId": "upload-1"}

    def list_parts(self, **kwargs):
        parts = []
        for number, upload in sorted(self.uploaded.items()):
            part = {"PartNumber": number, "ETag": f'"part-{number}"'}
            part["Size"] = len(upload["Body"])
            if upload.get("ChecksumCRC32"):
                part["ChecksumCRC32"] = upload["ChecksumCRC32"]
            parts.append(part)
        return {"Parts": parts, "IsTruncated": False}

    def upload_part(self, **kwargs):
        number = kwargs["PartNumber"]
        self.uploaded[number] = kwargs
        return {"ETag": f'"part-{number}"', "ChecksumCRC32": kwargs.get("ChecksumCRC32")}

    def complete_multipart_upload(self, **kwargs):
        self.completed = kwargs
        self.object_size = sum(len(upload["Body"]) for upload in self.uploaded.values())
        self.object_metadata = self.create_args.get("Metadata", {})
        return {"ETag": '"complete"'}

    def abort_multipart_upload(self, **kwargs):
        self.aborted = kwargs

    def head_object(self, **kwargs):
        return {"ContentLength": self.object_size, "Metadata": self.object_metadata}


class FailingS3(FakeS3):
    def upload_part(self, **kwargs):
        raise RuntimeError("network down")


class FailingOnceS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.should_fail = True

    def upload_part(self, **kwargs):
        if kwargs["PartNumber"] == 2 and self.should_fail:
            self.should_fail = False
            raise RuntimeError("temporary outage")
        return super().upload_part(**kwargs)


class CompletionLostS3(FakeS3):
    def complete_multipart_upload(self, **kwargs):
        super().complete_multipart_upload(**kwargs)
        raise RuntimeError("response lost")


class PutResponseLostS3(FakeS3):
    def put_object(self, **kwargs):
        super().put_object(**kwargs)
        raise RuntimeError("put response lost")


class MutatingS3(FakeS3):
    def __init__(self, source):
        super().__init__()
        self.source = source
        self.mutated = False

    def upload_part(self, **kwargs):
        response = super().upload_part(**kwargs)
        if not self.mutated:
            with self.source.open("ab") as stream:
                stream.write(b"changed")
            self.mutated = True
        return response


class AccessDeniedError(Exception):
    response = {
        "Error": {"Code": "AccessDenied"},
        "ResponseMetadata": {"HTTPStatusCode": 403},
    }


class PreconditionFailedError(Exception):
    response = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class NoSuchUploadError(Exception):
    response = {
        "Error": {"Code": "NoSuchUpload"},
        "ResponseMetadata": {"HTTPStatusCode": 404},
    }


class DestinationPreconditionS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.complete_attempts = 0
        self.object_size = 1
        self.object_metadata = {}

    def complete_multipart_upload(self, **kwargs):
        self.complete_attempts += 1
        raise PreconditionFailedError("destination exists")


class CrashAfterCommitS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.upload_gone = False

    def list_parts(self, **kwargs):
        if self.upload_gone:
            raise NoSuchUploadError("upload committed")
        return super().list_parts(**kwargs)

    def complete_multipart_upload(self, **kwargs):
        super().complete_multipart_upload(**kwargs)
        self.upload_gone = True
        raise SystemExit("simulated process death after commit")


class AccessDeniedS3(FakeS3):
    def __init__(self):
        super().__init__()
        self.attempts = 0

    def upload_part(self, **kwargs):
        self.attempts += 1
        raise AccessDeniedError("denied")


def make_sparse_file(path: Path, size: int) -> Path:
    with path.open("wb") as stream:
        stream.truncate(size)
    return path


def test_parse_s3_uri():
    assert parse_s3_uri("s3://bucket/a%20file.bin") == ("bucket", "a file.bin")
    with pytest.raises(ValueError):
        parse_s3_uri("https://bucket/key")


def test_plan_bounds_concurrency_by_memory(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 21 * 1024**2)
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        max_concurrency=10,
        max_in_flight_bytes="10MiB",
    )
    plan = plan_file(source, "s3://bucket/key", config=config)
    assert plan.part_count == 5
    assert plan.concurrency == 2
    assert plan.initial_concurrency == 2
    assert plan.adaptive is False


def test_multipart_upload_completes_and_removes_checkpoint(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 11 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        max_concurrency=2,
        checkpoint_path=checkpoint,
    )
    client = FakeS3()
    progress = []
    result = send_file(
        source,
        "s3://bucket/key",
        config=config,
        client=client,
        progress=lambda done, total: progress.append((done, total)),
    )
    assert result.parts == 3
    assert set(client.uploaded) == {1, 2, 3}
    assert len(client.completed["MultipartUpload"]["Parts"]) == 3
    assert not checkpoint.exists()
    assert progress[-1] == (11 * 1024**2, 11 * 1024**2)


def test_small_upload_uses_put_object(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("hello", encoding="utf-8")
    client = FakeS3()
    result = send_file(
        source,
        "s3://bucket/key",
        client=client,
        config=MultipartConfig(if_none_match=True),
    )
    assert result.parts == 1
    assert client.put["Bucket"] == "bucket"
    assert client.put["IfNoneMatch"] == "*"
    assert result.metadata["destination_condition"] == {"IfNoneMatch": "*"}


def test_small_put_response_loss_is_recovered_by_transfer_identity(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("hello", encoding="utf-8")
    result = send_file(
        source,
        "s3://bucket/key",
        client=PutResponseLostS3(),
        config=MultipartConfig(if_none_match=True, max_attempts=1),
    )
    assert result.metadata["completion_recovered"] is True


def test_small_put_supports_etag_compare_and_swap(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("replacement", encoding="utf-8")
    client = FakeS3()
    result = send_file(
        source,
        "s3://bucket/key",
        client=client,
        config=MultipartConfig(if_match='"current-etag"'),
    )
    assert client.put["IfMatch"] == '"current-etag"'
    assert result.metadata["destination_condition"] == {
        "IfMatch": '"current-etag"'
    }


def test_non_resumable_failure_aborts_remote_upload(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        resume=False,
        max_attempts=1,
    )
    client = FailingS3()
    with pytest.raises(RuntimeError, match="network down"):
        send_file(source, "s3://bucket/key", config=config, client=client)
    assert client.aborted["UploadId"] == "upload-1"


def test_failed_upload_resumes_remote_parts(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 11 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        max_concurrency=3,
        checkpoint_path=checkpoint,
        max_attempts=1,
    )
    client = FailingOnceS3()

    with pytest.raises(RuntimeError, match="temporary outage"):
        send_file(source, "s3://bucket/key", config=config, client=client)
    assert checkpoint.exists()

    result = send_file(source, "s3://bucket/key", config=config, client=client)
    assert result.resumed is True
    assert result.parts == 3
    # Either successful in-flight part may finish before the sibling failure is
    # observed. Resume must reuse every remotely committed part, independent of
    # thread scheduling.
    assert 1 <= result.metadata["reused_parts"] <= 2
    assert not checkpoint.exists()


def test_reserved_create_args_are_rejected(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("hello", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Dude-managed"):
        send_file(
            source,
            "s3://bucket/key",
            client=FakeS3(),
            create_args={"Bucket": "other"},
        )


def test_corrupt_checkpoint_is_not_silently_discarded(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text("not json", encoding="utf-8")
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        checkpoint_path=checkpoint,
    )
    with pytest.raises(CheckpointError, match="corrupt"):
        send_file(source, "s3://bucket/key", config=config, client=FakeS3())


def test_matching_checkpoint_missing_transfer_identity_fails_closed(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 11 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        checkpoint_path=checkpoint,
        max_attempts=1,
    )
    client = FailingOnceS3()
    with pytest.raises(RuntimeError):
        send_file(source, "s3://bucket/key", config=config, client=client)
    state = json.loads(checkpoint.read_text(encoding="utf-8"))
    state.pop("transfer_id")
    checkpoint.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(CheckpointError, match="missing its upload identity"):
        send_file(source, "s3://bucket/key", config=config, client=client)


def test_completion_response_loss_is_recovered_by_transfer_identity(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        max_attempts=1,
        verify_after_upload=True,
        if_none_match=True,
    )
    result = send_file(source, "s3://bucket/key", config=config, client=CompletionLostS3())
    assert result.metadata["completion_recovered"] is True
    assert result.metadata["verified"] is True


def test_multipart_destination_precondition_discards_unusable_upload(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    client = DestinationPreconditionS3()
    with pytest.raises(PreconditionFailedError, match="destination exists"):
        send_file(
            source,
            "s3://bucket/key",
            client=client,
            config=MultipartConfig(
                multipart_threshold=1,
                part_size="5MiB",
                if_none_match=True,
                max_attempts=4,
                checkpoint_path=checkpoint,
            ),
        )
    assert client.complete_attempts == 1
    assert client.aborted["UploadId"] == "upload-1"
    assert not checkpoint.exists()


def test_restart_recovers_multipart_committed_before_checkpoint_cleanup(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    client = CrashAfterCommitS3()
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        if_none_match=True,
        max_attempts=1,
        checkpoint_path=checkpoint,
    )
    with pytest.raises(SystemExit, match="process death"):
        send_file(source, "s3://bucket/key", client=client, config=config)
    assert checkpoint.exists()

    result = send_file(source, "s3://bucket/key", client=client, config=config)
    assert result.resumed is True
    assert result.metadata["completion_recovered"] is True
    assert result.metadata["recovery_phase"] == "restart"
    assert result.throughput_mib_s is None
    assert not checkpoint.exists()


def test_destination_conditions_are_mutually_exclusive(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("hello", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="mutually exclusive"):
        send_file(
            source,
            "s3://bucket/key",
            client=FakeS3(),
            config=MultipartConfig(if_none_match=True, if_match='"etag"'),
        )


def test_source_mutation_aborts_uncommitted_upload(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 6 * 1024**2)
    checkpoint = tmp_path / "checkpoint.json"
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        checkpoint_path=checkpoint,
    )
    client = MutatingS3(source)
    with pytest.raises(SourceChangedError):
        send_file(source, "s3://bucket/key", config=config, client=client)
    assert client.aborted["UploadId"] == "upload-1"
    assert not checkpoint.exists()


def test_access_denied_is_not_retried(tmp_path):
    source = make_sparse_file(tmp_path / "large.bin", 5 * 1024**2)
    config = MultipartConfig(
        multipart_threshold=1,
        part_size="5MiB",
        max_attempts=4,
        resume=False,
    )
    client = AccessDeniedS3()
    with pytest.raises(AccessDeniedError):
        send_file(source, "s3://bucket/key", config=config, client=client)
    assert client.attempts == 1


def test_transfer_metadata_is_injected_without_overwriting_user_metadata(tmp_path):
    source = tmp_path / "small.txt"
    source.write_text("hello", encoding="utf-8")
    client = FakeS3()
    result = send_file(
        source,
        "s3://bucket/key",
        client=client,
        create_args={"Metadata": {"dataset": "diagnostics"}},
    )
    assert client.object_metadata["dataset"] == "diagnostics"
    assert client.object_metadata["dude-transfer-id"] == result.metadata["transfer_id"]
