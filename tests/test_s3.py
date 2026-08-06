from pathlib import Path

import pytest

from dude_transfer.s3 import MultipartConfig, parse_s3_uri, plan_file, send_file


class FakeS3:
    def __init__(self):
        self.uploaded = {}
        self.completed = None
        self.put = None
        self.aborted = None

    def put_object(self, **kwargs):
        self.put = kwargs
        return {"ETag": '"small"'}

    def create_multipart_upload(self, **kwargs):
        self.create_args = kwargs
        return {"UploadId": "upload-1"}

    def list_parts(self, **kwargs):
        parts = []
        for number, upload in sorted(self.uploaded.items()):
            part = {"PartNumber": number, "ETag": f'"part-{number}"'}
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
        return {"ETag": '"complete"'}

    def abort_multipart_upload(self, **kwargs):
        self.aborted = kwargs


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
    result = send_file(source, "s3://bucket/key", client=client)
    assert result.parts == 1
    assert client.put["Bucket"] == "bucket"


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
    assert not checkpoint.exists()
