import pytest
from test_compose import FakeComposeS3, source
from test_s3 import FakeS3, make_sparse_file

from dude_transfer.compose import ComposeConfig, compose_s3_objects
from dude_transfer.s3 import MultipartConfig, send_file
from dude_transfer.utils import MIB

botocore_validate = pytest.importorskip("botocore.validate")
botocore_session = pytest.importorskip("botocore.session")


def _validate(operation, parameters):
    model = botocore_session.get_session().get_service_model("s3")
    shape = model.operation_model(operation).input_shape
    botocore_validate.validate_parameters(parameters, shape)


def test_generated_multipart_requests_match_botocore_models(tmp_path):
    source = make_sparse_file(tmp_path / "data.bin", 6 * 1024**2)
    client = FakeS3()
    send_file(
        source,
        "s3://bucket/key",
        client=client,
        config=MultipartConfig(multipart_threshold=1, part_size="5MiB"),
        create_args={
            "ExpectedBucketOwner": "123456789012",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/test",
        },
    )

    _validate("CreateMultipartUpload", client.create_args)
    for parameters in client.uploaded.values():
        _validate("UploadPart", parameters)
    completion = dict(client.completed)
    _validate("CompleteMultipartUpload", completion)


def test_generated_multipart_copy_requests_match_botocore_models(tmp_path):
    client = FakeComposeS3(
        {
            ("source", "a"): source(6 * MIB, '"a"', "version-a"),
            ("source", "b"): source(MIB, '"b"'),
        }
    )
    compose_s3_objects(
        ["s3://source/a", "s3://source/b"],
        "s3://destination/result",
        client=client,
        config=ComposeConfig(
            checkpoint_path=tmp_path / "compose.json",
            expected_source_bucket_owner="123456789012",
            request_payer="requester",
        ),
        create_args={
            "ExpectedBucketOwner": "123456789012",
            "RequestPayer": "requester",
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/test",
        },
    )

    _validate("CreateMultipartUpload", client.create_args)
    for request in client.head_requests:
        _validate("HeadObject", request)
    for upload in client.uploaded.values():
        _validate("UploadPartCopy", upload["request"])
    _validate("CompleteMultipartUpload", dict(client.completed))
