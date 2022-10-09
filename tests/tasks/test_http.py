import pytest
from requests import HTTPError

from pipelines import DEFAULT_AWS_REGION, DEFAULT_S3_BUCKET
from pipelines.tasks.http import download_and_store_to_s3


def test_download_and_store_to_s3_should_raise_if_error_code(requests_mock, s3):
    requests_mock.get("https://foo", text="ok", status_code=400)
    with pytest.raises(HTTPError):
        download_and_store_to_s3("https://foo", "labas", s3)


def test_download_and_store_to_s3_should_raise_if_empty(requests_mock, s3):
    requests_mock.get("https://foo", content=b"", status_code=200)
    with pytest.raises(ValueError):
        download_and_store_to_s3("https://foo", "labas", s3)


def test_download_and_store_to_s3_should_store_file_on_s3(requests_mock, s3):
    s3.create_bucket(
        Bucket=DEFAULT_S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": DEFAULT_AWS_REGION},
    )

    requests_mock.get("https://foo", content=b"1,2,3", status_code=200)
    assert download_and_store_to_s3("https://foo", "holamundo.csv", s3)
