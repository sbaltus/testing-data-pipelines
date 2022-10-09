import os

import boto3
import pytest
from moto.s3 import mock_s3

from pipelines import DEFAULT_AWS_REGION


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = DEFAULT_AWS_REGION


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name=DEFAULT_AWS_REGION)
