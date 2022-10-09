import abc
import datetime
from io import BytesIO, StringIO
from typing import Any

import boto3
import geopandas
import pandas

from pipelines import DEFAULT_S3_BUCKET


class Processor:
    """Common base for Processor tasks."""

    def __init__(
        self,
        s3_client: Any | None = None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
    ):
        """Creates the Processor object."""
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.source: str | StringIO | None = None
        self.processing_date: datetime.date | None = None

    def _set_s3_client(self):
        if not self.s3_client:
            self.s3_client = boto3.client("s3")

    @abc.abstractmethod
    def run(self, source: str | StringIO, processing_date: datetime.date):
        """Orchestrator."""

    def init(self, source: str | StringIO, processing_date: datetime.date):
        """Initialize the main fields."""
        self.source = source
        self.processing_date = processing_date

    def store(self, context: str, content: pandas.DataFrame | geopandas.GeoDataFrame):
        """Persists the departments object."""
        s3_target = f"departments_data/{context}/{self.processing_date.strftime('%Y%m%d')}.parquet"
        buffer = BytesIO()
        content.to_parquet(buffer, compression="gzip")
        self.s3_client.put_object(
            Bucket=self.s3_bucket, Key=s3_target, Body=buffer.getvalue()
        )

        return self.s3_bucket, s3_target
