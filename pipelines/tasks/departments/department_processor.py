import datetime
import json
from io import BytesIO, StringIO

import boto3
import botocore.client
import geopandas
import pandas
from shapely import geometry

from pipelines import DEFAULT_S3_BUCKET


class DepartmentProcessor:
    """Reads Departments CSV file and enriches it."""

    def __init__(
        self,
        s3_client: botocore.client.BaseClient | None = None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
    ):
        """Creates the DepartmentProcessor object."""
        self.s3_client = s3_client
        self.s3_bucket = s3_bucket
        self.department_source: str | StringIO | None = None
        self.processing_date: datetime.date | None = None
        self._departments: geopandas.GeoDataFrame | None = None

    def _set_s3_client(self):
        if not self.s3_client:
            self.s3_client = boto3.client("s3")

    @property
    def departments(self):
        """Departments geodataframe setter."""
        if self._departments is None:
            self._departments = self._csv_to_geodataframe()
        return self._departments

    def _csv_to_geodataframe(self):
        df = pandas.read_csv(self.department_source, sep=";", header=0)

        if df.shape[0] == 0:
            raise ValueError("Cannot process empty file")
        df["geometry"] = df["Geo Shape"].apply(json.loads).apply(geometry.shape)
        df["processing_date"] = self.processing_date
        gdf = geopandas.GeoDataFrame(df, crs="epsg:4326")
        gdf.drop(columns=["Geo Point", "Geo Shape"], axis=1, inplace=True)
        return gdf

    def add_centroid(self):
        """Adds the centroid (or barycentre) to the dataframe."""
        self.departments["centroid"] = self.departments["geometry"].centroid

    def store(self):
        """Persists the departments object."""
        s3_target = f"departments/{self.processing_date.strftime('%Y%m%d')}.parquet"
        buffer = BytesIO()
        self.departments.to_parquet(buffer, compression="gzip")
        self.s3_client.put_object(
            Bucket=self.s3_bucket, Key=s3_target, Body=buffer.getvalue()
        )

        return self.s3_bucket, s3_target

    def init(self, department_source: str | StringIO, processing_date: datetime.date):
        """Initialize the main fields."""
        self.department_source = department_source
        self.processing_date = processing_date

    def run(self, department_source: str | StringIO, processing_date: datetime.date):
        """Orchestrates the department processing."""
        self.init(department_source, processing_date)
        self._set_s3_client()
        self.add_centroid()
        return self.store()
