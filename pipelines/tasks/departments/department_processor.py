import datetime
import json
from io import StringIO
from typing import Any

import geopandas
import pandas
from shapely import geometry

from pipelines import DEFAULT_S3_BUCKET
from pipelines.tasks.departments.processor import Processor


class DepartmentProcessor(Processor):
    """Reads Departments CSV file and enriches it."""

    def __init__(
        self,
        s3_client: Any | None = None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
    ):
        """Creates the DepartmentProcessor object."""
        super().__init__(s3_client, s3_bucket)
        self._departments: geopandas.GeoDataFrame | None = None

    @property
    def departments(self):
        """Departments geodataframe setter."""
        if self._departments is None:
            self._departments = self._csv_to_geodataframe()
        return self._departments

    def _csv_to_geodataframe(self):
        df = pandas.read_csv(self.source, sep=";", header=0)

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

    def run(self, source: str | StringIO, processing_date: datetime.date):
        """Orchestrates processor's tasks."""
        self.init(source, processing_date)
        self.add_centroid()
        self._set_s3_client()
        return self.store("departments", self.departments)
