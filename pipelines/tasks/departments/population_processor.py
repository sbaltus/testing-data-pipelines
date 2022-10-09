import datetime
from io import StringIO
from typing import Any

import geopandas
import pandas

from pipelines import DEFAULT_S3_BUCKET
from pipelines.tasks.departments.processor import Processor


class PopulationProcessor(Processor):
    """Reads Population CSV file and enriches it."""

    def __init__(
        self,
        s3_client: Any | None = None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
    ):
        """Creates the DepartmentProcessor object."""
        super().__init__(s3_client, s3_bucket)
        self._population: geopandas.GeoDataFrame | None = None

    @property
    def population(self):
        """Population dataframe setter."""
        if self._population is None:
            self._population = self._csv_to_dataframe()
        return self._population

    def _csv_to_dataframe(self):
        df = pandas.read_csv(self.source, sep=";", header=0)

        if df.shape[0] == 0:
            raise ValueError("Cannot process empty file")
        df.sort_values(
            by=[
                "Code Officiel Région",
                "Code Officiel Département",
                "Année de recensement",
            ],
            inplace=True,
        )
        df.drop(
            columns=[
                "Nombre d’arrondissements départementaux",
                "Nombre de cantons",
                "Année d’entrée en vigueur",
                "Année de référence géographique",
            ],
            inplace=True,
        )
        df["processing_date"] = self.processing_date
        return df

    def latest_infos(self):
        """The source file contains census infos for several years, here we only keep the latest."""
        self._population = self.population[
            (
                self.population["Année de recensement"]
                == self.population["Année de recensement"].max()
            )
        ]

    def run(self, source: str | StringIO, processing_date: datetime.date):
        """Orchestrates processor's tasks."""
        self.init(source, processing_date)
        self.latest_infos()
        self._set_s3_client()
        return self.store("population", self.population)
