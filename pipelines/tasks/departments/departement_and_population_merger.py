from datetime import datetime
from io import StringIO
from typing import Any

import geopandas
import pandas
import sqlalchemy.engine
from sqlalchemy import text

from pipelines import DEFAULT_S3_BUCKET
from pipelines.tasks.departments.processor import Processor


class DepartmentAndPopulationMerger(Processor):
    """Responsible to merge and persist population and departments data."""

    def __init__(
        self,
        s3_client: Any | None = None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
        sql_engine: sqlalchemy.engine.Engine | None = None,
    ):
        """Creates the Processor object."""
        super().__init__(s3_client, s3_bucket)
        self.sql_engine = sql_engine

        self.department_source: str | StringIO | None = None
        self.population_source: str | StringIO | None = None
        self.processing_date: datetime.date | None = None

        self._departments: geopandas.GeoDataFrame | None = None
        self._population: pandas.DataFrame | None = None
        self._merged: geopandas.GeoDataFrame | None = None

    # pylint: disable=arguments-differ
    def init(self, processing_date: datetime.date, **kwargs):
        """Initialize the main fields."""
        self.department_source = kwargs.get("department_source")
        self.population_source = kwargs.get("population_source")
        self.processing_date = processing_date

    @property
    def population(self):
        """Population dataframe setter."""
        if self._population is None:
            self._population = self._read_or_raise(self.population_source)
        return self._population

    @property
    def departments(self):
        """Departments geodataframe setter."""
        if self._departments is None:
            df = geopandas.read_parquet(self.department_source)
            if df.shape[0] == 0:
                raise ValueError("cannot process empty file.")
            self._departments = df
        return self._departments

    @staticmethod
    def _read_or_raise(source: str | StringIO):
        df = pandas.read_parquet(source)
        if df.shape[0] == 0:
            raise ValueError("cannot process empty file.")
        return df

    @property
    def merged(self):
        """The resulting geodataframe after department and population are merged."""
        if self._merged is None:
            self._merged = self.merge()
        return self._merged

    def merge(self):
        """Joins departments and population data."""
        merged = self.departments.merge(
            self.population,
            how="inner",
            on="Code Officiel Département",
            suffixes=("", "_pop"),
        )
        merged.drop(
            [col for col in merged.columns if col.endswith("_pop")],
            axis=1,
            inplace=True,
        )
        if merged.shape[0] == 0:
            raise ValueError("cannot process empty file.")
        return merged

    # pylint: disable=arguments-differ,unused-argument
    def store(self, **kwargs):
        """Persists merged data to database."""
        self.merged["Population totale Département"] = self.merged[
            "Population totale Département"
        ].values.astype(int)
        census_year = int(self.merged["Année de recensement"].unique()[0])
        query = text(
            """DELETE FROM watcher.departments_info WHERE census_year =:census_year"""
        ).bindparams(census_year=census_year)
        self.sql_engine.execute(query)

        self.merged.drop(
            [
                "Année",
                "Code Officiel Courant Département",
                "Nom Officiel Département Majuscule",
                "Nom Officiel Département Minuscule",
                "Code Iso 3166-3 Zone",
                "viewport",
                "Population municipale Département",
                "Est une CTU",
            ],
            axis=1,
            inplace=True,
        )
        self.merged.reset_index(drop=True, inplace=True)
        self.merged.rename(
            columns={
                "Code Officiel Région": "region_code",
                "Nom Officiel Région": "region_name",
                "Code Officiel Département": "department_code",
                "Nom Officiel Département": "department_name",
                "Type": "department_type",
                "Statut Département": "department_status",
                "SIREN": "siren",
                "Population totale Département": "population",
                "Année de recensement": "census_year",
            },
            inplace=True,
        )

        self.merged.to_postgis(
            "departments_info", self.sql_engine, schema="watcher", if_exists="append"
        )

    # pylint: disable=arguments-differ
    def run(self, processing_date: datetime.date, **kwargs):
        """Orchestrates processors tasks."""
        self.init(processing_date, **kwargs)
        self.merge()
        self.store()
