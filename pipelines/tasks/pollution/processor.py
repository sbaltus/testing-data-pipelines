import datetime

import pandas
import sqlalchemy
from sqlalchemy import text

from pipelines.tasks.http import get_pollution_data_for_point
from pipelines.tasks.pollution.models import PollutionForPoint


class PollutionProcessor:
    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        start_date: datetime.date,
        sql_engine: sqlalchemy.engine.Engine | None = None,
    ):
        self.departments: list[tuple[int, tuple[float, float]]] | None = None
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.start_date = datetime.datetime.fromordinal(start_date.toordinal())
        self.sql_engine = sql_engine

    def persist_on_object_storage(self, pollution: PollutionForPoint):
        df = pandas.DataFrame.from_dict(pollution.to_dict())
        df.to_parquet(
            f"s3://{self.s3_bucket}/{self.s3_prefix}/{pollution.department_id}.parquet"
        )

    def load(self):
        df = pandas.read_parquet(f"s3://{self.s3_bucket}/{self.s3_prefix}/")
        self.sql_engine.execute(
            text(
                "DELETE FROM watcher.pollution "
                "WHERE processing_date = :processing_date"
            ).bindparams(processing_date=self.start_date)
        )
        df.to_sql("pollution", self.sql_engine, if_exists="append", schema="watcher")

    def run(self, departments):
        self.departments = departments
        for department_id, coord in self.departments:
            pollution = get_pollution_data_for_point(
                department_id, coord[0], coord[1], self.start_date
            )
            self.persist_on_object_storage(pollution)
        self.load()
