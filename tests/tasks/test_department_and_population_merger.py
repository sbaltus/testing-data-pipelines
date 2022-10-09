from datetime import date
from io import BytesIO

from tests import TEST_DATA_DIR

import pytest
from sqlalchemy import create_engine

from pipelines.tasks.departments.departement_and_population_merger import (
    DepartmentAndPopulationMerger,
)

VALID_DEPARTMENT_PARQUET_FILE_PATH = f"{TEST_DATA_DIR}/departments.parquet"
VALID_POPULATION_PARQUET_FILE_PATH = f"{TEST_DATA_DIR}/population.parquet"


def test_department_processor_is_instantiated(s3):
    dpm = DepartmentAndPopulationMerger(s3)
    assert dpm
    assert dpm.processing_date is None
    assert dpm.department_source is None
    assert dpm.population_source is None
    dpm.init(
        date.today(),
        department_source="source_1.parquet",
        population_source="source_2.parquet",
    )

    assert dpm.processing_date is not None
    assert dpm.department_source is not None
    assert dpm.population_source is not None


@pytest.mark.parametrize(
    "dpt_parquet_source,pop_parquet_source",
    [
        (BytesIO(b""), BytesIO(b"")),
        (f"{TEST_DATA_DIR}/empty-file", f"{TEST_DATA_DIR}/empty-file"),
    ],
)
def test_should_raise_if_no_data(dpt_parquet_source, pop_parquet_source, s3):
    dpm = DepartmentAndPopulationMerger(s3)
    dpm.init(
        date.today(),
        department_source=dpt_parquet_source,
        population_source=pop_parquet_source,
    )
    with pytest.raises(ValueError):
        dpm.population

    with pytest.raises(ValueError):
        dpm.departments


@pytest.mark.parametrize(
    "dpt_parquet_source,pop_parquet_source",
    [
        (
            f"{TEST_DATA_DIR}/departments.parquet",
            f"{TEST_DATA_DIR}/no-match-population.parquet",
        )
    ],
)
def test_should_raise_if_no_merge(dpt_parquet_source, pop_parquet_source, s3):
    dpm = DepartmentAndPopulationMerger(s3)
    dpm.init(
        date.today(),
        department_source=dpt_parquet_source,
        population_source=pop_parquet_source,
    )
    with pytest.raises(ValueError):
        dpm.merge()


def test_should_merge_properly(s3):
    dpm = DepartmentAndPopulationMerger(s3)
    dpm.init(
        date.today(),
        department_source=f"{TEST_DATA_DIR}/departments.parquet",
        population_source=f"{TEST_DATA_DIR}/population.parquet",
    )

    assert not dpm.merged.empty
    assert dpm.merged.shape[0] == 3


def test_should_store_merged():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(
        "watcher-postgres",
        user="postgres",
        password="password",
        dbname="watcher",
    ) as postgres:
        pg_url = postgres.get_connection_url()
        engine = create_engine(pg_url)
        dpm = DepartmentAndPopulationMerger(sql_engine=engine)
        dpm.run(
            date.today(),
            department_source=f"{TEST_DATA_DIR}/departments.parquet",
            population_source=f"{TEST_DATA_DIR}/population.parquet",
        )
