from datetime import date
from io import StringIO

from tests import TEST_DATA_DIR

import pytest

from pipelines import DEFAULT_AWS_REGION, DEFAULT_S3_BUCKET
from pipelines.tasks.departments.population_processor import PopulationProcessor

VALID_CSV_FILE_PATH = f"{TEST_DATA_DIR}/demographyref-france-departement.csv"
CSV_HEADER = "Code Officiel Région;Nom Officiel Région;Code Officiel Département;Nom Officiel Département;Nombre d’arrondissements départementaux;Nombre de cantons;Population municipale Département;Population totale Département;Année de recensement;Année d’entrée en vigueur;Année de référence géographique"
VALID_CSV_CONTENT = f"""{CSV_HEADER}
52;Pays de la Loire;44;Loire-Atlantique;3;31;1412502.0;1441302.0;2018;2021;2020
11;Île-de-France;78;Yvelines;4;21;1441398.0;1466448.0;2018;2021;2020
84;Auvergne-Rhône-Alpes;74;Haute-Savoie;4;17;816699.0;838044.0;2018;2021;2020
52;Pays de la Loire;44;Loire-Atlantique;3;31;1394909.0;1423152.0;2017;2020;2019
84;Auvergne-Rhône-Alpes;74;Haute-Savoie;4;17;793938.0;816748.0;2015;2018;2017
11;Île-de-France;78;Yvelines;4;21;1427291.0;1454532.0;2015;2018;2017
"""


def test_population_processor_is_instantiated(s3):
    pop = PopulationProcessor(s3)
    pop.init("fake_path.csv", date.today())
    assert pop


@pytest.mark.parametrize(
    "csv_source",
    [
        StringIO(""),
        StringIO(CSV_HEADER),
        f"{TEST_DATA_DIR}/empty-file",
        f"{TEST_DATA_DIR}/demographyref-france-departement-empty.csv",
    ],
)
def test_population_processor_should_raise_if_empty_csv(s3, csv_source):
    pop = PopulationProcessor(s3)
    assert pop
    with pytest.raises(ValueError):
        pop.init(csv_source, date.today())
        pop.population

    pop = PopulationProcessor(s3)
    with pytest.raises(ValueError):
        pop.run(csv_source, date.today())


@pytest.mark.parametrize(
    "csv_source",
    [
        StringIO(VALID_CSV_CONTENT),
        VALID_CSV_FILE_PATH,
    ],
)
def test_should_read_csv_properly(s3, csv_source):
    pop = PopulationProcessor(s3)
    pop.init(csv_source, date.today())
    assert not pop.population.empty
    assert pop.population.shape[0] > 0


@pytest.mark.parametrize(
    "csv_source",
    [
        StringIO(VALID_CSV_CONTENT),
        VALID_CSV_FILE_PATH,
    ],
)
def test_should_only_keep_latest_rows(s3, csv_source):
    pop = PopulationProcessor(s3)
    pop.init(csv_source, date.today())
    init_lenght = pop.population.shape[0]
    pop.latest_infos()
    assert pop.population.shape[0] < init_lenght


@pytest.mark.parametrize(
    "csv_source",
    [
        StringIO(VALID_CSV_CONTENT),
        VALID_CSV_FILE_PATH,
        f"{TEST_DATA_DIR}/demographyref-france-departement-match.csv",
    ],
)
def test_population_processor_stores_on_s3(csv_source, s3):
    s3.create_bucket(
        Bucket=DEFAULT_S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": DEFAULT_AWS_REGION},
    )
    processing_date = date(2022, 11, 3)
    pop = PopulationProcessor(s3)
    bucket, key = pop.run(csv_source, processing_date)
    assert bucket == DEFAULT_S3_BUCKET
    assert key == "departments_data/population/20221103.parquet"
    response = s3.get_object(Bucket=bucket, Key=key)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
