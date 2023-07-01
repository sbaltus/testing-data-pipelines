import pytest
from requests import HTTPError

from pipelines import DEFAULT_AWS_REGION, DEFAULT_S3_BUCKET
from pipelines.tasks.http import (
    download_and_store_to_s3,
    get_pollution_data_for_point,
)


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


open_weather_output = """{
  "coord":[
    50,
    50
  ],
  "list":[
    {
      "dt":1605182400,
      "main":{
        "aqi":1
      },
      "components":{
        "co":201.94053649902344,
        "no":0.01877197064459324,
        "no2":0.7711350917816162,
        "o3":68.66455078125,
        "so2":0.6407499313354492,
        "pm2_5":0.5,
        "pm10":0.540438711643219,
        "nh3":0.12369127571582794
      }
    }
  ]
}"""

url = "https://api.openweathermap.org/data/2.5/air_pollution/history?lat=13.4134995&lon=45.792650&start=86935946&end=86935946&appid=None"


def test_should_transform_response_to_pollution_data(requests_mock, faker):
    requests_mock.get(
        url=url, content=open_weather_output.encode("utf-8"), status_code=200
    )
    p = get_pollution_data_for_point(
        faker.latitude(), faker.longitude(), faker.date_time(), 1
    )
    assert p


@pytest.mark.parametrize("status_code,exception", [(200, ValueError), (400, HTTPError)])
def test_should_raise_if_error(requests_mock, faker, status_code, exception):
    requests_mock.get(url=url, content=b"{}", status_code=status_code)
    with pytest.raises(exception):
        get_pollution_data_for_point(
            faker.latitude(), faker.longitude(), faker.date_time(), 1
        )
