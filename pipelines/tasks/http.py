import datetime

import boto3
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient

from pipelines import DEFAULT_S3_BUCKET, OPENWEATHER_API_KEY
from pipelines.tasks.pollution.models import (
    AirQuality,
    AirQualityFromOpenWeather,
    PollutionForPoint,
    PollutionFromOpenWeather,
)


def download_and_store_to_s3(
    url: str,
    filename: str,
    s3: BaseClient | None = None,
    bucket=DEFAULT_S3_BUCKET,
    s3_hook: S3Hook | None = None,
) -> str:
    """Download URL content and store it on s3."""
    response = requests.get(url, timeout=2)
    response.raise_for_status()

    if len(response.content) == 0:
        raise ValueError("No data to save")

    if not s3:
        s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=filename, Body=response.content)
    return f"s3://{bucket}/{filename}"


def get_pollution_data_for_point(
    lat: float,
    long: float,
    start_date: datetime.datetime,
    department_id: int,
) -> PollutionForPoint:
    start = int(start_date.timestamp())
    url = f"https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={long}&start={start}" \
          f"&end={start}&appid={OPENWEATHER_API_KEY}"
    response = requests.get(url, timeout=2)
    response.raise_for_status()
    content = response.json()
    if len(content["list"]) != 1:
        raise ValueError("unprocessable payload")

    poll = PollutionFromOpenWeather(
        coord=content["coord"], list=[AirQualityFromOpenWeather(**content["list"][0])]
    )

    return PollutionForPoint(
        department_id=department_id,
        centroid=[lat, long],
        values=AirQuality(
            timestamp=poll.list[0].dt,
            main=poll.list[0].main,
            components=poll.list[0].components,
        ),
    )
