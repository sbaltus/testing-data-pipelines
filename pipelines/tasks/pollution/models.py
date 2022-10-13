import dataclasses


@dataclasses.dataclass
class PollutionComponents:
    co: float
    no: float
    no2: float
    o3: float
    so2: float
    pm2_5: float
    pm10: float
    nh3: float


@dataclasses.dataclass
class AirQuality:
    timestamp: int
    main: dict[str, int | float]
    components: PollutionComponents


@dataclasses.dataclass
class PollutionForPoint:
    department_id: int
    centroid: list[int | float]
    values: AirQuality

    def to_dict(self):
        return {
            "department_id": self.department_id,
            "centroid": self.centroid,
            "aqi": self.values.main["aqi"],
            **self.values.components,
        }


@dataclasses.dataclass
class AirQualityFromOpenWeather:
    dt: int
    main: dict[str, int | float]
    components: PollutionComponents


@dataclasses.dataclass
class PollutionFromOpenWeather:
    coord: list[int | float]
    list: list[AirQualityFromOpenWeather]
