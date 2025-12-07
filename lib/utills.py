import logging
from rich.logging import RichHandler
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql.types import (  # pyright: ignore
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.DEBUG, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
)


def get_logger(logger_name: str) -> logging.Logger:
    return logging.getLogger(logger_name)


@dataclass
class ProjectConfiguration:
    wiki_dump_path: str
    crawled_pages_path: str
    parsed_pages_tsv_path: str
    complete_data_tsv_path: str
    index_path: str


@dataclass
class DriverSearchResult:
    result_score: float | None
    filename: str | None
    website_url: str | None
    driver_name: str | None
    nationality: str | None
    series: str | None
    age: int | None
    birthday: datetime | None
    hometown: str | None
    races_started: int | None
    races_entered: int | None
    wins: int | None
    podiums: int | None
    pole_positions: int | None
    fastest_laps: int | None
    race_win_percentage: float | None
    podium_percentage: float | None
    driverdb_score: int | None
    current_team: str | None
    all_teams: list[str] | None
    car_number: int | None
    championships: int | None
    driver_description: str | None
    series_description: str | None
    team_description: str | None


WIKI_SCHEMA = StructType(
    [
        StructField("title", StringType(), True),
        StructField("ns", IntegerType(), True),
        StructField("id", LongType(), True),
        StructField(
            "redirect", StructType(
                [StructField("_title", StringType(), True)]), True
        ),
        StructField(
            "revision",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("timestamp", StringType(), True),
                        StructField(
                            "contributor",
                            StructType(
                                [
                                    StructField(
                                        "username", StringType(), True),
                                    StructField("ip", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "text",
                            StructType(
                                [
                                    StructField("_VALUE", StringType(), True),
                                    StructField("_bytes", LongType(), True),
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
    ]
)
