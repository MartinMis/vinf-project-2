from dataclasses import dataclass
from datetime import datetime

@dataclass
class DriverSearchResult:
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