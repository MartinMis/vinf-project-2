from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.text import Text

from typing import Any
from datetime import datetime

from utills import DriverSearchResult

console = Console()

def print_header() -> None:
    console.print(
        Panel(
            "Racing Driver Search"
        ),
        justify="center"
    )


def print_mode_menu() -> str:
    console.print(
        Panel(
            "- [bold]1[/bold] - Parse data\n"
            "- [bold]2[/bold] - Index data\n"
            "[green]- [bold]3[/bold] - Search[/green]\n"
            "- [bold]4[/bold] - Close",
            title="[bold]Operation modes[/bold]",
            title_align="left"
        )
    )
    return Prompt.ask("Selected mode: ", choices=["1", "2", "3", "4"])


def formated_value(value: str | int | float | datetime | list[str] | None):
    if value is None:
        return Text("Not specified", style="red")
    elif isinstance(value, str):
        return Text(value)
    elif isinstance(value, int) or isinstance(value, float):
        return Text(str(value))
    elif isinstance(value, datetime):
        return Text(value.strftime("%d.%m.%Y"))
    elif isinstance(value, list):
        for elem in value:
            if not isinstance(elem, str):
                raise AttributeError("List should contain only strings!")
        return Text(", ".join(value))
    else:
        raise AttributeError("Unknown value supplied")


def create_driver_table_long(search_result: DriverSearchResult) -> Table: 
    table = Table(title="Search result", style="green", show_header=False) 

    table.add_row("Name", formated_value(search_result.driver_name))
    table.add_row("Nationality", formated_value(search_result.nationality))
    table.add_row("Series", formated_value(search_result.series))
    table.add_row("Age", formated_value(search_result.age))
    table.add_row("Birthday", formated_value(search_result.birthday))
    table.add_row("Hometown", formated_value(search_result.hometown))
    table.add_section()
    table.add_row("Races started", formated_value(search_result.races_started))
    table.add_row("Races entered", formated_value(search_result.races_entered))
    table.add_row("Wins", formated_value(search_result.wins))
    table.add_row("Podiums", formated_value(search_result.podiums))
    table.add_row("Pole positions", formated_value(search_result.pole_positions))
    table.add_row("Fastest laps", formated_value(search_result.fastest_laps))
    table.add_row("Race win percentage", formated_value(search_result.race_win_percentage))
    table.add_row("Podium percentage", formated_value(search_result.podium_percentage))
    table.add_row("DriverDB score", formated_value(search_result.driverdb_score))
    table.add_section()
    table.add_row("Current team", formated_value(search_result.current_team))
    table.add_row("All teams", formated_value(search_result.all_teams))
    table.add_row("Car number", formated_value(search_result.car_number))
    table.add_row("Championship wins", formated_value(search_result.championships))
    table.add_row("Driver description", formated_value(search_result.driver_description))
    table.add_row("Series description", formated_value(search_result.series_description))
    table.add_row("Team description", formated_value(search_result.team_description))
    table.add_section()
    table.add_row("URL", formated_value(search_result.website_url))

    return table

def print_results_list(results_list: list[DriverSearchResult]):
    # Guard statement for variable type
    if not isinstance(results_list, list):
        raise AttributeError("Invalid attribute, must be a list!")
    # Guard statement for types of list memebers
    for result in results_list:
        if not isinstance(result, DriverSearchResult):
            raise AttributeError("List can only contain DriverSearchResults!")
    
    for result in results_list:
        console.print(create_driver_table_long(result))


if __name__ == "__main__":
    print_header()
    print_mode_menu()
    
    test_driver = DriverSearchResult(
        filename="/root/file.html",
        website_url="www.driverdb.com",
        driver_name="Test Testovski",
        nationality="Bhutanese",
        series="Ultimate racing truck",
        age=589,
        birthday=datetime(1873, 12, 12),
        hometown="Prievidza",
        races_entered=5,
        races_started=6,
        wins=7,
        podiums=8,
        pole_positions=99,
        fastest_laps=6,
        race_win_percentage=128.5,
        podium_percentage=999.9,
        driverdb_score=5000,
        current_team=None,
        all_teams=["Very Fast Racing", "Mildly Slow Racing", "Ferrari"],
        car_number=1,
        championships=6,
        driver_description="Just a chill guy",
        series_description="Car go brrr",
        team_description="Such win much wow"
    )
    print_results_list([test_driver, test_driver])