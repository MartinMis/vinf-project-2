from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

from datetime import datetime

from .utills import DriverSearchResult

console = Console()


def print_header() -> None:
    console.print(Panel("Racing Driver Search"), justify="center")


def print_mode_menu() -> str:
    console.print(
        Panel(
            "- [bold]1[/bold] - Parse data\n"
            "- [bold]2[/bold] - Index data\n"
            "[green]- [bold]3[/bold] - Search[/green]\n"
            "- [bold]4[/bold] - Close",
            title="[bold]Operation modes[/bold]",
            title_align="left",
        )
    )
    return Prompt.ask("Selected mode: ", choices=["1", "2", "3", "4"])


def formated_value(value: str | int | float | datetime | list[str] | None):
    if value is None:
        return Text("Not specified", style="red")
    elif isinstance(value, str):
        return Text(clean_up_text(value))
    elif isinstance(value, int) or isinstance(value, float):
        return Text(str(value))
    elif isinstance(value, datetime):
        return Text(value.strftime("%d.%m.%Y"))
    elif isinstance(value, list):
        for elem in value:
            if not isinstance(elem, str):
                raise AttributeError("List should contain only strings!")
        return Text(clean_up_text(", ".join(value)))


def create_driver_table_long(search_result: DriverSearchResult) -> Table:
    table = Table(
        title=f"Search result score: {search_result.result_score}",
        style="green",
        show_header=False,
    )

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
    table.add_row(
        "Race win percentage", formated_value(search_result.race_win_percentage)
    )
    table.add_row("Podium percentage", formated_value(search_result.podium_percentage))
    table.add_row("DriverDB score", formated_value(search_result.driverdb_score))
    table.add_section()
    table.add_row("Current team", formated_value(search_result.current_team))
    table.add_row("All teams", formated_value(search_result.all_teams))
    table.add_row("Car number", formated_value(search_result.car_number))
    table.add_row("Championship wins", formated_value(search_result.championships))
    table.add_row(
        "Driver description", formated_value(search_result.driver_description)
    )
    table.add_row(
        "Series description", formated_value(search_result.series_description)
    )
    table.add_row("Team description", formated_value(search_result.team_description))
    table.add_section()
    table.add_row("URL", formated_value(search_result.website_url))

    return table


def create_driver_table_wide(results: list[DriverSearchResult]):
    table = Table(
        title="Search results",
        show_lines=True,
    )
    table.add_column("Score")
    table.add_column("Name")
    table.add_column("Nationality")
    table.add_column("Series")
    table.add_column("Age")
    table.add_column("Birthday")
    table.add_column("Hometown")
    table.add_column("Races started")
    table.add_column("Races entered")
    table.add_column("wins")
    table.add_column("Podiums")
    table.add_column("Pole positions")
    table.add_column("Fastest laps")
    table.add_column("Race win percentage")
    table.add_column("Podium percentage")
    table.add_column("DriverDB score")
    table.add_column("Current team")
    table.add_column("All teams")
    table.add_column("Car number")
    table.add_column("Championship wins")

    for result in results:
        table.add_row(
            formated_value(result.result_score),
            formated_value(result.driver_name),
            formated_value(result.nationality),
            formated_value(result.series),
            formated_value(result.age),
            formated_value(result.birthday),
            formated_value(result.hometown),
            formated_value(result.races_started),
            formated_value(result.races_entered),
            formated_value(result.wins),
            formated_value(result.podiums),
            formated_value(result.pole_positions),
            formated_value(result.fastest_laps),
            formated_value(result.race_win_percentage),
            formated_value(result.podium_percentage),
            formated_value(result.driverdb_score),
            formated_value(result.current_team),
            formated_value(result.all_teams),
            formated_value(result.car_number),
            formated_value(result.championships),
        )
    return table


def clean_up_text(text: str) -> str:
    return text.replace("[", "").replace("]", "").replace("'''", "")


def print_results_list(results_list: list[DriverSearchResult]):
    # Guard statement for variable type
    if not isinstance(results_list, list):
        raise AttributeError("Invalid attribute, must be a list!")
    # Guard statement for types of list memebers
    for result in results_list:
        if not isinstance(result, DriverSearchResult):
            raise AttributeError("List can only contain DriverSearchResults!")

    console.print(create_driver_table_wide(results_list))
