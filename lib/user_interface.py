from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm

console = Console()

def print_header() -> None:
    console.print(
        Panel(
            "Racing Driver Search"
        ),
        justify="center"
    )


def print_mode_menu() -> None:
    console.print(
        Panel(
            "- [bold]1[/bold] - Parse data\n"
            "- 2 Index data\n",
            title="[bold]Operation modes[/bold]",
            title_align="left"
        )
    )
    


if __name__ == "__main__":
    print_header()
    print_mode_menu()