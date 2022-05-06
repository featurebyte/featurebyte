"""
Sample code for CLI implementation
"""
from typing import Optional

from enum import Enum
from random import choice

import typer
from rich.console import Console

from featurebyte import version
from featurebyte.example import hello


class Color(str, Enum):
    """
    Enum for color options
    """

    WHITE = "white"
    RED = "red"
    CYAN = "cyan"
    MAGENTA = "magenta"
    YELLOW = "yellow"
    GREEN = "green"


app = typer.Typer(
    name="featurebyte",
    help="Python Library for FeatureOps",
    add_completion=False,
)
console = Console()


def version_callback(print_version: bool) -> None:
    """Print the version of the package.

    Parameters
    ----------
    print_version : bool
        Whether to print version

    Raises
    ------
    Exit
        If print_version is True
    """
    if print_version:
        console.print(f"[yellow]featurebyte[/] version: [bold blue]{version}[/]")
        raise typer.Exit()


@app.command(name="")
def main(
    name: str = typer.Option(..., help="Person to greet."),
    color: Optional[Color] = typer.Option(
        None,
        "-c",
        "--color",
        "--colour",
        case_sensitive=False,
        help="Color for print. If not specified then choice will be random.",
    ),
    print_version: bool = typer.Option(  # pylint: disable=W0613
        None,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Prints the version of the featurebyte package.",
    ),
) -> None:
    """Print a greeting with a giving name.

    Parameters
    ----------
    name : str
        Name to print
    color : Color
        Color to use
    print_version : bool
        Whether to print version
    """
    if color is None:
        color = choice(list(Color))

    greeting: str = hello(name)
    console.print(f"[bold {color}]{greeting}[/]")


if __name__ == "__main__":
    app()
