from typing import Optional

import typer
from rich.console import Console

from src.logging_conf import setup_logging
from src.process.processor import Processor
from src.settings import config

app = typer.Typer(help="File Loader CLI - Process files in parallel")
console = Console()


@app.command()
def process(
    file: Optional[str] = typer.Option(
        None,
        "--file",
        "-f",
        help="File to process (including file extension)",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    ),
) -> None:
    if log_level:
        config.LOG_LEVEL = log_level.upper()
        console.print(f"[green]Log level set to:[/green] {log_level.upper()}")

    setup_logging()
    processor = Processor()
    console.print(f"[green]Processing files from:[/green] {config.DIRECTORY_PATH}")
    if file:
        processor.process_file(file)
    else:
        processor.process_files_in_parallel()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
