import logging
import threading
import time
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

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
    directory_path: Optional[str] = typer.Option(
        None,
        "--directory-path",
        "-d",
        help="Directory to process",
    ),
    archive_path: Optional[str] = typer.Option(
        None,
        "--archive-path",
        "-a",
        help="Archive directory",
    ),
    duplicate_files_path: Optional[str] = typer.Option(
        None,
        "--duplicate-files-path",
        "-dfp",
        help="Duplicate files directory",
    ),
) -> None:
    config.LOG_LEVEL = "WARNING"

    setup_logging()

    root_logger = logging.getLogger("src")
    for handler in root_logger.handlers:
        if isinstance(handler, RichHandler):
            handler.console = console
            handler.show_time = False
            handler.show_path = False

    processor = Processor(
        directory_path=directory_path,
        archive_path=archive_path,
        duplicate_files_path=duplicate_files_path,
    )
    console.print(f"[green]Processing files from:[/green] {config.DIRECTORY_PATH}")

    if file:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f"[cyan]Processing {file}...", total=None)
            processor.process_file(file)
            progress.update(task, description="[green]✓ Complete!")
    else:
        console.print("[green]Gathering files to process...[/green]")
        total_files = processor.file_paths_queue.qsize()

        if total_files == 0:
            console.print("[yellow]No files found to process[/yellow]")
            return

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total} files)"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            redirect_stderr=True,
            redirect_stdout=False,
        ) as progress:
            task = progress.add_task(
                "[cyan]Processing files...",
                total=total_files,
            )
            initial_results_count = len(processor.results)

            processing_done = threading.Event()

            def process_files():
                processor.process_files_in_parallel()
                processing_done.set()

            thread = threading.Thread(target=process_files, daemon=True)
            thread.start()

            while (
                not processing_done.is_set()
                or len(processor.results) < initial_results_count + total_files
            ):
                completed = len(processor.results) - initial_results_count
                progress.update(task, completed=completed)
                time.sleep(0.1)

            completed = len(processor.results) - initial_results_count
            progress.update(
                task,
                completed=completed,
                description="[green]✓ All files processed",
            )
            thread.join()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
