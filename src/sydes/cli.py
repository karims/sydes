from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from sydes.orchestrator.pipeline import run_analyze

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


@app.command()
def analyze(
    repo: str = typer.Argument(..., help="Path to the repo to analyze"),
    max_files: Optional[int] = typer.Option(None, help="Limit scanned files (debug)"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    if not repo_path.exists():
        raise typer.BadParameter(f"Repo path does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise typer.BadParameter(f"Repo path is not a directory: {repo_path}")

    result = run_analyze(repo_path, max_files=max_files)

    console.print(f"[bold green]sydes[/bold green] analyze: {repo_path}")
    console.print(
        f"Detected framework: [bold]{result.framework}[/bold] (confidence={result.confidence:.2f})"
    )
    console.print(f"Python files scanned: {result.files_scanned}")
    console.print(f"Candidate API files: {len(result.candidate_files)}")

    console.print("")
    console.print(f"Routes found: [bold]{len(result.routes)}[/bold]")
    for r in result.routes[:50]:
        console.print(f"  {r.method:<6} {r.path:<35} -> {r.handler_name}")
    if len(result.routes) > 50:
        console.print(f"  … and {len(result.routes) - 50} more")

@app.command()
def ping() -> None:
    console.print("pong")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
