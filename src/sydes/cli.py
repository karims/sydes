from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import json
import typer
from rich.console import Console
from rich.table import Table

from sydes.orchestrator.pipeline import run_analyze
from sydes.store.sqlite_store import SydesSQLiteStore
from sydes.graph.builder import build_endpoint_graph


app = typer.Typer(no_args_is_help=True, add_completion=False)

endpoints_app = typer.Typer(no_args_is_help=True)
app.add_typer(endpoints_app, name="endpoints")

graph_app = typer.Typer(no_args_is_help=True)
app.add_typer(graph_app, name="graph")

scans_app = typer.Typer(no_args_is_help=True)
app.add_typer(scans_app, name="scans")

structure_app = typer.Typer(no_args_is_help=True)
app.add_typer(structure_app, name="structure")

console = Console()


@app.command()
def analyze(
    repo: str = typer.Argument(..., help="Path to the repo to analyze"),
    max_files: Optional[int] = typer.Option(None, help="Limit scanned files (debug)"),
    git: bool = typer.Option(False, help="Only scan files changed in git"),
    git_base: str = typer.Option("HEAD~1", help="Git base revision (used with --git)"),
    git_to: str = typer.Option("HEAD", help="Git target revision (used with --git)"),
) -> None:

    repo_path = Path(repo).expanduser().resolve()
    if not repo_path.exists():
        raise typer.BadParameter(f"Repo path does not exist: {repo_path}")
    if not repo_path.is_dir():
        raise typer.BadParameter(f"Repo path is not a directory: {repo_path}")

    result = run_analyze(
        repo_path,
        max_files=max_files,
        git_mode=git,
        git_base=git_base,
        git_to=git_to,
    )


    console.print(f"[bold green]sydes[/bold green] analyze: {repo_path}")
    console.print(
        f"Detected framework: [bold]{result.framework}[/bold] (confidence={result.confidence:.2f})"
    )
    console.print(f"Python files scanned: {result.files_scanned}")
    console.print(f"Candidate API files: {len(result.candidate_files)}")

    console.print("")
    console.print(f"Routes found (changed files only): [bold]{len(result.routes)}[/bold]")
    for r in result.routes[:50]:
        console.print(f"  {r.method:<6} {r.path:<35} -> {r.handler_name}")
    if len(result.routes) > 50:
        console.print(f"  … and {len(result.routes) - 50} more")
    console.print("")
    console.print(f"DB: {result.db_path}")
    console.print(f"Changed files: {result.changed_files}")
    console.print(f"Inserted routes: {result.inserted_routes}")
    console.print(f"Removed files: {result.removed_files}")
    if result.scan_id is not None:
        console.print(f"Scan saved: {result.scan_id}")
        console.print("Tip: run [bold]sydes diff <repo> --last[/bold] to see changes.")
    else:
        console.print("[yellow]Scan snapshot failed (analyze still succeeded).[/yellow]")


@endpoints_app.command("list")
def endpoints_list(
    repo: str = typer.Argument(..., help="Path to the repo"),
    method: Optional[str] = typer.Option(None, help="Filter by HTTP method (GET/POST/...)"),
    path: Optional[str] = typer.Option(None, help="Filter by exact HTTP path"),
    path_contains: Optional[str] = typer.Option(None, help="Substring match on HTTP path"),
    file_contains: Optional[str] = typer.Option(None, help="Substring match on file path"),
    handler_contains: Optional[str] = typer.Option(None, help="Substring match on handler name"),
    limit: int = typer.Option(200, help="Max rows to print"),
    format: str = typer.Option("table", help="Output format: table|json"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    rows = store.list_routes(
        method=method,
        http_path=path,
        path_contains=path_contains,
        file_contains=file_contains,
        handler_contains=handler_contains,
        limit=limit,
    )

    console.print(f"[bold]DB:[/bold] {db_path}")
    console.print(f"[bold]Routes:[/bold] {len(rows)} (showing up to {limit})")

    if format.lower() == "json":
        console.print(json.dumps(rows, indent=2))
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("METHOD", no_wrap=True)
    table.add_column("PATH")
    table.add_column("HANDLER")
    table.add_column("FILE:LINE", no_wrap=True)
    table.add_column("SRC", no_wrap=True)

    for r in rows:
        file_line = f"{r['rel_path']}:{r['decl_line']}"
        table.add_row(
            r["method"],
            r["http_path"],
            r["handler_name"],
            file_line,
            r["source"],
        )

    console.print(table)


@scans_app.command("list")
def scans_list(
    repo: str = typer.Argument(..., help="Path to the repo"),
    limit: int = typer.Option(20, help="How many scans to show"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    scans = store.list_scans(limit=limit)

    console.print(f"[bold]DB:[/bold] {db_path}")
    table = Table(show_header=True, header_style="bold")
    table.add_column("SCAN_ID", no_wrap=True)
    table.add_column("CREATED_AT (UTC)", no_wrap=True)
    table.add_column("GIT_COMMIT")

    for s in scans:
        created_utc = datetime.fromtimestamp(int(s.created_at), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        table.add_row(str(s.scan_id), created_utc, s.git_commit or "-")

    console.print(table)


@app.command()
def diff(
    repo: str = typer.Argument(..., help="Path to the repo"),
    last: bool = typer.Option(False, help="Diff latest scan vs previous scan"),
    from_scan: Optional[int] = typer.Option(None, help="From scan_id"),
    to_scan: Optional[int] = typer.Option(None, help="To scan_id"),
    format: str = typer.Option("table", help="Output format: table|json"),
    limit: int = typer.Option(50, help="Max rows per section to print"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    if last:
        scans = store.list_scans(limit=2)
        if len(scans) < 2:
            raise typer.BadParameter(
                "Only 1 scan found for this repo.\n\n"
                "To diff changes:\n"
                "  1) Run: sydes analyze <repo>\n"
                "  2) Make code changes\n"
                "  3) Run: sydes analyze <repo>\n"
                "  4) Then: sydes diff <repo> --last\n"
            )
        to_id = scans[0].scan_id
        from_id = scans[1].scan_id
    else:
        if from_scan is None or to_scan is None:
            raise typer.BadParameter("Provide --last OR both --from-scan and --to-scan.")
        from_id = int(from_scan)
        to_id = int(to_scan)

    d = store.diff_scans(from_id, to_id)

    if format.lower() == "json":
        console.print(json.dumps({"from": from_id, "to": to_id, **d}, indent=2))
        return

    console.print(f"[bold]DB:[/bold] {db_path}")
    console.print(f"[bold]Diff[/bold] from scan {from_id} -> {to_id}")
    console.print("")

    def _print_section(title: str, rows: list[dict], cols: list[str]) -> None:
        console.print(f"[bold]{title}:[/bold] {len(rows)}")
        t = Table(show_header=True, header_style="bold")
        for c in cols:
            t.add_column(c)
        for r in rows[:limit]:
            t.add_row(*[str(r.get(c, "")) for c in cols])
        console.print(t)
        if len(rows) > limit:
            console.print(f"  … and {len(rows) - limit} more")
        console.print("")

    _print_section(
        "Added",
        d["added"],
        ["method", "http_path", "rel_path", "handler_name"],
    )
    _print_section(
        "Removed",
        d["removed"],
        ["method", "http_path", "rel_path", "handler_name"],
    )
    _print_section(
        "Moved",
        d["moved"],
        ["method", "http_path", "from_rel_path", "to_rel_path", "from_handler", "to_handler"],
    )
    _print_section(
        "Handler changed",
        d["handler_changed"],
        ["method", "http_path", "rel_path", "from_handler", "to_handler"],
    )


# graph commands you already have (unchanged from Phase 2.2)
@graph_app.command("stats")
def graph_stats(
    repo: str = typer.Argument(..., help="Path to the repo"),
    limit: int = typer.Option(10, help="How many top files/handlers to show"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    rows = store.list_routes(limit=100_000)
    result = build_endpoint_graph(rows)
    g = result.graph

    endpoint_count = sum(1 for n in g.nodes.values() if n.type == "endpoint")
    file_count = sum(1 for n in g.nodes.values() if n.type == "file")
    handler_count = sum(1 for n in g.nodes.values() if n.type == "handler")
    declares_edges = sum(1 for e in g.edges if e.type == "DECLARES")
    handles_edges = sum(1 for e in g.edges if e.type == "HANDLES")

    console.print(f"[bold]DB:[/bold] {db_path}")
    console.print(f"[bold]Graph generated_at:[/bold] {result.generated_at}")
    console.print("")
    console.print(f"Nodes: endpoints={endpoint_count}, files={file_count}, handlers={handler_count}")
    console.print(f"Edges: DECLARES={declares_edges}, HANDLES={handles_edges}")


@structure_app.command("export")
def structure_export(
    repo: str = typer.Argument(..., help="Path to the repo"),
    format: str = typer.Option("table", help="Output format: table|json"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    rows = store.list_routes(limit=100_000)
    result = build_endpoint_graph(rows)
    g = result.graph

    console.print(f"[bold]DB:[/bold] {db_path}")
    ts = int(result.generated_at)
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    console.print(f"[bold]Graph generated_at:[/bold] {dt}")

    if format.lower() == "json":
        console.print(json.dumps(result.to_dict(), indent=2))
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("TYPE", no_wrap=True)
    table.add_column("NAME")
    table.add_column("DEGREE", no_wrap=True)

    # Compute degree (how many edges touch each node)
    deg: dict[str, int] = {nid: 0 for nid in g.nodes.keys()}
    for e in g.edges:
        # handle common edge field names safely
        src = getattr(e, "src", None) or getattr(e, "from_id", None) or getattr(e, "from_node", None)
        dst = getattr(e, "dst", None) or getattr(e, "to_id", None) or getattr(e, "to_node", None)

        if src in deg:
            deg[src] += 1
        if dst in deg:
            deg[dst] += 1

    for n in g.nodes.values():
        table.add_row(
            n.type,
            str(getattr(n, "name", n.id)),
            str(deg.get(n.id, 0)),
        )


    console.print(table)


@app.command()
def ping() -> None:
    console.print("pong")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
