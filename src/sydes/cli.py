from __future__ import annotations

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
    console.print("")
    console.print(f"DB: {result.db_path}")
    console.print(f"Changed files: {result.changed_files}")
    console.print(f"Inserted routes: {result.inserted_routes}")
    console.print(f"Removed files: {result.removed_files}")
    console.print("")
    console.print("Tip: run [bold]sydes endpoints list <repo>[/bold] to list everything from the DB.")


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


@graph_app.command("stats")
def graph_stats(
    repo: str = typer.Argument(..., help="Path to the repo"),
    limit: int = typer.Option(10, help="How many top files/handlers to show"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    rows = store.list_routes(limit=100_000)  # stats wants all routes; DB query is cheap
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

    # Top files by endpoints declared
    from collections import Counter

    file_to_endpoints = Counter()
    handler_to_endpoints = Counter()

    # Build quick counts from edges
    for e in g.edges:
        if e.type == "DECLARES":
            # src=file:..., dst=endpoint:...
            file_to_endpoints[e.src] += 1
        elif e.type == "HANDLES":
            handler_to_endpoints[e.src] += 1

    console.print("")
    console.print(f"[bold]Top files by endpoints declared (limit {limit}):[/bold]")
    for fid, cnt in file_to_endpoints.most_common(limit):
        label = g.nodes[fid].label if fid in g.nodes else fid
        console.print(f"  {cnt:>4}  {label}")

    console.print("")
    console.print(f"[bold]Top handlers by endpoints handled (limit {limit}):[/bold]")
    for hid, cnt in handler_to_endpoints.most_common(limit):
        # handler label is just handler_name; still useful
        label = g.nodes[hid].label if hid in g.nodes else hid
        console.print(f"  {cnt:>4}  {label}")


@graph_app.command("export")
def graph_export(
    repo: str = typer.Argument(..., help="Path to the repo"),
    format: str = typer.Option("json", help="Export format: json|dot"),
    out: Optional[str] = typer.Option(None, help="Output path (default: print to stdout)"),
    limit: int = typer.Option(200_000, help="Max routes to load for export (safety)"),
) -> None:
    repo_path = Path(repo).expanduser().resolve()
    db_path = SydesSQLiteStore.db_path_for_repo(repo_path)
    store = SydesSQLiteStore(db_path, repo_root=repo_path)

    rows = store.list_routes(limit=limit)
    result = build_endpoint_graph(rows)
    g = result.graph

    fmt = format.lower().strip()
    if fmt not in ("json", "dot"):
        raise typer.BadParameter("format must be one of: json, dot")

    if fmt == "json":
        payload = {
            "repo": str(repo_path),
            "db": str(db_path),
            "generated_at": result.generated_at,
            "nodes": [
                {"id": n.id, "type": n.type, "label": n.label}
                for n in sorted(g.nodes.values(), key=lambda x: (x.type, x.id))
            ],
            "edges": [
                {"src": e.src, "dst": e.dst, "type": e.type}
                for e in g.edges
            ],
        }
        text = json.dumps(payload, indent=2)
    else:
        # DOT (Graphviz) export
        lines = []
        lines.append("digraph sydes {")
        lines.append('  rankdir="LR";')
        lines.append('  node [shape="box"];')

        # Grouping by type with different shapes (still deterministic)
        for node in sorted(g.nodes.values(), key=lambda x: (x.type, x.id)):
            # Keep IDs safe for DOT: quote them
            label = node.label.replace('"', '\\"')
            lines.append(f'  "{node.id}" [label="{label}"];')

        for e in g.edges:
            lines.append(f'  "{e.src}" -> "{e.dst}" [label="{e.type}"];')

        lines.append("}")
        text = "\n".join(lines)

    if out:
        out_path = Path(out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
        console.print(f"[bold green]Wrote[/bold green] {fmt} graph to: {out_path}")
    else:
        console.print(text)


@app.command()
def ping() -> None:
    console.print("pong")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
