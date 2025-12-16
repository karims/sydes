from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable

from sydes.graph.model import Graph, GraphEdge, GraphNode


@dataclass(frozen=True)
class GraphBuildResult:
    graph: Graph
    generated_at: int


def _endpoint_id(method: str, http_path: str) -> str:
    return f"endpoint:{method.upper()} {http_path}"


def _file_id(rel_path: str) -> str:
    return f"file:{rel_path}"


def _handler_id(rel_path: str, handler_name: str) -> str:
    # v0: scope handler to file to avoid collisions across modules
    return f"handler:{rel_path}:{handler_name}"


def build_endpoint_graph(route_rows: Iterable[dict]) -> GraphBuildResult:
    """
    Build a thin deterministic graph from routes.

    Nodes:
      - endpoint: METHOD + PATH
      - file: rel_path
      - handler: rel_path + handler_name (scoped)

    Edges:
      - file -> endpoint (DECLARES)
      - handler -> endpoint (HANDLES)
    """
    g = Graph()
    now = int(time.time())

    # De-dupe edges (important for stable export)
    edge_seen: set[tuple[str, str, str]] = set()

    for r in route_rows:
        method = str(r["method"]).upper()
        http_path = str(r["http_path"])
        rel_path = str(r["rel_path"])
        handler_name = str(r["handler_name"])

        eid = _endpoint_id(method, http_path)
        fid = _file_id(rel_path)
        hid = _handler_id(rel_path, handler_name)

        g.add_node(GraphNode(id=eid, type="endpoint", label=f"{method} {http_path}"))
        g.add_node(GraphNode(id=fid, type="file", label=rel_path))
        g.add_node(GraphNode(id=hid, type="handler", label=handler_name))

        e1 = (fid, eid, "DECLARES")
        if e1 not in edge_seen:
            g.add_edge(GraphEdge(src=fid, dst=eid, type="DECLARES"))
            edge_seen.add(e1)

        e2 = (hid, eid, "HANDLES")
        if e2 not in edge_seen:
            g.add_edge(GraphEdge(src=hid, dst=eid, type="HANDLES"))
            edge_seen.add(e2)

    # Make output stable (for diffs)
    g.edges.sort(key=lambda e: (e.type, e.src, e.dst))

    return GraphBuildResult(graph=g, generated_at=now)
