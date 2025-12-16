from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


NodeType = Literal["endpoint", "file", "handler"]
EdgeType = Literal["DECLARES", "HANDLES"]


@dataclass(frozen=True)
class GraphNode:
    id: str
    type: NodeType
    label: str


@dataclass(frozen=True)
class GraphEdge:
    src: str
    dst: str
    type: EdgeType


@dataclass
class Graph:
    nodes: dict[str, GraphNode]
    edges: list[GraphEdge]

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []

    def add_node(self, node: GraphNode) -> None:
        # de-dupe by id
        if node.id not in self.nodes:
            self.nodes[node.id] = node

    def add_edge(self, edge: GraphEdge) -> None:
        self.edges.append(edge)
