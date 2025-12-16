from sydes.graph.builder import build_endpoint_graph


def test_build_endpoint_graph_basic():
    rows = [
        {"method": "GET", "http_path": "/a", "rel_path": "app.py", "handler_name": "a", "decl_line": 10},
        {"method": "GET", "http_path": "/b", "rel_path": "other.py", "handler_name": "b", "decl_line": 20},
    ]
    res = build_endpoint_graph(rows)
    g = res.graph

    # Nodes: 2 endpoints + 2 files + 2 handlers
    assert len(g.nodes) == 6

    endpoint_ids = {n.id for n in g.nodes.values() if n.type == "endpoint"}
    assert endpoint_ids == {"endpoint:GET /a", "endpoint:GET /b"}

    # Edges: 2 DECLARES + 2 HANDLES
    assert len(g.edges) == 4
    assert sum(1 for e in g.edges if e.type == "DECLARES") == 2
    assert sum(1 for e in g.edges if e.type == "HANDLES") == 2


def test_build_endpoint_graph_handler_scoped_to_file():
    # same handler_name in different files should not collide
    rows = [
        {"method": "GET", "http_path": "/x", "rel_path": "a.py", "handler_name": "read", "decl_line": 1},
        {"method": "GET", "http_path": "/y", "rel_path": "b.py", "handler_name": "read", "decl_line": 1},
    ]
    res = build_endpoint_graph(rows)
    g = res.graph

    handler_ids = {n.id for n in g.nodes.values() if n.type == "handler"}
    assert "handler:a.py:read" in handler_ids
    assert "handler:b.py:read" in handler_ids
