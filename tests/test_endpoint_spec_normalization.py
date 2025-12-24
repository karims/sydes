from sydes.testgen.normalize import normalize_routes


def test_normalize_routes_basic_and_deterministic():
    rows = [
        {
            "method": "get",
            "http_path": "users",              # missing leading slash on purpose
            "handler_name": "list_users",
            "rel_path": r"routers\users.py",   # windows slashes on purpose
            "source": "ast",
        }
    ]

    specs = normalize_routes(rows, framework="fastapi")

    assert len(specs) == 1
    s = specs[0]

    assert s.method == "GET"
    assert s.path == "/users"
    assert s.handler == "list_users"
    assert s.file_path == "routers/users.py"
    assert s.framework == "fastapi"
    assert isinstance(s.id, str) and len(s.id) == 40  # sha1 hex


def test_normalize_routes_sorting_stable():
    rows = [
        {"method": "get", "http_path": "/b", "handler_name": "h2", "rel_path": "r.py"},
        {"method": "get", "http_path": "/a", "handler_name": "h1", "rel_path": "r.py"},
    ]
    specs = normalize_routes(rows, framework="fastapi")
    assert [s.path for s in specs] == ["/a", "/b"]
