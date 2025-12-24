

from sydes.testgen.normalize import normalize_routes


def test_normalize_routes_basic():
    rows = [
        {
            "method": "get",
            "http_path": "/users",
            "handler_name": "list_users",
            "rel_path": "routers/users.py",
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
