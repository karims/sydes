from sydes.testgen.plan import build_test_plan
from sydes.testgen.normalize import normalize_routes


def test_build_test_plan_groups_by_file_and_names_tests():
    rows = [
        {"method": "get", "http_path": "/users", "handler_name": "list_users", "rel_path": "routers/users.py"},
        {"method": "get", "http_path": "/users/{id}", "handler_name": "get_user", "rel_path": "routers/users.py"},
        {"method": "post", "http_path": "/auth/login", "handler_name": "login", "rel_path": "routers/auth.py"},
    ]
    specs = normalize_routes(rows, framework="fastapi")
    plan = build_test_plan(specs, generated_root="tests/generated")

    assert plan.generated_root == "tests/generated"
    assert len(plan.files) == 2

    files = {f.module_key: f for f in plan.files}
    assert "routers/users.py" in files
    assert "routers/auth.py" in files

    users_file = files["routers/users.py"]
    assert users_file.rel_path.endswith("test_users.py")
    assert [e.test_name for e in users_file.endpoints] == [
        "test_get_users",
        "test_get_users_by_id",
    ]
