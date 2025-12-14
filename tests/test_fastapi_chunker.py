from sydes.extractors.fastapi.chunker import extract_routes_from_source


def test_extract_routes_basic_app_and_router():
    src = """
from fastapi import FastAPI, APIRouter

app = FastAPI()
router = APIRouter()

@app.get("/users")
def list_users():
    return {"ok": True}

@router.post("/items/{id}")
async def create_item(id: int):
    return {"id": id}

def helper():
    pass
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 2
    assert routes[0].method == "GET"
    assert routes[0].path == "/users"
    assert routes[0].handler_name == "list_users"

    assert routes[1].method == "POST"
    assert routes[1].path == "/items/{id}"
    assert routes[1].handler_name == "create_item"


def test_extract_routes_ignores_non_constant_paths():
    src = """
from fastapi import FastAPI
app = FastAPI()
prefix = "/v1"

@app.get(prefix + "/users")
def list_users():
    return {}
"""
    routes = extract_routes_from_source(src)
    assert routes == []


def test_extract_routes_supports_path_keyword():
    src = """
from fastapi import FastAPI
app = FastAPI()

@app.get(path="/health")
def health():
    return {"ok": True}
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 1
    assert routes[0].method == "GET"
    assert routes[0].path == "/health"
    assert routes[0].handler_name == "health"

def test_extract_routes_multiple_decorators_on_same_handler():
    src = """
from fastapi import FastAPI
app = FastAPI()

@app.get("/a")
@app.post("/a")
def handler():
    return {}
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 2
    methods = {r.method for r in routes}
    assert methods == {"GET", "POST"}
    assert all(r.path == "/a" for r in routes)
    assert all(r.handler_name == "handler" for r in routes)


def test_extract_routes_router_prefix_is_not_resolved_in_v0():
    # In v0 we do NOT resolve router prefixes. That requires more context.
    src = """
from fastapi import APIRouter
router = APIRouter(prefix="/v1")

@router.get("/users")
def list_users():
    return {}
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 1
    assert routes[0].path == "/users"
    assert routes[0].method == "GET"


def test_extract_routes_add_api_route_methods_list():
    src = """
from fastapi import FastAPI
app = FastAPI()

def list_users():
    return {}

app.add_api_route("/users", list_users, methods=["GET"])
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 1
    assert routes[0].method == "GET"
    assert routes[0].path == "/users"
    assert routes[0].handler_name == "list_users"


def test_extract_routes_add_api_route_multiple_methods():
    src = """
from fastapi import FastAPI
app = FastAPI()

def handler():
    return {}

app.add_api_route("/x", handler, methods=["POST", "PUT"])
"""
    routes = extract_routes_from_source(src)
    assert len(routes) == 2
    methods = {r.method for r in routes}
    assert methods == {"POST", "PUT"}


def test_extract_routes_add_api_route_ignores_dynamic_methods():
    src = """
from fastapi import FastAPI
app = FastAPI()

def handler():
    return {}

m = ["GET"]
app.add_api_route("/x", handler, methods=m)
"""
    routes = extract_routes_from_source(src)
    assert routes == []
