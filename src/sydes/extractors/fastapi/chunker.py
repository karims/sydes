from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

_HTTP_METHOD_ATTRS = {
    "get": "GET",
    "post": "POST",
    "put": "PUT",
    "patch": "PATCH",
    "delete": "DELETE",
    "options": "OPTIONS",
    "head": "HEAD",
}


@dataclass(frozen=True)
class RouteDecl:
    method: str
    path: str
    handler_name: str
    start_line: int
    end_line: int
    decorator_line: int
    file_path: str = ""


def extract_routes_from_source(source: str) -> list[RouteDecl]:
    """
    Parse Python source and extract FastAPI routes declared via decorators like:
      @app.get("/path")
      @router.post("/path")
    Uses ast only; does not import/execute code.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    routes: list[RouteDecl] = []

    for node in _iter_function_defs(tree):
        handler_name = node.name
        start_line = getattr(node, "lineno", 1) or 1
        end_line = getattr(node, "end_lineno", start_line) or start_line

        for dec in node.decorator_list:
            maybe = _parse_fastapi_route_decorator(dec)
            if maybe is None:
                continue

            method, path, decorator_line = maybe
            routes.append(
                RouteDecl(
                    method=method,
                    path=path,
                    handler_name=handler_name,
                    start_line=start_line,
                    end_line=end_line,
                    decorator_line=decorator_line,
                )
            )
    # Also extract programmatic routes: app.add_api_route(...)
    for node in ast.walk(tree):
        for (method, path, handler_name, line) in _parse_add_api_route_call(node):
            # We don't know function span from this call; keep best-effort lines as the call line.
            routes.append(
                RouteDecl(
                    method=method,
                    path=path,
                    handler_name=handler_name,
                    start_line=line,
                    end_line=line,
                    decorator_line=line,
                )
            )


    # stable ordering: by decorator line, then handler name
    routes.sort(key=lambda r: (r.decorator_line, r.handler_name))
    return routes


def extract_routes_from_file(path: Path, max_bytes: int = 500_000) -> list[RouteDecl]:
    try:
        data = path.read_bytes()[:max_bytes]
        source = data.decode("utf-8", errors="ignore")
    except Exception:
        return []
    routes = extract_routes_from_source(source)
    abs_path = str(path.resolve())
    return [RouteDecl(**{**r.__dict__, "file_path": abs_path}) for r in routes]


def _iter_function_defs(tree: ast.AST) -> Iterable[ast.AST]:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node


def _parse_fastapi_route_decorator(dec: ast.AST) -> Optional[tuple[str, str, int]]:
    """
    Recognize decorators of form:
      @<anything>.<method>(<path>, ...)
    where <method> in HTTP methods (get/post/...)
    Extract first positional arg or keyword 'path' if it's a constant string.

    Returns (METHOD, path, decorator_line) or None.
    """
    decorator_line = getattr(dec, "lineno", 1) or 1

    # decorator must be a call: @router.get("/x")
    if not isinstance(dec, ast.Call):
        return None

    # call.func must be something like: <expr>.get
    func = dec.func
    if not isinstance(func, ast.Attribute):
        return None

    attr = func.attr
    if attr not in _HTTP_METHOD_ATTRS:
        return None

    method = _HTTP_METHOD_ATTRS[attr]

    # path can be first positional arg: @app.get("/x")
    path_value = None
    if dec.args:
        path_value = _const_str(dec.args[0])

    # or keyword arg: @app.get(path="/x")
    if path_value is None:
        for kw in dec.keywords or []:
            if kw.arg == "path":
                path_value = _const_str(kw.value)
                break

    if path_value is None:
        return None

    return (method, path_value, decorator_line)


def _const_str(node: ast.AST) -> Optional[str]:
    # Python 3.8+: string literal is ast.Constant(value=str)
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.strip()

    # We intentionally do NOT attempt to evaluate f-strings / format / concatenations in v0.
    return None


def _const_str_list(node: ast.AST) -> Optional[list[str]]:
    # methods=["GET","POST"] or ("GET",)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        out = []
        for elt in node.elts:
            s = _const_str(elt)
            if s is None:
                return None
            out.append(s.strip().upper())
        return out

    # methods="GET"
    s = _const_str(node)
    if s is not None:
        return [s.strip().upper()]

    return None


def _handler_to_name(node: ast.AST) -> Optional[str]:
    # handler name in add_api_route("/x", handler, ...)
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        # e.g. handlers.list_users
        try:
            return ast.unparse(node)  # py3.9+
        except Exception:
            return node.attr
    return None


def _parse_add_api_route_call(node: ast.AST) -> list[tuple[str, str, str, int]]:
    """
    Return list of (METHOD, path, handler_name, call_line)
    """
    if not isinstance(node, ast.Call):
        return []
    func = node.func
    if not isinstance(func, ast.Attribute):
        return []
    if func.attr != "add_api_route":
        return []

    call_line = getattr(node, "lineno", 1) or 1

    # Need at least: add_api_route(path, endpoint, ...)
    if len(node.args) < 2:
        return []

    path = _const_str(node.args[0])
    if path is None:
        return []

    handler_name = _handler_to_name(node.args[1])
    if handler_name is None:
        return []

    methods_node = None
    for kw in node.keywords or []:
        if kw.arg == "methods":
            methods_node = kw.value
            break

    if methods_node is None:
        return []  # v0: don't guess defaults

    methods = _const_str_list(methods_node)
    if not methods:
        return []

    out = []
    for m in methods:
        out.append((m, path.strip(), handler_name, call_line))
    return out
