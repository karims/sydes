from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Any


def _iter_py_files(repo_root: Path) -> list[Path]:
    out: list[Path] = []
    for p in repo_root.rglob("*.py"):
        # skip common junk
        if any(part in (".venv", "venv", "__pycache__", ".git", ".sydes") for part in p.parts):
            continue
        out.append(p)
    return out


def _safe_parse(path: Path) -> ast.AST | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except Exception:
        return None


def _name_of_expr(node: ast.AST) -> str:
    # best-effort stringify for common cases
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_name_of_expr(node.value)}.{node.attr}"
    if isinstance(node, ast.Call):
        return _name_of_expr(node.func)
    if isinstance(node, ast.Subscript):
        return _name_of_expr(node.value)
    return node.__class__.__name__


def extract_fastapi_structure(repo_root: Path) -> dict[str, Any]:
    """
    Deterministic structural signals:
      - include_router(...) edges
      - Depends(...) names
    """
    repo_root = repo_root.resolve()

    includes: list[dict[str, str]] = []
    depends: list[dict[str, str]] = []

    for f in _iter_py_files(repo_root):
        tree = _safe_parse(f)
        if tree is None:
            continue

        rel_path = os.path.relpath(str(f), str(repo_root))

        for node in ast.walk(tree):
            # include_router: <something>.include_router(<router_expr>, prefix="...")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr == "include_router":
                    frm = _name_of_expr(node.func.value)
                    router_expr = _name_of_expr(node.args[0]) if node.args else ""
                    prefix = None
                    for kw in node.keywords:
                        if kw.arg == "prefix":
                            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                                prefix = kw.value.value
                            else:
                                prefix = _name_of_expr(kw.value)
                    includes.append(
                        {
                            "from": frm,
                            "router": router_expr or "-",
                            "prefix": prefix or "",
                            "rel_path": rel_path,
                        }
                    )

            # Depends: Depends(x) or fastapi.Depends(x)
            if isinstance(node, ast.Call):
                fn = _name_of_expr(node.func)
                if fn.endswith("Depends"):
                    name = ""
                    if node.args:
                        name = _name_of_expr(node.args[0])
                    depends.append({"name": name or "Depends", "rel_path": rel_path})

    # de-dupe
    inc_seen = set()
    inc_dedup = []
    for r in includes:
        k = (r["from"], r["router"], r.get("prefix") or "", r["rel_path"])
        if k in inc_seen:
            continue
        inc_seen.add(k)
        inc_dedup.append(r)

    dep_seen = set()
    dep_dedup = []
    for d in depends:
        k = (d["name"], d["rel_path"])
        if k in dep_seen:
            continue
        dep_seen.add(k)
        dep_dedup.append(d)

    return {"includes": inc_dedup, "depends": dep_dedup}
