from __future__ import annotations

from pathlib import Path

from sydes.repo.ignore import should_ignore_dir


def scan_python_files(repo_path: Path, max_files: int | None = None) -> list[str]:
    """
    Return a list of absolute file paths (as strings) for .py files under repo_path.
    Lightweight, deterministic. v0 intentionally simple.
    """
    out: list[str] = []
    for root, dirs, files in _walk(repo_path):
        root_p = Path(root)

        # prune ignored dirs
        dirs[:] = [d for d in dirs if not should_ignore_dir(root_p / d)]

        for f in files:
            if f.endswith(".py"):
                out.append(str((root_p / f).resolve()))
                if max_files is not None and len(out) >= max_files:
                    return out
    return out


def _walk(repo_path: Path):
    # Separate helper to make unit testing easier (can be mocked)
    return __import__("os").walk(repo_path)


def _file_contains_any(path: str, needles: list[str], max_bytes: int = 200_000) -> bool:
    try:
        with open(path, "rb") as f:
            data = f.read(max_bytes)
        text = data.decode("utf-8", errors="ignore")
        return any(n in text for n in needles)
    except Exception:
        return False


def select_candidate_api_files(py_files: list[str], framework_hint: str) -> list[str]:
    """
    Given a list of python files, select likely web/API entrypoints.
    For v0: heuristics only. Later: framework-specific chunkers.
    """
    if framework_hint == "fastapi":
        needles = ["from fastapi import", "FastAPI(", "APIRouter", "@app.", "@router.", "add_api_route"]
    elif framework_hint == "flask":
        needles = ["from flask import", "Flask(", "@app.route", "Blueprint("]
    elif framework_hint == "django":
        needles = ["from django.urls", "path(", "re_path(", "urlpatterns", "django.http"]
    else:
        needles = ["@app.", "route", "FastAPI(", "Flask(", "django.urls"]

    return [p for p in py_files if _file_contains_any(p, needles)]
