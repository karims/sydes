from __future__ import annotations

from pathlib import Path

DEFAULT_IGNORES = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
}


def should_ignore_dir(dir_path: Path) -> bool:
    return dir_path.name in DEFAULT_IGNORES
