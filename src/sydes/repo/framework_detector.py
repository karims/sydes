from __future__ import annotations

from collections import Counter

from sydes.repo.scanner import _file_contains_any


def detect_python_framework(py_files: list[str], sample_limit: int = 200) -> tuple[str, float]:
    """
    Heuristic detection. v0: deterministic, fast, no LLM.
    Returns (framework, confidence).
    """
    sample = py_files[:sample_limit]
    scores = Counter()

    for p in sample:
        if _file_contains_any(p, ["from fastapi import", "FastAPI(", "APIRouter"]):
            scores["fastapi"] += 3
        if _file_contains_any(p, ["from flask import", "Flask(", "@app.route", "Blueprint("]):
            scores["flask"] += 2
        if _file_contains_any(p, ["from django.urls", "urlpatterns", "path(", "re_path("]):
            scores["django"] += 2

    if not scores:
        return ("unknown", 0.2)

    framework, top = scores.most_common(1)[0]
    total = sum(scores.values())
    confidence = max(0.3, min(0.99, top / max(total, 1)))
    return (framework, confidence)
