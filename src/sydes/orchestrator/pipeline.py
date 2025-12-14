from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sydes.extractors.fastapi.chunker import RouteDecl, extract_routes_from_file
from sydes.repo.framework_detector import detect_python_framework
from sydes.repo.scanner import scan_python_files, select_candidate_api_files


@dataclass(frozen=True)
class AnalyzeResult:
    framework: str
    confidence: float
    files_scanned: int
    candidate_files: list[str]
    routes: list[RouteDecl]


def run_analyze(repo_path: Path, max_files: int | None = None) -> AnalyzeResult:
    py_files = scan_python_files(repo_path, max_files=max_files)
    framework, confidence = detect_python_framework(py_files)
    candidates = select_candidate_api_files(py_files, framework_hint=framework)

    routes: list[RouteDecl] = []
    if framework == "fastapi":
        for p in candidates:
            routes.extend(extract_routes_from_file(Path(p)))
    
    # Dedup: (file, method, path, handler, decorator_line)
    seen = set()
    deduped = []
    for r in routes:
        key = (r.file_path, r.method, r.path, r.handler_name, r.decorator_line)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    routes = deduped

    rel_candidates = [str(Path(p).relative_to(repo_path)) for p in candidates]
    return AnalyzeResult(
        framework=framework,
        confidence=confidence,
        files_scanned=len(py_files),
        candidate_files=rel_candidates,
        routes=routes,
    )
