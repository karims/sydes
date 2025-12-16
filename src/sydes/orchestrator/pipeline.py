from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from sydes.extractors.fastapi.chunker import RouteDecl, extract_routes_from_file
from sydes.repo.framework_detector import detect_python_framework
from sydes.repo.scanner import scan_python_files, select_candidate_api_files
from sydes.store.sqlite_store import FileStatus, SydesSQLiteStore, _now_ts


@dataclass(frozen=True)
class AnalyzeResult:
    framework: str
    confidence: float
    files_scanned: int
    candidate_files: list[str]
    routes: list[RouteDecl]
    changed_files: int
    inserted_routes: int
    removed_files: int
    db_path: str


def run_analyze(repo_path: Path, max_files: int | None = None) -> AnalyzeResult:
    repo_path = repo_path.resolve()
    store = SydesSQLiteStore(SydesSQLiteStore.db_path_for_repo(repo_path), repo_root=repo_path)

    py_files = scan_python_files(repo_path, max_files=max_files)
    framework, confidence = detect_python_framework(py_files)
    candidates = select_candidate_api_files(py_files, framework_hint=framework)

    changed_files = 0
    inserted_routes = 0

    # Keep for removal detection (REL paths, Windows-safe)
    disk_set = set(
        os.path.relpath(str(Path(p).resolve()), str(repo_path))
        for p in candidates
    )
    tracked_files = set(store.list_tracked_files())

    # Remove files that were tracked but no longer present as candidates
    removed_files = 0
    for stale_rel in tracked_files - disk_set:
        store.remove_file(stale_rel)
        removed_files += 1

    # Process candidates incrementally
    collected_routes: list[RouteDecl] = []
    for p in candidates:
        fpath = Path(p).resolve()
        rel_path = os.path.relpath(str(fpath), str(repo_path))

        sha, mtime_ns, size_bytes = store.compute_file_fingerprint(fpath)
        prev = store.get_file_status(rel_path)  # ✅ rel_path
        if prev and prev.sha256 == sha:
            continue

        changed_files += 1

        routes: list[RouteDecl] = []
        source = "unknown"
        if framework == "fastapi":
            routes = extract_routes_from_file(fpath)
            source = "ast"

        # Dedup per file (method, path, handler, decl line)
        seen = set()
        deduped = []
        for r in routes:
            key = (r.method, r.path, r.handler_name, r.decorator_line)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(r)
        routes = deduped

        inserted_routes += store.replace_routes_for_file(rel_path, routes, source=source)  # ✅ rel_path
        store.upsert_file_status(
            FileStatus(
                rel_path=rel_path,
                sha256=sha,
                mtime_ns=mtime_ns,
                size_bytes=size_bytes,
                last_scanned_at=_now_ts(),
            )
        )

        collected_routes.extend(routes)

    rel_candidates = [
        os.path.relpath(str(Path(p).resolve()), str(repo_path))
        for p in candidates
    ]

    return AnalyzeResult(
        framework=framework,
        confidence=confidence,
        files_scanned=len(py_files),
        candidate_files=rel_candidates,
        routes=collected_routes,  # only routes from changed files (by design)
        changed_files=changed_files,
        inserted_routes=inserted_routes,
        removed_files=removed_files,
        db_path=str(store.db_path),
    )
