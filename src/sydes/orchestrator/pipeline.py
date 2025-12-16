from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

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
    scan_id: int | None
    mode: str  # "full" | "git"


def _best_effort_git_commit(repo_path: Path) -> str | None:
    try:
        if not (repo_path / ".git").exists():
            return None
        out = subprocess.check_output(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or None
    except Exception:
        return None


def _best_effort_git_changes(repo_path: Path, base_ref: str, to_ref: str) -> tuple[set[str], set[str]]:
    """
    Returns (changed_files, deleted_files) as rel paths (POSIX-ish from git, we normalize).
    Uses: git diff --name-status base..to
    """
    try:
        if not (repo_path / ".git").exists():
            return set(), set()

        out = subprocess.check_output(
            ["git", "-C", str(repo_path), "diff", "--name-status", f"{base_ref}..{to_ref}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        changed: set[str] = set()
        deleted: set[str] = set()

        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            # Format: "M\tpath" or "A\tpath" or "D\tpath" or "R100\told\tnew"
            parts = line.split("\t")
            status = parts[0]
            if status.startswith("R") and len(parts) >= 3:
                # treat rename as delete+add
                oldp, newp = parts[1], parts[2]
                deleted.add(oldp.replace("/", os.sep))
                changed.add(newp.replace("/", os.sep))
                continue

            if len(parts) < 2:
                continue
            path = parts[1].replace("/", os.sep)

            if status == "D":
                deleted.add(path)
            else:
                changed.add(path)

        return changed, deleted
    except Exception:
        return set(), set()


def run_analyze(
    repo_path: Path,
    max_files: int | None = None,
    git_mode: bool = False,
    git_base: str = "HEAD~1",
    git_to: str = "HEAD",
) -> AnalyzeResult:
    repo_path = repo_path.resolve()
    store = SydesSQLiteStore(SydesSQLiteStore.db_path_for_repo(repo_path), repo_root=repo_path)

    py_files = scan_python_files(repo_path, max_files=max_files)
    framework, confidence = detect_python_framework(py_files)
    candidates = select_candidate_api_files(py_files, framework_hint=framework)

    changed_files = 0
    inserted_routes = 0
    removed_files = 0
    mode = "git" if git_mode else "full"

    # FULL mode: candidate set is the whole world, so we can remove stale tracked files safely.
    if not git_mode:
        disk_set = {os.path.relpath(str(Path(p).resolve()), str(repo_path)) for p in candidates}
        tracked_files = set(store.list_tracked_files())

        for stale_rel in tracked_files - disk_set:
            store.remove_file(stale_rel)
            removed_files += 1

        candidates_to_process = candidates

    else:
        # GIT mode: only process changed candidates; do NOT delete "stale" files because
        # they might just be unchanged. Only handle deletions explicitly from git.
        changed_rel, deleted_rel = _best_effort_git_changes(repo_path, git_base, git_to)

        # remove deleted files if they were tracked
        for d in deleted_rel:
            store.remove_file(d)
            removed_files += 1

        # process only changed files that are in candidates
        cand_rel = {os.path.relpath(str(Path(p).resolve()), str(repo_path)) for p in candidates}
        changed_candidates_rel = sorted(changed_rel & cand_rel)

        candidates_to_process = [str((repo_path / rel).resolve()) for rel in changed_candidates_rel]

    collected_routes: list[RouteDecl] = []

    for p in candidates_to_process:
        fpath = Path(p).resolve()
        rel_path = os.path.relpath(str(fpath), str(repo_path))

        if not fpath.exists() or not fpath.is_file():
            # If file disappeared outside git detection, remove and continue
            store.remove_file(rel_path)
            removed_files += 1
            continue

        sha, mtime_ns, size_bytes = store.compute_file_fingerprint(fpath)
        prev = store.get_file_status(rel_path)
        if prev and prev.sha256 == sha:
            continue

        changed_files += 1

        routes: list[RouteDecl] = []
        source = "unknown"
        if framework == "fastapi":
            routes = extract_routes_from_file(fpath)
            source = "ast"

        # Dedup per file
        seen = set()
        deduped = []
        for r in routes:
            key = (r.method, r.path, r.handler_name, r.decorator_line)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(r)
        routes = deduped

        inserted_routes += store.replace_routes_for_file(rel_path, routes, source=source)
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
        os.path.relpath(str(Path(p).resolve()), str(repo_path)) for p in candidates
    ]

    scan_id: int | None = None
    git_commit = _best_effort_git_commit(repo_path)
    try:
        scan_id = store.create_scan(git_commit=git_commit)
        store.snapshot_current_endpoints(scan_id)
    except Exception:
        scan_id = None

    return AnalyzeResult(
        framework=framework,
        confidence=confidence,
        files_scanned=len(py_files),
        candidate_files=rel_candidates,
        routes=collected_routes,
        changed_files=changed_files,
        inserted_routes=inserted_routes,
        removed_files=removed_files,
        db_path=str(store.db_path),
        scan_id=scan_id,
        mode=mode,
    )
