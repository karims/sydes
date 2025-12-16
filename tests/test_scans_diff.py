from pathlib import Path
import textwrap

from sydes.orchestrator.pipeline import run_analyze
from sydes.store.sqlite_store import SydesSQLiteStore


def write(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(s), encoding="utf-8")


def test_scan_snapshots_and_diff(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()

    f = repo / "app.py"
    write(
        f,
        """
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/a")
        def a(): return {}
        """,
    )

    r1 = run_analyze(repo)
    assert r1.scan_id is not None

    # modify endpoint
    write(
        f,
        """
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/b")
        def b(): return {}
        """,
    )
    r2 = run_analyze(repo)
    assert r2.scan_id is not None

    store = SydesSQLiteStore(SydesSQLiteStore.db_path_for_repo(repo), repo_root=repo)
    scans = store.list_scans(limit=10)
    assert len(scans) >= 2

    d = store.diff_scans(r1.scan_id, r2.scan_id)
    assert {x["http_path"] for x in d["removed"]} == {"/a"}
    assert {x["http_path"] for x in d["added"]} == {"/b"}
