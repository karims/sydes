from pathlib import Path
import textwrap

from sydes.orchestrator.pipeline import run_analyze
from sydes.store.sqlite_store import SydesSQLiteStore


def write(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(s), encoding="utf-8")


def test_incremental_updates_only_changed_files(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()

    f1 = repo / "app.py"
    f2 = repo / "other.py"

    write(
        f1,
        """
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/a")
        def a(): return {}
        """,
    )
    write(
        f2,
        """
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/b")
        def b(): return {}
        """,
    )

    r1 = run_analyze(repo)
    assert Path(r1.db_path).exists()
    assert r1.changed_files >= 1

    # list from DB (Phase 2.1 requires repo_root)
    store = SydesSQLiteStore(SydesSQLiteStore.db_path_for_repo(repo), repo_root=repo)
    rows = store.list_routes(limit=50)

    assert {x["http_path"] for x in rows} == {"/a", "/b"}
    assert {x["rel_path"] for x in rows} == {"app.py", "other.py"}

    # Second run: unchanged -> no changes
    r2 = run_analyze(repo)
    assert r2.changed_files == 0

    # Modify only f2
    write(
        f2,
        """
        from fastapi import FastAPI
        app = FastAPI()

        @app.get("/b2")
        def b(): return {}
        """,
    )
    r3 = run_analyze(repo)
    assert r3.changed_files == 1

    rows2 = store.list_routes(limit=50)
    assert {x["http_path"] for x in rows2} == {"/a", "/b2"}
