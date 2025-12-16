from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from sydes.extractors.fastapi.chunker import RouteDecl


def _now_ts() -> int:
    return int(time.time())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class FileStatus:
    """Tracked file fingerprint for incremental scans (repo-relative)."""

    rel_path: str
    sha256: str
    mtime_ns: int
    size_bytes: int
    last_scanned_at: int


class SydesSQLiteStore:
    """Repo-local SQLite store.

    Phase 2.1 notes:
    - Paths stored in DB are repo-relative for portability.
    - Existing Phase-2 DBs (absolute paths) are migrated in-place.
    """

    SCHEMA_VERSION = "1.1"

    def __init__(self, db_path: Path, repo_root: Path):
        self.db_path = db_path
        self.repo_root = repo_root.resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db_and_migrate()

    @staticmethod
    def db_path_for_repo(repo_root: Path) -> Path:
        return repo_root / ".sydes" / "specs.db"

    # ----------------------------
    # Connection / schema
    # ----------------------------

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        return con

    def _init_db_and_migrate(self) -> None:
        with self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA foreign_keys=ON;")

            # meta table always exists
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

            # Detect existing tables
            has_files = (
                con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='files';"
                ).fetchone()
                is not None
            )
            has_routes = (
                con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='routes';"
                ).fetchone()
                is not None
            )

            if has_files and has_routes:
                files_cols = self._table_columns(con, "files")
                routes_cols = self._table_columns(con, "routes")

                # old phase-2 schema used: files.path, routes.file_path
                old_schema = ("path" in files_cols) or ("file_path" in routes_cols)
                if old_schema:
                    self._migrate_1_0_to_1_1(con)
                    self._set_meta(con, "schema_version", self.SCHEMA_VERSION)
                    return

            # Create v1.1 schema if missing
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    rel_path TEXT PRIMARY KEY,
                    sha256 TEXT NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    last_scanned_at INTEGER NOT NULL
                );
                """
            )

            con.execute(
                """
                CREATE TABLE IF NOT EXISTS routes (
                    id TEXT PRIMARY KEY,
                    rel_path TEXT NOT NULL,
                    method TEXT NOT NULL,
                    http_path TEXT NOT NULL,
                    handler_name TEXT NOT NULL,
                    start_line INTEGER NOT NULL,
                    end_line INTEGER NOT NULL,
                    decl_line INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                """
            )

            con.execute("CREATE INDEX IF NOT EXISTS idx_routes_file ON routes(rel_path);")
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_routes_mpath ON routes(method, http_path);"
            )

            if self._get_meta(con, "schema_version") is None:
                self._set_meta(con, "schema_version", self.SCHEMA_VERSION)

    def _migrate_1_0_to_1_1(self, con: sqlite3.Connection) -> None:
        """Migrate Phase-2 schema (absolute paths) to Phase-2.1 (repo-relative)."""

        # Create new tables
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS files_new (
                rel_path TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                mtime_ns INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                last_scanned_at INTEGER NOT NULL
            );
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS routes_new (
                id TEXT PRIMARY KEY,
                rel_path TEXT NOT NULL,
                method TEXT NOT NULL,
                http_path TEXT NOT NULL,
                handler_name TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                decl_line INTEGER NOT NULL,
                source TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            """
        )

        # Copy files: path -> rel_path
        rows = con.execute(
            "SELECT path, sha256, mtime_ns, size_bytes, last_scanned_at FROM files;"
        ).fetchall()
        for (path, sha, mtime_ns, size_bytes, last_scanned_at) in rows:
            # best-effort convert abs->rel if possible
            try:
                rel = str(Path(path).resolve().relative_to(self.repo_root))
            except Exception:
                rel = str(path)
            con.execute(
                """
                INSERT OR REPLACE INTO files_new(rel_path, sha256, mtime_ns, size_bytes, last_scanned_at)
                VALUES(?,?,?,?,?)
                """,
                (rel, sha, mtime_ns, size_bytes, last_scanned_at),
            )

        # Copy routes: file_path -> rel_path
        rrows = con.execute(
            """
            SELECT id, file_path, method, http_path, handler_name,
                   start_line, end_line, decl_line, source, updated_at
            FROM routes;
            """
        ).fetchall()

        for (
            rid,
            file_path,
            method,
            http_path,
            handler_name,
            start_line,
            end_line,
            decl_line,
            source,
            updated_at,
        ) in rrows:
            try:
                rel = str(Path(file_path).resolve().relative_to(self.repo_root))
            except Exception:
                rel = str(file_path)

            con.execute(
                """
                INSERT OR REPLACE INTO routes_new(
                    id, rel_path, method, http_path, handler_name,
                    start_line, end_line, decl_line, source, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    rid,
                    rel,
                    method,
                    http_path,
                    handler_name,
                    start_line,
                    end_line,
                    decl_line,
                    source,
                    updated_at,
                ),
            )

        # Swap tables
        con.execute("DROP TABLE routes;")
        con.execute("DROP TABLE files;")
        con.execute("ALTER TABLE routes_new RENAME TO routes;")
        con.execute("ALTER TABLE files_new RENAME TO files;")

        con.execute("CREATE INDEX IF NOT EXISTS idx_routes_file ON routes(rel_path);")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_routes_mpath ON routes(method, http_path);"
        )

    # ----------------------------
    # File tracking
    # ----------------------------

    def get_file_status(self, rel_path: str) -> Optional[FileStatus]:
        with self._connect() as con:
            row = con.execute(
                "SELECT rel_path, sha256, mtime_ns, size_bytes, last_scanned_at FROM files WHERE rel_path=?",
                (rel_path,),
            ).fetchone()
            if not row:
                return None
            return FileStatus(
                rel_path=row["rel_path"],
                sha256=row["sha256"],
                mtime_ns=row["mtime_ns"],
                size_bytes=row["size_bytes"],
                last_scanned_at=row["last_scanned_at"],
            )

    def upsert_file_status(self, status: FileStatus) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO files(rel_path, sha256, mtime_ns, size_bytes, last_scanned_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(rel_path) DO UPDATE SET
                    sha256=excluded.sha256,
                    mtime_ns=excluded.mtime_ns,
                    size_bytes=excluded.size_bytes,
                    last_scanned_at=excluded.last_scanned_at;
                """,
                (
                    status.rel_path,
                    status.sha256,
                    status.mtime_ns,
                    status.size_bytes,
                    status.last_scanned_at,
                ),
            )

    def remove_file(self, rel_path: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM routes WHERE rel_path=?", (rel_path,))
            con.execute("DELETE FROM files WHERE rel_path=?", (rel_path,))

    def list_tracked_files(self) -> list[str]:
        with self._connect() as con:
            rows = con.execute("SELECT rel_path FROM files").fetchall()
            return [r["rel_path"] for r in rows]

    # ----------------------------
    # Routes
    # ----------------------------

    def _route_id(self, rel_path: str, r: RouteDecl) -> str:
        # stable id based on where it came from + what it is (repo-relative)
        base = f"{rel_path}|{r.method}|{r.path}|{r.handler_name}|{r.decorator_line}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def replace_routes_for_file(self, rel_path: str, routes: Iterable[RouteDecl], source: str) -> int:
        """Delete old routes for file and insert new ones.

        Returns number inserted.
        """
        ts = _now_ts()
        to_insert = []
        for r in routes:
            rid = self._route_id(rel_path, r)
            to_insert.append(
                (
                    rid,
                    rel_path,
                    r.method,
                    r.path,
                    r.handler_name,
                    int(r.start_line),
                    int(r.end_line),
                    int(r.decorator_line),
                    source,
                    ts,
                )
            )

        with self._connect() as con:
            con.execute("DELETE FROM routes WHERE rel_path=?", (rel_path,))
            con.executemany(
                """
                INSERT OR REPLACE INTO routes(
                    id, rel_path, method, http_path, handler_name,
                    start_line, end_line, decl_line, source, updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                to_insert,
            )

        return len(to_insert)

    def list_routes(
        self,
        method: Optional[str] = None,
        http_path: Optional[str] = None,  # exact
        path_contains: Optional[str] = None,
        file_contains: Optional[str] = None,
        handler_contains: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        q = """
        SELECT id, rel_path, method, http_path, handler_name,
               start_line, end_line, decl_line, source, updated_at
        FROM routes
        """
        where: list[str] = []
        params: list[object] = []

        if method:
            where.append("method = ?")
            params.append(method.upper())
        if http_path:
            where.append("http_path = ?")
            params.append(http_path)
        if path_contains:
            where.append("http_path LIKE ?")
            params.append(f"%{path_contains}%")
        if file_contains:
            where.append("rel_path LIKE ?")
            params.append(f"%{file_contains}%")
        if handler_contains:
            where.append("handler_name LIKE ?")
            params.append(f"%{handler_contains}%")

        if where:
            q += " WHERE " + " AND ".join(where)

        q += " ORDER BY method, http_path, rel_path, decl_line LIMIT ?"
        params.append(int(limit))

        with self._connect() as con:
            rows = con.execute(q, tuple(params)).fetchall()
            return [dict(r) for r in rows]

    # ----------------------------
    # helpers for incremental hashing
    # ----------------------------

    def compute_file_fingerprint(self, path: Path, max_bytes: int = 2_000_000) -> tuple[str, int, int]:
        """Returns (sha256, mtime_ns, size_bytes)."""
        stat = path.stat()
        mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9)))
        size_bytes = int(stat.st_size)

        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[:max_bytes]
        sha = _sha256_bytes(data)
        return sha, mtime_ns, size_bytes

    # ----------------------------
    # internal helpers
    # ----------------------------

    def _rel(self, path: Path) -> str:
        p = path.resolve()
        try:
            return str(p.relative_to(self.repo_root))
        except Exception:
            return str(p)

    def _table_columns(self, con: sqlite3.Connection, table: str) -> set[str]:
        rows = con.execute(f"PRAGMA table_info({table});").fetchall()
        return {r[1] for r in rows}

    def _get_meta(self, con: sqlite3.Connection, key: str) -> Optional[str]:
        row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _set_meta(self, con: sqlite3.Connection, key: str, value: str) -> None:
        con.execute(
            """
            INSERT INTO meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
