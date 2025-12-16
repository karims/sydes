from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


def _now_ts() -> int:
    return int(time.time())


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _endpoint_id(method: str, http_path: str) -> str:
    base = f"{method.upper()}|{http_path}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FileStatus:
    rel_path: str
    sha256: str
    mtime_ns: int
    size_bytes: int
    last_scanned_at: int


@dataclass(frozen=True)
class ScanMeta:
    scan_id: int
    created_at: int
    git_commit: Optional[str]


class SydesSQLiteStore:
    SCHEMA_VERSION = "1.2"

    def __init__(self, db_path: Path, repo_root: Path):
        self.db_path = db_path
        self.repo_root = repo_root.resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db_and_migrate()

    @staticmethod
    def db_path_for_repo(repo_root: Path) -> Path:
        return repo_root / ".sydes" / "specs.db"

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        return con

    # -------------------- schema & migrations --------------------

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

            schema_version = self._get_meta(con, "schema_version")

            # If schema_version missing, we may be on old layouts; detect and migrate.
            if schema_version is None:
                # Detect old schema by checking tables/columns
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
                    # very old used abs path columns
                    old_schema_1_0 = ("path" in files_cols) or ("file_path" in routes_cols)
                else:
                    old_schema_1_0 = False

                if old_schema_1_0:
                    self._migrate_1_0_to_1_1(con)
                    schema_version = "1.1"
                    self._set_meta(con, "schema_version", schema_version)
                else:
                    # Fresh DB: create 1.2 directly
                    self._create_schema_1_2(con)
                    self._set_meta(con, "schema_version", self.SCHEMA_VERSION)
                    return

            # If schema is 1.1, migrate to 1.2 (add scans tables)
            if schema_version == "1.1":
                self._migrate_1_1_to_1_2(con)
                self._set_meta(con, "schema_version", self.SCHEMA_VERSION)
                return

            # If schema already 1.2, ensure tables exist (idempotent)
            if schema_version == "1.2":
                self._create_schema_1_2(con)
                return

            # Unknown schema
            raise RuntimeError(f"Unsupported schema_version in DB: {schema_version}")

    def _create_schema_1_2(self, con: sqlite3.Connection) -> None:
        # Core tables (1.1)
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
        con.execute("CREATE INDEX IF NOT EXISTS idx_routes_mpath ON routes(method, http_path);")

        # Phase 3.0: scans + endpoint snapshots
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS scans (
                scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                git_commit TEXT
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_scans_created_at ON scans(created_at);")

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS scan_endpoints (
                scan_id INTEGER NOT NULL,
                endpoint_id TEXT NOT NULL,
                method TEXT NOT NULL,
                http_path TEXT NOT NULL,
                rel_path TEXT NOT NULL,
                handler_name TEXT NOT NULL,
                decl_line INTEGER NOT NULL,
                source TEXT NOT NULL,
                PRIMARY KEY (scan_id, endpoint_id),
                FOREIGN KEY (scan_id) REFERENCES scans(scan_id) ON DELETE CASCADE
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_scan_endpoints_scan ON scan_endpoints(scan_id);")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_scan_endpoints_mpath ON scan_endpoints(method, http_path);"
        )

    def _migrate_1_0_to_1_1(self, con: sqlite3.Connection) -> None:
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
            SELECT id, file_path, method, http_path, handler_name, start_line, end_line, decl_line, source, updated_at
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
                (rid, rel, method, http_path, handler_name, start_line, end_line, decl_line, source, updated_at),
            )

        # Swap tables
        con.execute("DROP TABLE routes;")
        con.execute("DROP TABLE files;")
        con.execute("ALTER TABLE routes_new RENAME TO routes;")
        con.execute("ALTER TABLE files_new RENAME TO files;")

        con.execute("CREATE INDEX IF NOT EXISTS idx_routes_file ON routes(rel_path);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_routes_mpath ON routes(method, http_path);")

    def _migrate_1_1_to_1_2(self, con: sqlite3.Connection) -> None:
        # Just ensure 1.2 tables exist (idempotent)
        self._create_schema_1_2(con)

    # -------------------- files & routes (incremental) --------------------

    def get_file_status(self, rel_path: str) -> Optional[FileStatus]:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT rel_path, sha256, mtime_ns, size_bytes, last_scanned_at
                FROM files WHERE rel_path=?
                """,
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
                (status.rel_path, status.sha256, status.mtime_ns, status.size_bytes, status.last_scanned_at),
            )

    def remove_file(self, rel_path: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM routes WHERE rel_path=?", (rel_path,))
            con.execute("DELETE FROM files WHERE rel_path=?", (rel_path,))

    def list_tracked_files(self) -> list[str]:
        with self._connect() as con:
            rows = con.execute("SELECT rel_path FROM files").fetchall()
            return [r["rel_path"] for r in rows]

    def replace_routes_for_file(self, rel_path: str, routes: Iterable[object], source: str) -> int:
        """
        Delete old routes for file and insert new ones.
        The `routes` objects must have fields:
          method, path, handler_name, start_line, end_line, decorator_line
        """
        ts = _now_ts()
        to_insert = []
        for r in routes:
            # Build stable route id (per decl)
            base = f"{rel_path}|{r.method}|{r.path}|{r.handler_name}|{r.decorator_line}"
            rid = hashlib.sha1(base.encode("utf-8")).hexdigest()
            to_insert.append(
                (
                    rid,
                    rel_path,
                    str(r.method).upper(),
                    str(r.path),
                    str(r.handler_name),
                    int(r.start_line),
                    int(r.end_line),
                    int(r.decorator_line),
                    str(source),
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
        http_path: Optional[str] = None,
        path_contains: Optional[str] = None,
        file_contains: Optional[str] = None,
        handler_contains: Optional[str] = None,
        limit: int = 200,
    ) -> list[dict]:
        q = """
        SELECT id, rel_path, method, http_path, handler_name, start_line, end_line, decl_line, source, updated_at
        FROM routes
        """
        params: list[object] = []
        where: list[str] = []

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

    # -------------------- incremental hashing --------------------

    def compute_file_fingerprint(self, path: Path, max_bytes: int = 2_000_000) -> tuple[str, int, int]:
        """
        Returns (sha256, mtime_ns, size_bytes)
        """
        stat = path.stat()
        mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1e9)))
        size_bytes = int(stat.st_size)

        data = path.read_bytes()
        if len(data) > max_bytes:
            data = data[:max_bytes]
        sha = _sha256_bytes(data)
        return sha, mtime_ns, size_bytes

    # -------------------- scans & diffs (Phase 3.0) --------------------

    def create_scan(self, git_commit: Optional[str] = None) -> int:
        with self._connect() as con:
            con.execute(
                "INSERT INTO scans(created_at, git_commit) VALUES(?, ?)",
                (_now_ts(), git_commit),
            )
            scan_id = con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            return int(scan_id)

    def list_scans(self, limit: int = 50) -> list[ScanMeta]:
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT scan_id, created_at, git_commit
                FROM scans
                ORDER BY scan_id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            return [
                ScanMeta(
                    scan_id=int(r["scan_id"]),
                    created_at=int(r["created_at"]),
                    git_commit=r["git_commit"],
                )
                for r in rows
            ]

    def snapshot_current_endpoints(self, scan_id: int) -> int:
        """
        Snapshot the CURRENT routes table into scan_endpoints for this scan_id.
        Returns number of endpoints snapshotted (distinct METHOD+PATH).
        """
        rows = self.list_routes(limit=1_000_000)

        # For endpoint-level snapshot, we keep one row per METHOD+PATH.
        # If duplicates exist, keep the first deterministically (by file+decl_line via SQL ordering already).
        seen: set[str] = set()
        inserts: list[tuple[object, ...]] = []

        for r in rows:
            method = str(r["method"]).upper()
            http_path = str(r["http_path"])
            eid = _endpoint_id(method, http_path)
            if eid in seen:
                continue
            seen.add(eid)

            inserts.append(
                (
                    int(scan_id),
                    eid,
                    method,
                    http_path,
                    str(r["rel_path"]),
                    str(r["handler_name"]),
                    int(r["decl_line"]),
                    str(r["source"]),
                )
            )

        with self._connect() as con:
            con.executemany(
                """
                INSERT OR REPLACE INTO scan_endpoints(
                    scan_id, endpoint_id, method, http_path, rel_path, handler_name, decl_line, source
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                inserts,
            )

        return len(inserts)

    def get_scan_endpoints(self, scan_id: int) -> dict[str, dict]:
        """
        Returns dict endpoint_id -> row dict
        """
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT endpoint_id, method, http_path, rel_path, handler_name, decl_line, source
                FROM scan_endpoints
                WHERE scan_id=?
                """,
                (int(scan_id),),
            ).fetchall()
            return {str(r["endpoint_id"]): dict(r) for r in rows}

    def diff_scans(self, from_scan_id: int, to_scan_id: int) -> dict[str, list[dict]]:
        """
        Returns:
          added: endpoints in to, not in from
          removed: endpoints in from, not in to
          moved: same endpoint id, but rel_path changed
          handler_changed: same endpoint id+file, but handler_name changed
        """
        a = self.get_scan_endpoints(from_scan_id)
        b = self.get_scan_endpoints(to_scan_id)

        a_ids = set(a.keys())
        b_ids = set(b.keys())

        added = [b[i] for i in sorted(b_ids - a_ids)]
        removed = [a[i] for i in sorted(a_ids - b_ids)]

        moved: list[dict] = []
        handler_changed: list[dict] = []

        for i in sorted(a_ids & b_ids):
            ra = a[i]
            rb = b[i]
            if ra["rel_path"] != rb["rel_path"]:
                moved.append(
                    {
                        "endpoint_id": i,
                        "method": rb["method"],
                        "http_path": rb["http_path"],
                        "from_rel_path": ra["rel_path"],
                        "to_rel_path": rb["rel_path"],
                        "from_handler": ra["handler_name"],
                        "to_handler": rb["handler_name"],
                    }
                )
                continue

            if ra["handler_name"] != rb["handler_name"]:
                handler_changed.append(
                    {
                        "endpoint_id": i,
                        "method": rb["method"],
                        "http_path": rb["http_path"],
                        "rel_path": rb["rel_path"],
                        "from_handler": ra["handler_name"],
                        "to_handler": rb["handler_name"],
                    }
                )

        return {
            "added": added,
            "removed": removed,
            "moved": moved,
            "handler_changed": handler_changed,
        }

    # -------------------- meta helpers --------------------

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
