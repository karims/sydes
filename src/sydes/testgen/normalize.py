from __future__ import annotations

import hashlib
import re
from typing import Iterable, List

from sydes.testgen.specs import EndpointSpec


_PARAM_ANGLE = re.compile(r"<([A-Za-z_][A-Za-z0-9_]*)>")
_PARAM_COLON = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")
_MULTI_SLASH = re.compile(r"/{2,}")


def _sha1_hex(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _normalize_file_path(p: str) -> str:
    # repo-relative, forward slashes, no leading "./"
    p = (p or "").strip().replace("\\", "/")
    if p.startswith("./"):
        p = p[2:]
    return p


def _normalize_path(path: str) -> str:
    p = (path or "").strip()
    if not p.startswith("/"):
        p = "/" + p

    # normalize common param styles into "{param}"
    p = _PARAM_ANGLE.sub(r"{\1}", p)     # <id> -> {id}
    p = _PARAM_COLON.sub(r"{\1}", p)     # :id  -> {id}

    # collapse accidental double slashes
    p = _MULTI_SLASH.sub("/", p)

    # keep "/" as-is, otherwise strip trailing slash for stability
    if p != "/" and p.endswith("/"):
        p = p[:-1]
    return p


def _fallback_handler(method: str, path: str) -> str:
    # deterministic fallback if handler missing
    # e.g. GET /users/{id} -> get_users_by_id
    tokens = []
    for seg in path.strip("/").split("/"):
        if not seg:
            continue
        if seg.startswith("{") and seg.endswith("}"):
            tokens.append(f"by_{seg[1:-1]}")
        else:
            tokens.append(seg)
    base = "_".join(tokens) if tokens else "root"
    return f"{method.lower()}_{base}"


def normalize_routes(rows: Iterable[dict], framework: str) -> List[EndpointSpec]:
    """
    Convert DB route rows into normalized EndpointSpec objects.

    Determinism guarantees:
    - normalized method/path/file_path
    - stable endpoint id
    - stable ordering
    """
    specs: list[EndpointSpec] = []

    for r in rows:
        method = str(r.get("method", "")).upper().strip() or "GET"
        path = _normalize_path(str(r.get("http_path", "") or r.get("path", "")))
        file_path = _normalize_file_path(str(r.get("rel_path", "") or r.get("file_path", "")))

        handler = str(r.get("handler_name", "") or r.get("handler", "")).strip()
        if not handler:
            handler = _fallback_handler(method, path)

        source = str(r.get("source", "unknown")).strip() or "unknown"

        stable_id = _sha1_hex(f"{method}:{path}:{file_path}:{handler}")

        specs.append(
            EndpointSpec(
                id=stable_id,
                method=method,
                path=path,
                handler=handler,
                file_path=file_path,
                framework=framework,
                source=source,
                router_prefix=r.get("router_prefix"),
                tags=tuple(r.get("tags", ())) if r.get("tags") else (),
            )
        )

    # stable ordering = stable generated output
    specs.sort(key=lambda s: (s.file_path, s.path, s.method, s.handler, s.id))
    return specs
