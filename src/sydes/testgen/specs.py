from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class EndpointSpec:
    """
    Canonical representation of a single API endpoint.

    Deterministic, minimal contract used by Phase 4 planning + generation.
    """

    id: str                     # stable identity (sha1)
    method: str                 # GET, POST, ...
    path: str                   # /users/{id}
    handler: str                # best-effort handler name (can be derived)
    file_path: str              # repo-relative path (forward slashes)
    framework: str              # fastapi, express, ...
    source: str                 # ast, regex, etc.

    # Optional / future fields
    router_prefix: Optional[str] = None
    tags: tuple[str, ...] = ()
