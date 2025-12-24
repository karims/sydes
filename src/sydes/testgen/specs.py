from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class EndpointSpec:
    """
    Canonical representation of a single API endpoint.

    This is the normalized form used by test planning and generation.
    """

    method: str                 # GET, POST, ...
    path: str                   # /users/{id}
    handler: str                # function or handler name
    file_path: str              # relative path in repo
    framework: str              # fastapi, express, ...
    source: str                 # ast, regex, etc.

    # Optional / future fields
    router_prefix: Optional[str] = None
    tags: tuple[str, ...] = ()
