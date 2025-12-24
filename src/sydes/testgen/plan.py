from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from sydes.testgen.specs import EndpointSpec


_SAFE = re.compile(r"[^a-zA-Z0-9_]+")


@dataclass(frozen=True)
class EndpointTestPlan:
    endpoint_id: str
    test_name: str
    method: str
    path: str


@dataclass(frozen=True)
class TestFilePlan:
    rel_path: str              # e.g. tests/generated/test_users.py
    module_key: str            # stable grouping key (e.g. routers/users.py)
    endpoints: Tuple[EndpointTestPlan, ...]


@dataclass(frozen=True)
class TestPlan:
    generated_root: str
    files: Tuple[TestFilePlan, ...]


def _sha1_short(text: str, n: int = 6) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:n]


def _stem_from_file_path(file_path: str) -> str:
    # routers/users.py -> users
    base = (file_path or "api").replace("\\", "/").split("/")[-1]
    if base.endswith(".py"):
        base = base[:-3]
    base = base.strip() or "api"
    base = _SAFE.sub("_", base).strip("_").lower()
    return base or "api"


def _test_name(method: str, path: str) -> str:
    # GET /users/{id} -> test_get_users_by_id
    parts: List[str] = []
    for seg in path.strip("/").split("/"):
        if not seg:
            continue
        if seg.startswith("{") and seg.endswith("}"):
            parts.append(f"by_{seg[1:-1]}")
        else:
            parts.append(seg)

    body = "_".join(parts) if parts else "root"
    body = _SAFE.sub("_", body).strip("_").lower()
    return f"test_{method.lower()}_{body}"


def build_test_plan(
    specs: Iterable[EndpointSpec],
    generated_root: str = "tests/generated",
) -> TestPlan:
    """
    Phase 4.2: EndpointSpec -> TestPlan (grouping + naming only; no IO).
    Deterministic by construction.
    """
    by_module: Dict[str, List[EndpointSpec]] = {}

    for s in specs:
        key = s.file_path or "unknown"
        by_module.setdefault(key, []).append(s)

    # deterministic file ordering
    module_keys = sorted(by_module.keys())

    used_filenames: Dict[str, str] = {}
    file_plans: List[TestFilePlan] = []

    for module_key in module_keys:
        endpoints = by_module[module_key]
        endpoints.sort(key=lambda e: (e.path, e.method, e.handler, e.id))

        stem = _stem_from_file_path(module_key)
        filename = f"test_{stem}.py"

        # collision-safe filenames
        if filename in used_filenames and used_filenames[filename] != module_key:
            filename = f"test_{stem}__{_sha1_short(module_key)}.py"
        used_filenames[filename] = module_key

        rel_path = f"{generated_root.rstrip('/')}/{filename}"

        # deterministic test name collision handling
        counts: Dict[str, int] = {}
        planned: List[EndpointTestPlan] = []
        for e in endpoints:
            base = _test_name(e.method, e.path)
            counts.setdefault(base, 0)
            counts[base] += 1
            name = base if counts[base] == 1 else f"{base}_{counts[base]}"
            planned.append(
                EndpointTestPlan(
                    endpoint_id=e.id,
                    test_name=name,
                    method=e.method,
                    path=e.path,
                )
            )

        file_plans.append(
            TestFilePlan(
                rel_path=rel_path,
                module_key=module_key,
                endpoints=tuple(planned),
            )
        )

    return TestPlan(generated_root=generated_root, files=tuple(file_plans))
