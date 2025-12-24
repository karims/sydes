from __future__ import annotations

from typing import Iterable, List

from sydes.testgen.specs import EndpointSpec


def normalize_routes(
    rows: Iterable[dict],
    framework: str,
) -> List[EndpointSpec]:
    """
    Convert DB route rows into normalized EndpointSpec objects.
    """

    specs: list[EndpointSpec] = []

    for r in rows:
        specs.append(
            EndpointSpec(
                method=r["method"].upper(),
                path=r["http_path"],
                handler=r["handler_name"],
                file_path=r["rel_path"],
                framework=framework,
                source=r.get("source", "unknown"),
            )
        )

    return specs
