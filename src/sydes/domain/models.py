from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

HttpMethod = Literal["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]


class ParamSpec(BaseModel):
    name: str
    type: str = "unknown"
    optional: bool = False


class ResponseSpec(BaseModel):
    status_code: int
    description: str = ""
    example_shape: Optional[dict[str, Any]] = None


class SideEffectSpec(BaseModel):
    type: str  # e.g. db_read/db_write/external_call/queue_publish
    target: str = ""
    description: str = ""


class EndpointSpec(BaseModel):
    method: HttpMethod
    path: str
    handler_name: str = ""
    file_path: str = ""
    auth_required: Optional[bool] = None

    path_params: list[ParamSpec] = Field(default_factory=list)
    query_params: list[ParamSpec] = Field(default_factory=list)
    body_schema: dict[str, Any] = Field(default_factory=dict)

    responses: list[ResponseSpec] = Field(default_factory=list)
    side_effects: list[SideEffectSpec] = Field(default_factory=list)
