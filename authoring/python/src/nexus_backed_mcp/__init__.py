"""Expose Nexus operations as MCP tools.

Import as ``import nexus_backed_mcp as nexus_mcp``. The author keeps the
nexusrpc decorators. These decorators go below them:

- ``@nexus_mcp.service`` on the service definition. It declares the ``list_tools``
  operation.
- ``@nexus_mcp.service_handler`` on the service handler. It implements
  ``list_tools``.
- ``@nexus_mcp.tool(...)`` on each operation to expose as a tool, above the Nexus
  operation decorator. It works for sync and async operations. Operations without
  it are not tools.

The tool name is the Nexus operation name.
"""

from __future__ import annotations

import functools
import inspect
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

import mcp.types
import nexusrpc
import nexusrpc.handler
import pydantic
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel, Field, TypeAdapter

__all__ = ["LIST_TOOLS_OPERATION", "Manifest", "service", "service_handler", "tool"]

LIST_TOOLS_OPERATION = "list_tools"
_MARKER = "__nexus_mcp_tool__"
# Common LLM APIs accept only these tool names.
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
# The connector adds tools with these names.
_RESERVED_NAMES = frozenset({"get_operation_result", "cancel_operation"})

C = TypeVar("C", bound=type)
F = TypeVar("F", bound=Callable[..., Any])


class Manifest(BaseModel):
    """Output of the ``list_tools`` operation."""

    tools: list[dict[str, Any]] = Field(default_factory=list)
    """MCP tool definitions. Each tool name is a Nexus operation name."""


@dataclass(frozen=True)
class _ToolConfig:
    title: str | None = None
    description: str | None = None
    annotations: mcp.types.ToolAnnotations | None = None
    meta: Mapping[str, Any] | None = None


def tool(
    *,
    title: str | None = None,
    description: str | None = None,
    annotations: mcp.types.ToolAnnotations | None = None,
    meta: Mapping[str, Any] | None = None,
) -> Callable[[F], F]:
    """Mark a sync or async Nexus operation as an MCP tool.

    Put it above the Nexus operation decorator. The description defaults to the
    method docstring.
    """
    config = _ToolConfig(title, description, annotations, meta)

    def decorate(op: F) -> F:
        if nexusrpc.get_operation(op) is None:
            raise TypeError("nexus_mcp.tool must be above a Nexus operation decorator")
        setattr(op, _MARKER, config)
        return op

    return decorate


def service(cls: C) -> C:
    """Declare the ``list_tools`` operation. Put it below ``@nexusrpc.service``."""
    if nexusrpc.get_service_definition(cls) is not None:
        raise TypeError(f"{cls.__name__}: put @nexus_mcp.service below @nexusrpc.service")
    if LIST_TOOLS_OPERATION in cls.__dict__ or LIST_TOOLS_OPERATION in getattr(cls, "__annotations__", {}):
        raise TypeError(f"{cls.__name__} must not declare {LIST_TOOLS_OPERATION}; nexus_mcp.service adds it")
    setattr(
        cls,
        LIST_TOOLS_OPERATION,
        nexusrpc.Operation(name=LIST_TOOLS_OPERATION, input_type=type(None), output_type=Manifest),
    )
    return cls


def service_handler(cls: C) -> C:
    """Implement the ``list_tools`` operation. Put it below
    ``@nexusrpc.handler.service_handler``."""
    if nexusrpc.get_service_definition(cls) is not None:
        raise TypeError(
            f"{cls.__name__}: put @nexus_mcp.service_handler below @nexusrpc.handler.service_handler"
        )

    @nexusrpc.handler.sync_operation
    async def list_tools(self: Any, ctx: StartOperationContext, input: None) -> Manifest:
        """Return the tools that this service exposes."""
        # The service definition is linked to the handler only after
        # @nexusrpc.handler.service_handler runs, so the manifest is built here.
        return _manifest_for(type(self))

    setattr(cls, LIST_TOOLS_OPERATION, list_tools)
    return cls


@functools.cache
def _manifest_for(handler_class: type) -> Manifest:
    defn = nexusrpc.get_service_definition(handler_class)
    if defn is None:
        raise ValueError(f"{handler_class.__name__} is not a Nexus service handler")
    tools: list[dict[str, Any]] = []
    for op in defn.operation_definitions.values():
        if op.name == LIST_TOOLS_OPERATION:
            continue
        method = getattr(handler_class, op.method_name or op.name, None)
        config = getattr(method, _MARKER, None)
        if not isinstance(config, _ToolConfig):
            continue
        if not _NAME_RE.match(op.name):
            raise ValueError(f"Tool name {op.name!r} must match {_NAME_RE.pattern}")
        if op.name in _RESERVED_NAMES:
            raise ValueError(f"Tool name {op.name!r} is reserved by the connector")

        description = config.description
        if description is None and method.__doc__:
            description = inspect.cleandoc(method.__doc__)
        mcp_tool = mcp.types.Tool(
            name=op.name,
            title=config.title,
            description=description,
            input_schema=_object_schema(op.input_type) or {"type": "object", "properties": {}},
            output_schema=_object_schema(op.output_type),
            annotations=config.annotations,
            _meta=dict(config.meta) if config.meta is not None else None,
        )
        tools.append(mcp_tool.model_dump(mode="json", by_alias=True, exclude_none=True))
    return Manifest(tools=tools)


def _object_schema(annotation: Any) -> dict[str, Any] | None:
    if annotation in (None, Any, type(None)):
        return None
    try:
        schema = TypeAdapter(annotation).json_schema()
    except (pydantic.PydanticUserError, TypeError, ValueError):
        return None
    return schema if schema.get("type") == "object" else None
