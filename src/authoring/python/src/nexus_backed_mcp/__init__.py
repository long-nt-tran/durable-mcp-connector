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

``@nexus_mcp.service_handler(expose="all")`` exposes every operation as a tool. Then
``@nexus_mcp.exclude`` keeps one operation out, and ``@nexus_mcp.tool(...)`` only adds
options.

The tool name is the Nexus operation name.

To keep state across calls, return a handle from a create tool and take it as an
argument, as the MCP 2026-07-28 spec recommends.

``nexus_mcp.session_id(ctx)`` is legacy. It returns the MCP session ID of a call, or
``None`` if the caller has no session. MCP 2026-07-28 has no sessions.
"""

from __future__ import annotations

import functools
import inspect
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, TypeVar

import mcp.types
import nexusrpc
import nexusrpc.handler
import pydantic
from nexusrpc.handler import StartOperationContext
from pydantic import BaseModel, Field, TypeAdapter

__all__ = [
    "LIST_TOOLS_OPERATION",
    "SESSION_HEADER",
    "TIMEOUT_META_KEY",
    "Manifest",
    "exclude",
    "service",
    "service_handler",
    "session_id",
    "tool",
]

LIST_TOOLS_OPERATION = "list_tools"
# Nexus header that carries the MCP session ID. The connector and the Workflow adapter set it.
SESSION_HEADER = "temporal-mcp-session-id"
# Tool _meta key for the schedule-to-close timeout of the operation, in milliseconds.
# The connector and the in-Workflow client read it.
TIMEOUT_META_KEY = "io.temporal/scheduleToCloseTimeoutMs"
_MARKER = "__nexus_mcp_tool__"
_EXCLUDE_MARKER = "__nexus_mcp_exclude__"
_EXPOSE_ATTR = "__nexus_mcp_expose__"
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
    schedule_to_close_timeout: timedelta | None = None


def tool(
    *,
    title: str | None = None,
    description: str | None = None,
    annotations: mcp.types.ToolAnnotations | None = None,
    meta: Mapping[str, Any] | None = None,
    schedule_to_close_timeout: timedelta | None = None,
) -> Callable[[F], F]:
    """Mark a sync or async Nexus operation as an MCP tool.

    Put it above the Nexus operation decorator. The description defaults to the
    method docstring. ``schedule_to_close_timeout`` bounds each call of the tool.
    The caller sets it on the Nexus operation.
    """
    config = _ToolConfig(title, description, annotations, meta, schedule_to_close_timeout)

    def decorate(op: F) -> F:
        if nexusrpc.get_operation(op) is None:
            raise TypeError("nexus_mcp.tool must be above a Nexus operation decorator")
        setattr(op, _MARKER, config)
        return op

    return decorate


def exclude(op: F) -> F:
    """Keep an operation out of the tools of an ``expose="all"`` service handler.

    Put it above the Nexus operation decorator.
    """
    if nexusrpc.get_operation(op) is None:
        raise TypeError("nexus_mcp.exclude must be above a Nexus operation decorator")
    setattr(op, _EXCLUDE_MARKER, True)
    return op


def session_id(ctx: nexusrpc.handler.OperationContext) -> str | None:
    """Return the MCP session ID of the current call, or ``None`` if there is no session.

    Legacy. The connector sends a session ID only with ``--stateful``. The Workflow
    adapter sends the agent Workflow ID. MCP 2026-07-28 has no sessions.
    """
    for key, value in ctx.headers.items():
        if key.lower() == SESSION_HEADER and value:
            return value
    return None


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


def service_handler(
    cls: C | None = None, /, *, expose: Literal["marked", "all"] = "marked"
) -> Any:
    """Implement the ``list_tools`` operation. Put it below
    ``@nexusrpc.handler.service_handler``.

    ``expose="marked"`` (default) exposes only operations with ``@nexus_mcp.tool``.
    ``expose="all"`` exposes every operation except those with ``@nexus_mcp.exclude``.
    """
    if expose not in ("marked", "all"):
        raise ValueError(f"expose must be 'marked' or 'all', not {expose!r}")
    if cls is None:
        return lambda c: _service_handler(c, expose)
    return _service_handler(cls, expose)


def _service_handler(cls: C, expose: str) -> C:
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
    setattr(cls, _EXPOSE_ATTR, expose)
    return cls


@functools.cache
def _manifest_for(handler_class: type) -> Manifest:
    defn = nexusrpc.get_service_definition(handler_class)
    if defn is None:
        raise ValueError(f"{handler_class.__name__} is not a Nexus service handler")
    expose_all = getattr(handler_class, _EXPOSE_ATTR, "marked") == "all"
    tools: list[dict[str, Any]] = []
    for op in defn.operation_definitions.values():
        if op.name == LIST_TOOLS_OPERATION:
            continue
        method = getattr(handler_class, op.method_name or op.name, None)
        if getattr(method, _EXCLUDE_MARKER, False):
            continue
        config = getattr(method, _MARKER, None)
        if not isinstance(config, _ToolConfig):
            if not expose_all:
                continue
            config = _ToolConfig()
        if not _NAME_RE.match(op.name):
            raise ValueError(f"Tool name {op.name!r} must match {_NAME_RE.pattern}")
        if op.name in _RESERVED_NAMES:
            raise ValueError(f"Tool name {op.name!r} is reserved by the connector")

        description = config.description
        if description is None and method is not None and method.__doc__:
            description = inspect.cleandoc(method.__doc__)
        meta = dict(config.meta or {})
        if config.schedule_to_close_timeout is not None:
            meta[TIMEOUT_META_KEY] = int(config.schedule_to_close_timeout.total_seconds() * 1000)
        mcp_tool = mcp.types.Tool(
            name=op.name,
            title=config.title,
            description=description,
            input_schema=_object_schema(op.input_type) or {"type": "object", "properties": {}},
            output_schema=_object_schema(op.output_type),
            annotations=config.annotations,
            _meta=meta or None,
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
