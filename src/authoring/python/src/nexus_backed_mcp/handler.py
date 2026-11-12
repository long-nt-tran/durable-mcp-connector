"""Handler APIs that expose Nexus operations as MCP tools.

Import this module only in handler Worker code. Workflow code imports
``nexus_backed_mcp``.

- ``@service_handler`` on the service handler, below
  ``@nexusrpc.handler.service_handler``. It implements ``list_tools``.
- ``@tool(...)`` on each operation to expose as a tool, above the Nexus operation
  decorator. It works for sync and async operations. Operations without it are not
  tools.
- ``@service_handler(expose="all")`` exposes every operation as a tool. Then
  ``@exclude`` keeps one operation out, and ``@tool(...)`` only adds options.
- ``@tool_errors(...)`` below the Nexus operation decorator turns the named
  exceptions into MCP error results.

To keep state across calls, return a handle from a create tool and take it as an
argument, as the MCP 2026-07-28 spec recommends.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal, TypeVar

import mcp.types
import nexusrpc
import nexusrpc.handler
import pydantic
from pydantic import TypeAdapter
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationResult,
    TemporalStartOperationContext,
    temporal_operation,
)

from . import LIST_TOOLS_OPERATION, RESULT_ENVELOPE_KEY, TIMEOUT_META_KEY, Manifest

__all__ = [
    "ToolError",
    "exclude",
    "service_handler",
    "tool",
    "tool_error_result",
    "tool_errors",
]

_MARKER = "__nexus_mcp_tool__"
_EXCLUDE_MARKER = "__nexus_mcp_exclude__"
_EXPOSE_ATTR = "__nexus_mcp_expose__"

C = TypeVar("C", bound=type)
F = TypeVar("F", bound=Callable[..., Any])


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

    Put it above the Nexus operation decorator. If ``description`` is unset or
    empty, the description is the method docstring. ``schedule_to_close_timeout``
    bounds each call of the tool. The caller sets it on the Nexus operation.
    """
    if schedule_to_close_timeout is not None and (
        not isinstance(schedule_to_close_timeout, timedelta)
        or schedule_to_close_timeout < timedelta()
    ):
        raise ValueError("schedule_to_close_timeout must be a non-negative timedelta")
    config = _ToolConfig(
        title, description, annotations, meta, schedule_to_close_timeout
    )

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


class ToolError(Exception):
    """Raise in a tool to return an MCP error result with this message."""


def tool_error_result(message: str) -> dict[str, Any]:
    """Return an operation result that the caller reads as an MCP error result."""
    return {
        RESULT_ENVELOPE_KEY: {
            "content": [{"type": "text", "text": message}],
            "isError": True,
        }
    }


def tool_errors(
    *exception_types: type[BaseException],
) -> Callable[[F], F]:
    """Turn the named exceptions into MCP error results. The default is ``ToolError``.

    Put it below the Nexus operation decorator. The exception message becomes the
    error text, so the model can read it and retry. Other exceptions fail the
    operation as before. It applies to the start method only. A failure in a
    Workflow that backs an async operation fails the operation.
    """
    caught = exception_types or (ToolError,)
    if not all(isinstance(t, type) and issubclass(t, BaseException) for t in caught):
        raise TypeError("tool_errors takes exception types")

    def decorate(fn: F) -> F:
        if nexusrpc.get_operation(fn) is not None:
            raise TypeError(
                "nexus_mcp.tool_errors must be below the Nexus operation decorator"
            )
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("nexus_mcp.tool_errors needs an async start method")

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except caught as e:
                return TemporalOperationResult.sync(tool_error_result(str(e)))

        return wrapper  # type: ignore[return-value]

    return decorate


def service_handler(
    cls: C | None = None, /, *, expose: Literal["marked", "all"] = "marked"
) -> Any:
    """Implement the ``list_tools`` operation. Put it below
    ``@nexusrpc.handler.service_handler``.

    ``expose="marked"`` (default) exposes only operations with ``@tool``.
    ``expose="all"`` exposes every operation except those with ``@exclude``.
    Tool names and input schemas are checked when the class is built.
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
    _validate_tools(cls, expose == "all")

    @temporal_operation
    async def list_tools(
        self,
        ctx: TemporalStartOperationContext,
        client: TemporalNexusClient,
        input: None,
    ) -> TemporalOperationResult[Manifest]:
        """Return the tools that this service exposes."""
        # The service definition is linked to the handler only after
        # @nexusrpc.handler.service_handler runs, so the manifest is built here.
        return TemporalOperationResult.sync(_manifest_for(type(self)))

    setattr(cls, LIST_TOOLS_OPERATION, list_tools)
    setattr(cls, _EXPOSE_ATTR, expose)
    return cls


def _is_tool(method: Any, expose_all: bool) -> bool:
    if getattr(method, _EXCLUDE_MARKER, False):
        return False
    return expose_all or isinstance(getattr(method, _MARKER, None), _ToolConfig)


def _validate_tools(cls: type, expose_all: bool) -> None:
    """Check tool names and input schemas from the handler methods."""
    for method in vars(cls).values():
        op = nexusrpc.get_operation(method)
        if op is None or not _is_tool(method, expose_all):
            continue
        _check_tool(op.name, op.input_type)


def _check_tool(name: str, input_type: Any) -> dict[str, Any] | None:
    if (
        not name.strip()
        or name == LIST_TOOLS_OPERATION
        or name.startswith("__temporal_")
    ):
        raise ValueError(f"Tool name {name!r} must be non-empty and non-reserved")
    schema = _object_schema(input_type)
    if schema is None and input_type not in (None, Any, type(None)):
        raise ValueError(f"Tool {name!r} must have an object input schema")
    return schema


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
        if not _is_tool(method, expose_all):
            continue
        config = getattr(method, _MARKER, None)
        if not isinstance(config, _ToolConfig):
            config = _ToolConfig()
        input_schema = _check_tool(op.name, op.input_type)

        description = config.description
        if (not description or not description.strip()) and method is not None:
            description = inspect.cleandoc(method.__doc__) if method.__doc__ else None
        meta = dict(config.meta or {})
        if config.schedule_to_close_timeout is not None:
            meta[TIMEOUT_META_KEY] = int(
                config.schedule_to_close_timeout.total_seconds() * 1000
            )
        mcp_tool = mcp.types.Tool(
            name=op.name,
            title=config.title,
            description=description or None,
            input_schema=input_schema or {"type": "object", "properties": {}},
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
