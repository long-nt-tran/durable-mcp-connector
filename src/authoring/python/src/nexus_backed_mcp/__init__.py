"""Expose Nexus operations as MCP tools.

This module is safe to import in Workflow code. It holds the service-definition
decorator and shared constants. The handler APIs are in ``nexus_backed_mcp.handler``,
so handler dependencies stay out of the Workflow sandbox.

Import as ``import nexus_backed_mcp as nexus_mcp`` and
``import nexus_backed_mcp.handler``. The author keeps the nexusrpc decorators.
These decorators go below them:

- ``@nexus_mcp.service`` on the service definition. It declares the ``list_tools``
  operation.
- ``@nexus_backed_mcp.handler.service_handler`` on the service handler. It
  implements ``list_tools``.
- ``@nexus_backed_mcp.handler.tool(...)`` on each operation to expose as a tool,
  above the Nexus operation decorator.

The tool name is the Nexus operation name.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar

import nexusrpc

__all__ = [
    "LIST_TOOLS_OPERATION",
    "RESULT_ENVELOPE_KEY",
    "Manifest",
    "service",
]

LIST_TOOLS_OPERATION = "list_tools"
# An operation result with only this key is a complete MCP CallToolResult. The
# connector and the in-Workflow client return it unchanged.
RESULT_ENVELOPE_KEY = "io.temporal/mcpCallToolResult"

C = TypeVar("C", bound=type)


@dataclass
class Manifest:
    """Output of the ``list_tools`` operation."""

    tools: list[dict[str, Any]] = field(default_factory=list)
    """MCP tool definitions. Each tool name is a Nexus operation name."""


def service(cls: C) -> C:
    """Declare the ``list_tools`` operation. Put it below ``@nexusrpc.service``."""
    if nexusrpc.get_service_definition(cls) is not None:
        raise TypeError(
            f"{cls.__name__}: put @nexus_mcp.service below @nexusrpc.service"
        )
    if LIST_TOOLS_OPERATION in cls.__dict__ or LIST_TOOLS_OPERATION in getattr(
        cls, "__annotations__", {}
    ):
        raise TypeError(
            f"{cls.__name__} must not declare {LIST_TOOLS_OPERATION}; nexus_mcp.service adds it"
        )
    setattr(
        cls,
        LIST_TOOLS_OPERATION,
        nexusrpc.Operation(
            name=LIST_TOOLS_OPERATION, input_type=type(None), output_type=Manifest
        ),
    )
    return cls
