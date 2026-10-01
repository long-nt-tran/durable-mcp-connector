"""Nexus-backed MCP server with one short tool and one long tool.

Run from the examples/ directory:
    just nexus-backed
"""

from __future__ import annotations

import asyncio
import random

from pydantic import BaseModel
from temporalio import workflow

# The Workflow sandbox re-imports this file. These modules are not Workflow code.
with workflow.unsafe.imports_passed_through():
    import nexus_backed_mcp as nexus_mcp
    import nexusrpc
    import nexusrpc.handler
    import temporalio.nexus
    from mcp.types import ToolAnnotations
    from temporalio.client import Client
    from temporalio.contrib.pydantic import pydantic_data_converter
    from temporalio.envconfig import ClientConfig
    from temporalio.worker import Worker

TASK_QUEUE = "lucky-number-tools"


class LuckyNumberInput(BaseModel):
    topic: str


class DelayedLuckyNumberInput(BaseModel):
    topic: str
    delay_seconds: float = 5.0


class DelayedLuckyNumberOutput(BaseModel):
    message: str


@workflow.defn
class DelayedLuckyNumberWorkflow:
    """Back the long tool. Wait on a durable timer, then return a lucky number."""

    @workflow.run
    async def run(self, input: DelayedLuckyNumberInput) -> DelayedLuckyNumberOutput:
        await workflow.sleep(input.delay_seconds)
        number = workflow.random().randint(1, 100)
        return DelayedLuckyNumberOutput(message=f"{input.topic}'s delayed lucky number is {number}.")


# Service definition. Non-MCP Nexus callers use it too.
@nexusrpc.service(name="lucky-number-tools")
@nexus_mcp.service
class LuckyNumberService:
    get_lucky_number: nexusrpc.Operation[LuckyNumberInput, str]
    get_delayed_lucky_number: nexusrpc.Operation[DelayedLuckyNumberInput, DelayedLuckyNumberOutput]


@nexusrpc.handler.service_handler(service=LuckyNumberService)
@nexus_mcp.service_handler
class LuckyNumberTools:
    # Short tool: a sync Nexus operation.
    @nexus_mcp.tool(title="Get a lucky number")
    @nexusrpc.handler.sync_operation
    async def get_lucky_number(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LuckyNumberInput
    ) -> str:
        """Return a lucky number for a topic."""
        return f"{input.topic}'s lucky number today is {random.randint(1, 100)}."

    # Long tool: a Workflow-backed Nexus operation. The call can outlast a client timeout.
    @nexus_mcp.tool(
        title="Get a delayed lucky number",
        annotations=ToolAnnotations(read_only_hint=True, idempotent_hint=True),
    )
    @temporalio.nexus.workflow_run_operation
    async def get_delayed_lucky_number(
        self, ctx: temporalio.nexus.WorkflowRunOperationContext, input: DelayedLuckyNumberInput
    ) -> temporalio.nexus.WorkflowHandle[DelayedLuckyNumberOutput]:
        """Return a lucky number for a topic after a delay."""
        # request_id is stable across Nexus start retries, so a retry reuses the Workflow.
        return await ctx.start_workflow(
            DelayedLuckyNumberWorkflow.run, input, id=f"delayed-lucky-number-{ctx.request_id}"
        )


async def main() -> None:
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        data_converter=pydantic_data_converter,
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DelayedLuckyNumberWorkflow],
        nexus_service_handlers=[LuckyNumberTools()],
    )
    print(f"MCP server ready: service='lucky-number-tools' taskQueue={TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
