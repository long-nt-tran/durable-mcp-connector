"""Nexus-backed MCP server with a short tool, a long tool, and two handle tools.

Run from the examples/ directory:
    just nexus-backed
"""

from __future__ import annotations

import asyncio
import random
import secrets
from datetime import timedelta

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
    from temporalio.service import RPCError, RPCStatusCode
    from temporalio.worker import Worker

TASK_QUEUE = "lucky-number-tools"


class LuckyNumberInput(BaseModel):
    topic: str


class DelayedLuckyNumberInput(BaseModel):
    topic: str
    delay_seconds: float = 5.0


class DelayedLuckyNumberOutput(BaseModel):
    message: str


class CreateTopicListInput(BaseModel):
    pass


class CreateTopicListOutput(BaseModel):
    list_id: str


class RememberTopicInput(BaseModel):
    list_id: str
    topic: str


class RememberTopicOutput(BaseModel):
    list_id: str
    topics: list[str]


@workflow.defn
class DelayedLuckyNumberWorkflow:
    """Back the long tool. Wait on a durable timer, then return a lucky number."""

    @workflow.run
    async def run(self, input: DelayedLuckyNumberInput) -> DelayedLuckyNumberOutput:
        await workflow.sleep(input.delay_seconds)
        number = workflow.random().randint(1, 100)
        return DelayedLuckyNumberOutput(message=f"{input.topic}'s delayed lucky number is {number}.")


@workflow.defn
class TopicListWorkflow:
    """Back the handle tools. Hold the topics of one list. End after 30 idle minutes."""

    def __init__(self) -> None:
        self._topics: list[str] = []
        self._touched = False

    @workflow.run
    async def run(self) -> None:
        while True:
            self._touched = False
            try:
                await workflow.wait_condition(lambda: self._touched, timeout=timedelta(minutes=30))
            except asyncio.TimeoutError:
                await workflow.wait_condition(workflow.all_handlers_finished)
                return

    @workflow.update
    def add(self, topic: str) -> list[str]:
        self._topics.append(topic)
        self._touched = True
        return list(self._topics)


# Service definition. Non-MCP Nexus callers use it too.
@nexusrpc.service(name="lucky-number-tools")
@nexus_mcp.service
class LuckyNumberService:
    get_lucky_number: nexusrpc.Operation[LuckyNumberInput, str]
    get_delayed_lucky_number: nexusrpc.Operation[DelayedLuckyNumberInput, DelayedLuckyNumberOutput]
    create_topic_list: nexusrpc.Operation[CreateTopicListInput, CreateTopicListOutput]
    remember_topic: nexusrpc.Operation[RememberTopicInput, RememberTopicOutput]


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
        # Each call fails if it does not complete in 10 minutes.
        schedule_to_close_timeout=timedelta(minutes=10),
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

    # Handle tools: they share state through an explicit handle, as in the MCP 2026-07-28
    # spec. The list is in one Workflow per handle, not in this Worker, so it survives
    # Worker restarts.
    @nexus_mcp.tool(title="Create a topic list")
    @nexusrpc.handler.sync_operation
    async def create_topic_list(
        self, ctx: nexusrpc.handler.StartOperationContext, input: CreateTopicListInput
    ) -> CreateTopicListOutput:
        """Create an empty topic list and return its list_id.

        Pass the list_id to remember_topic. A list expires after 30 idle minutes.
        """
        # The handle is the only check, so it has 128 random bits and cannot be guessed.
        list_id = "tl_" + secrets.token_urlsafe(16)
        await temporalio.nexus.client().start_workflow(
            TopicListWorkflow.run, id=_list_workflow_id(list_id), task_queue=TASK_QUEUE
        )
        return CreateTopicListOutput(list_id=list_id)

    @nexus_mcp.tool(title="Remember a topic")
    @nexusrpc.handler.sync_operation
    async def remember_topic(
        self, ctx: nexusrpc.handler.StartOperationContext, input: RememberTopicInput
    ) -> RememberTopicOutput:
        """Add a topic to a topic list and return all topics in the list.

        Get list_id from create_topic_list.
        """
        handle = temporalio.nexus.client().get_workflow_handle(_list_workflow_id(input.list_id))
        try:
            topics = await handle.execute_update(TopicListWorkflow.add, input.topic)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND:
                raise
            # The Workflow is unknown or closed. A closed Workflow is an expired list.
            raise nexusrpc.HandlerError(
                f"Topic list {input.list_id} does not exist or has expired. "
                "Call create_topic_list to make a new list.",
                type=nexusrpc.HandlerErrorType.NOT_FOUND,
            ) from e
        return RememberTopicOutput(list_id=input.list_id, topics=topics)


def _list_workflow_id(list_id: str) -> str:
    return f"topic-list-{list_id}"


async def main() -> None:
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        data_converter=pydantic_data_converter,
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DelayedLuckyNumberWorkflow, TopicListWorkflow],
        nexus_service_handlers=[LuckyNumberTools()],
    )
    print(f"MCP server ready: service='lucky-number-tools' taskQueue={TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
