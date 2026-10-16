"""Temporal Agent Harness agent and its Worker. The agent calls the tools with Workflow
Nexus operations through in_workflow_client.

Run from the examples/ directory:
    just agent-worker
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import timedelta

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream

# The Workflow sandbox re-imports this file. These modules are not Workflow code.
with workflow.unsafe.imports_passed_through():
    from agents import Agent, Runner, TResponseInputItem
    from in_workflow_client.openai_agents import WorkflowNexusMCPServer
    from temporal_agent_harness.ai_sdks.openai_agents import (
        ModelActivityParameters,
        OpenAIAgentsPlugin,
    )
    from temporal_agent_harness.ai_sdks.openai_agents_harness import (
        as_harness_mcp_server,
        harness_observer_factory,
        mark_durable_mcp_server,
        stream_to_provider,
    )
    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner
    from temporal_agent_harness.plugin import AgentHarnessPlugin
    from temporalio.client import Client
    from temporalio.envconfig import ClientConfig
    from temporalio.worker import Worker

    from examples.mcp_client.servers import (
        NEXUS_BACKED_MCP_SERVER,
        NEXUS_BACKED_MCP_SERVER_ENDPOINT,
        NEXUS_PROXY_MCP_SERVER,
        NEXUS_PROXY_MCP_SERVER_ENDPOINT,
    )

TASK_QUEUE = "nexus-tools-agent"


@agent.defn(name="NexusToolsAgent")
class NexusToolsAgentWorkflow:
    """A conversational agent with the tools of both example MCP servers."""

    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.always_require_human_approval(),
        )
        self._conversation: list[TResponseInputItem] = []

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Answer one user message. The agent may call the tools of both servers."""
        # Each tool call is a Workflow Nexus operation, so the server is durable. The
        # harness accepts only MCP servers that are marked durable.
        sdk_agent = Agent(
            name="Assistant",
            instructions="You are a friendly assistant. Answer in brief, natural prose. Execute tools when requested.",
            model="gpt-5.1",
            mcp_servers=[
                as_harness_mcp_server(
                    mark_durable_mcp_server(
                        WorkflowNexusMCPServer(
                            service=NEXUS_BACKED_MCP_SERVER,
                            endpoint=NEXUS_BACKED_MCP_SERVER_ENDPOINT,
                        )
                    ),
                    self._runner,
                ),
                as_harness_mcp_server(
                    mark_durable_mcp_server(
                        WorkflowNexusMCPServer(
                            service=NEXUS_PROXY_MCP_SERVER,
                            endpoint=NEXUS_PROXY_MCP_SERVER_ENDPOINT,
                        )
                    ),
                    self._runner,
                ),
            ],
        )
        result = Runner.run_streamed(
            sdk_agent,
            input=[*self._conversation, {"role": "user", "content": message.text}],
            context=self._runner,
        )
        async for _ in result.stream_events():
            pass
        self._conversation = result.to_input_list()
        return TextReply(text=str(result.final_output))


async def main() -> None:
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("error: set OPENAI_API_KEY")
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=timedelta(seconds=30),
            stream_to_provider=stream_to_provider,
        ),
        observer_factory=harness_observer_factory,
    )
    # The harness plugin goes last so that its data converter settings apply on top.
    client = await Client.connect(
        **ClientConfig.load_client_connect_config(),
        plugins=[plugin, AgentHarnessPlugin()],
    )
    worker = Worker(client, task_queue=TASK_QUEUE, workflows=[NexusToolsAgentWorkflow])
    print(f"Agent worker ready: taskQueue={TASK_QUEUE!r}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
