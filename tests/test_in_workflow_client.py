from in_workflow_client import InWorkflowClient, coerce_call_tool_result


def test_object_becomes_structured_content_and_text():
    result = coerce_call_tool_result({"message": "hi"})
    assert result.structured_content == {"message": "hi"}
    assert '"message": "hi"' in result.content[0].text


def test_string_becomes_text():
    result = coerce_call_tool_result("hello")
    assert result.content[0].text == "hello"
    assert result.structured_content is None


def _recording_client(manifests):
    """InWorkflowClient with _execute replaced by a recorder, so no Workflow is needed."""
    client = InWorkflowClient({service: f"{service}-endpoint" for service in manifests})
    calls = []

    async def execute(service, endpoint, operation, argument, *, timeout=None, summary=None):
        calls.append((service, operation, argument, summary))
        return manifests[service] if operation == "list_tools" else "ok"

    client._execute = execute
    return client, calls


async def test_call_tool_uses_the_dispatch_operation():
    client, calls = _recording_client({
        "proxy": {"tools": [{"name": "search", "inputSchema": {"type": "object"}}],
                  "dispatch": {"operation": "call_tool"}},
        "inbound": {"tools": [{"name": "lookup", "inputSchema": {"type": "object"}}]},
    })
    await client.list_tools()
    await client.call_tool("search", {"q": "bug"})
    await client.call_tool("lookup", {"k": 1})
    assert calls[-2] == ("proxy", "call_tool", {"name": "search", "arguments": {"q": "bug"}}, "search")
    assert calls[-1] == ("inbound", "lookup", {"k": 1}, "lookup")
