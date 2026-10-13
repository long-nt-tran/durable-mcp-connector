from in_workflow_client import coerce_call_tool_result


def test_object_becomes_structured_content_and_text():
    result = coerce_call_tool_result({"message": "hi"})
    assert result.structured_content == {"message": "hi"}
    assert '"message": "hi"' in result.content[0].text


def test_string_becomes_text():
    result = coerce_call_tool_result("hello")
    assert result.content[0].text == "hello"
    assert result.structured_content is None
