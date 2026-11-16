"""Model client tests. No real SDK client is built: the Qwen client gets a fake
OpenAI client that records its arguments and returns a canned reply."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from agentos import worker
from agentos.config import ConfigError, Settings, load_settings
from agentos.llm import (
    QwenLLMClient,
    from_openai_response,
    to_openai_messages,
    to_openai_tools,
)
from tests.helpers.fake_openai import FakeOpenAI, _response, _tool_call

PROVIDER_VARS = (
    "AGENTOS_LLM_PROVIDER",
    "AGENTOS_MODEL",
    "AGENTOS_QWEN_THINKING",
    "QWEN_BASE_URL",
    "QWEN_API_KEY",
)


def _qwen(fake: FakeOpenAI, **kwargs) -> QwenLLMClient:
    return QwenLLMClient(
        api_key="unused", base_url="http://fake.invalid", model="qwen-test", client=fake, **kwargs
    )


TOOLS = [
    {
        "name": "search_items",
        "description": "Search items.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    }
]


async def test_qwen_request_carries_the_model_and_the_tools():
    """Fails if the Qwen request goes out without the configured model or the agent's tools."""
    fake = FakeOpenAI()
    await _qwen(fake).complete("sys", [{"role": "user", "content": "hi"}], TOOLS)
    assert fake.calls[0]["model"] == "qwen-test"
    assert fake.calls[0]["tools"] == to_openai_tools(TOOLS)


def test_openai_tools_shape():
    out = to_openai_tools(TOOLS)
    assert out == [
        {
            "type": "function",
            "function": {
                "name": "search_items",
                "description": "Search items.",
                "parameters": TOOLS[0]["input_schema"],
            },
        }
    ]


def test_openai_messages_round_trip_history():
    history = [
        {"role": "user", "content": "find my laptop request"},
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "let me search", "signature": "sig"},
                {"type": "text", "text": "Searching."},
                {"type": "tool_use", "id": "tu_1", "name": "search_items", "input": {"q": "x"}},
                {"type": "tool_use", "id": "tu_2", "name": "my_items", "input": {}},
            ],
        },
        {
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "tu_1", "content": "found 1"},
                {
                    "type": "tool_result",
                    "tool_use_id": "tu_2",
                    "content": [{"type": "text", "text": "bad "}, {"type": "text", "text": "id"}],
                    "is_error": True,
                },
                {"type": "text", "text": "thanks"},
            ],
        },
        {
            "role": "assistant",
            "content": [
                {"type": "redacted_thinking", "data": "xyz"},
                {"type": "text", "text": "Done."},
            ],
        },
    ]
    out = to_openai_messages("be helpful", history)
    assert out == [
        {"role": "system", "content": "be helpful"},
        {"role": "user", "content": "find my laptop request"},
        {
            "role": "assistant",
            "content": "Searching.",
            "tool_calls": [
                {
                    "id": "tu_1",
                    "type": "function",
                    "function": {"name": "search_items", "arguments": json.dumps({"q": "x"})},
                },
                {
                    "id": "tu_2",
                    "type": "function",
                    "function": {"name": "my_items", "arguments": "{}"},
                },
            ],
        },
        {"role": "tool", "tool_call_id": "tu_1", "content": "found 1"},
        {"role": "tool", "tool_call_id": "tu_2", "content": "Error: bad id"},
        {"role": "user", "content": "thanks"},
        {"role": "assistant", "content": "Done."},
    ]
    assert isinstance(out[2]["tool_calls"][0]["function"]["arguments"], str)


def test_from_openai_response_tool_calls():
    resp = _response(
        content=None,
        tool_calls=[
            _tool_call("call_1", "search_items", '{"query": "laptop"}'),
            _tool_call("call_2", "my_items", "{}"),
        ],
        finish_reason="tool_calls",
    )
    out = from_openai_response(resp)
    assert out.text is None
    assert [(c.id, c.name, c.input) for c in out.tool_calls] == [
        ("call_1", "search_items", {"query": "laptop"}),
        ("call_2", "my_items", {}),
    ]
    assert out.stop_reason == "tool_use"
    assert out.content == [
        {"type": "tool_use", "id": "call_1", "name": "search_items", "input": {"query": "laptop"}},
        {"type": "tool_use", "id": "call_2", "name": "my_items", "input": {}},
    ]


def test_from_openai_response_bad_arguments():
    resp = _response(
        tool_calls=[
            _tool_call("call_1", "search_items", "{not json"),
            _tool_call("call_2", "search_items", "[1, 2]"),
        ],
        finish_reason="tool_calls",
    )
    out = from_openai_response(resp)
    assert [(c.id, c.input) for c in out.tool_calls] == [("call_1", {}), ("call_2", {})]


def test_from_openai_response_text_only():
    resp = _response(content="  Hello there.  ", finish_reason="stop")
    resp.choices[0].message.reasoning_content = "private thoughts"
    out = from_openai_response(resp)
    assert out.text == "Hello there."
    assert out.tool_calls == []
    assert out.stop_reason == "end_turn"
    assert out.content == [{"type": "text", "text": "Hello there."}]
    assert from_openai_response(_response(content="x", finish_reason="length")).stop_reason == (
        "max_tokens"
    )


async def test_qwen_thinking_flag():
    fake = FakeOpenAI()
    await _qwen(fake).complete("sys", [{"role": "user", "content": "hi"}], [])
    assert fake.calls[0]["extra_body"] == {"enable_thinking": False}
    assert "tools" not in fake.calls[0]

    fake_on = FakeOpenAI()
    await _qwen(fake_on, thinking=True).complete("sys", [{"role": "user", "content": "hi"}], [])
    assert fake_on.calls[0]["extra_body"] == {"enable_thinking": True}


def test_settings_provider_defaults(monkeypatch):
    """Fails if the provider variable is ignored, read case-sensitively, or an unknown provider is accepted."""
    for name in PROVIDER_VARS:
        monkeypatch.delenv(name, raising=False)

    default = load_settings()
    assert default.llm_provider == "anthropic"
    assert default.qwen_thinking is False

    monkeypatch.setenv("AGENTOS_LLM_PROVIDER", "QWEN")
    monkeypatch.setenv("AGENTOS_QWEN_THINKING", "Yes")
    qwen = load_settings()
    assert qwen.llm_provider == "qwen"
    assert qwen.qwen_thinking is True

    monkeypatch.setenv("AGENTOS_LLM_PROVIDER", "openai")
    with pytest.raises(ConfigError) as exc:
        load_settings()
    assert "AGENTOS_LLM_PROVIDER" in str(exc.value)
    assert "anthropic" in str(exc.value) and "qwen" in str(exc.value)


def test_require_qwen_key_names_variable(monkeypatch):
    monkeypatch.delenv("QWEN_API_KEY", raising=False)
    settings = load_settings()
    with pytest.raises(RuntimeError) as exc:
        settings.require("qwen_api_key")
    assert "QWEN_API_KEY" in str(exc.value)
    with pytest.raises(RuntimeError) as exc:
        Settings().require("qwen_base_url")
    assert "QWEN_BASE_URL" in str(exc.value)


@pytest.mark.parametrize(
    ("provider", "host", "settings"),
    [
        ("anthropic", "api.anthropic.com", {"anthropic_api_key": "k"}),
        (
            "qwen",
            "qwen.test",
            {"qwen_api_key": "k", "qwen_base_url": "http://qwen.test/v1"},
        ),
    ],
)
async def test_build_llm_calls_the_chosen_provider_with_the_chosen_model(
    monkeypatch, provider, host, settings
):
    """Fails if the provider setting builds the other provider's client, or drops the configured model."""
    import anthropic
    import httpx
    import httpx2
    import openai

    seen: list = []
    # The Anthropic SDK is built on httpx2, the OpenAI SDK on httpx.
    library = httpx2 if provider == "anthropic" else httpx

    def answer(request):
        seen.append(request)
        if provider == "anthropic":
            return library.Response(
                200,
                json={
                    "id": "m1",
                    "type": "message",
                    "role": "assistant",
                    "model": "chosen-model",
                    "content": [{"type": "text", "text": "hello"}],
                    "stop_reason": "end_turn",
                    "usage": {"input_tokens": 1, "output_tokens": 1},
                },
            )
        return library.Response(
            200,
            json={
                "id": "c1",
                "object": "chat.completion",
                "created": 0,
                "model": "chosen-model",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "hello"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    # The SDK clients the builder makes send through a transport that answers locally.
    module, name = (
        (anthropic, "AsyncAnthropic") if provider == "anthropic" else (openai, "AsyncOpenAI")
    )
    sdk = getattr(module, name)

    def local(*args, **kwargs):
        kwargs["http_client"] = library.AsyncClient(transport=library.MockTransport(answer))
        return sdk(*args, **kwargs)

    monkeypatch.setattr(module, name, local)

    client = worker.build_llm(Settings(llm_provider=provider, model="chosen-model", **settings))
    reply = await client.complete("sys", [{"role": "user", "content": "hi"}], [])

    assert reply.text == "hello"
    [request] = seen
    assert request.url.host == host
    assert json.loads(request.content)["model"] == "chosen-model"


class FakeAnthropic:
    """Stands in for ``AsyncAnthropic``: records each create call, answers with a tool call."""

    def __init__(self):
        self.calls: list[dict] = []
        block = SimpleNamespace(
            type="tool_use",
            id="toolu_1",
            name="search_items",
            input={"query": "x"},
            model_dump=lambda **_: {
                "type": "tool_use",
                "id": "toolu_1",
                "name": "search_items",
                "input": {"query": "x"},
            },
        )
        self._response = SimpleNamespace(content=[block], stop_reason="tool_use", usage=None)
        self.messages = SimpleNamespace(create=self._create)

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        return self._response


async def test_anthropic_tool_call_ids_are_namespaced_for_us_and_plain_for_the_server():
    """Fails if two replies share a tool-call ID in history, or the server is sent an ID it did not make."""
    from agentos.llm import AnthropicLLMClient

    fake = FakeAnthropic()
    client = AnthropicLLMClient.__new__(AnthropicLLMClient)
    client._client, client._model = fake, "claude-test"

    first = await client.complete("sys", [{"role": "user", "content": "hi"}], TOOLS)
    second = await client.complete("sys", [{"role": "user", "content": "hi"}], TOOLS)

    [one], [two] = first.tool_calls, second.tool_calls
    assert one.id != two.id and one.id.endswith("-toolu_1") and two.id.endswith("-toolu_1")
    assert first.content[0]["id"] == one.id
    history = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": first.content},
        {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": one.id, "content": "found"}],
        },
    ]
    await client.complete("sys", history, TOOLS)
    sent = fake.calls[-1]["messages"]
    assert sent[1]["content"][0]["id"] == "toolu_1"
    assert sent[2]["content"][0]["tool_use_id"] == "toolu_1"
    assert history[1]["content"][0]["id"] == one.id, "the stored history is not rewritten"
