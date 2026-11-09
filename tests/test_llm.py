"""Model client tests. No real SDK client is built: the Qwen client gets a fake
OpenAI client that records its arguments and returns a canned reply."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from agentos import worker
from agentos.config import ConfigError, Settings, load_settings
from agentos.llm import (
    MAX_TOKENS,
    QwenLLMClient,
    from_openai_response,
    to_openai_messages,
    to_openai_tools,
)

PROVIDER_VARS = (
    "AGENTOS_LLM_PROVIDER",
    "AGENTOS_MODEL",
    "AGENTOS_QWEN_THINKING",
    "RESEARCH_QWEN_BASE_URL",
    "QWEN_API_KEY",
)


def _tool_call(call_id: str, name: str, arguments: str):
    return SimpleNamespace(
        id=call_id,
        type="function",
        function=SimpleNamespace(name=name, arguments=arguments),
    )


def _response(content=None, tool_calls=None, finish_reason="stop"):
    message = SimpleNamespace(role="assistant", content=content, tool_calls=tool_calls)
    return SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason=finish_reason)])


class FakeOpenAI:
    """Stands in for ``openai.AsyncOpenAI``: records each create call."""

    def __init__(self, response=None):
        self.calls: list[dict] = []
        self._response = response or _response(content="ok")
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        return self._response


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


async def test_max_tokens_is_3072():
    assert MAX_TOKENS == 3072
    fake = FakeOpenAI()
    await _qwen(fake).complete("sys", [{"role": "user", "content": "hi"}], TOOLS)
    assert fake.calls[0]["max_tokens"] == 3072
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
    for name in PROVIDER_VARS:
        monkeypatch.delenv(name, raising=False)

    default = load_settings()
    assert default.llm_provider == "anthropic"
    assert default.model == "claude-sonnet-5-5"
    assert default.qwen_thinking is False

    monkeypatch.setenv("AGENTOS_LLM_PROVIDER", "QWEN")
    monkeypatch.setenv("AGENTOS_QWEN_THINKING", "Yes")
    qwen = load_settings()
    assert qwen.llm_provider == "qwen"
    assert qwen.model == "qwen3.7-plus"
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
    assert "RESEARCH_QWEN_BASE_URL" in str(exc.value)


def test_build_llm_selects_provider(monkeypatch):
    built: list[tuple] = []

    class FakeAnthropicClient:
        def __init__(self, *args):
            built.append(("anthropic", args))

    class FakeQwenClient:
        def __init__(self, *args):
            built.append(("qwen", args))

    monkeypatch.setattr(worker, "AnthropicLLMClient", FakeAnthropicClient)
    monkeypatch.setattr(worker, "QwenLLMClient", FakeQwenClient)

    assert isinstance(worker.build_llm(Settings(anthropic_api_key="k")), FakeAnthropicClient)
    qwen_settings = Settings(
        llm_provider="qwen",
        model="qwen3.7-plus",
        qwen_api_key="k",
        qwen_base_url="http://fake.invalid",
    )
    assert isinstance(worker.build_llm(qwen_settings), FakeQwenClient)
    assert built[1] == ("qwen", ("k", "http://fake.invalid", "qwen3.7-plus", False))
