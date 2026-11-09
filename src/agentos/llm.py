"""The LLM behind the agent loop.

``LLMClient`` is the only interface workflows' activities use. There are two
real clients: ``AnthropicLLMClient`` (the default) calls Claude through the
Messages API with tool use, and ``QwenLLMClient`` calls Qwen through an
OpenAI-compatible Chat Completions endpoint. The scripted fake replays queued
responses for tests and never touches the network.

Chat history and tool definitions are always in Anthropic Messages format. The
Qwen client translates them to OpenAI format on the way out and translates the
reply back, so nothing outside this module knows which provider is in use.

Each real client imports its SDK inside ``__init__``, so importing this module
never needs an SDK to initialise and never fails for a missing key. Keys are
passed in by the caller from settings and are never read, logged or printed
here.
"""

from __future__ import annotations

import copy
import json
from typing import Protocol

from agentos.models import LLMResponse, ToolCall

# Upper bound on tokens the model may generate per call, for every provider.
MAX_TOKENS = 3072


class LLMClient(Protocol):
    async def complete(
        self, system: str, messages: list[dict], tools: list[dict]
    ) -> LLMResponse: ...


class AnthropicLLMClient:
    """Calls Claude with the agent's system prompt, history and tools."""

    def __init__(self, api_key: str, model: str):
        # Imported here so tests that never build the real client do not need
        # the SDK to initialise anything.
        from anthropic import AsyncAnthropic

        self._client = AsyncAnthropic(api_key=api_key)
        self._model = model

    async def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResponse:
        kwargs: dict = {
            "model": self._model,
            "max_tokens": MAX_TOKENS,
            "system": system,
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = tools
        resp = await self._client.messages.create(**kwargs)
        return to_llm_response(resp)


def to_llm_response(resp) -> LLMResponse:
    """Map an Anthropic ``Message`` to ``LLMResponse``, by block type."""
    texts: list[str] = []
    calls: list[ToolCall] = []
    for block in resp.content:
        if block.type == "text":
            texts.append(block.text)
        elif block.type == "tool_use":
            calls.append(ToolCall(id=block.id, name=block.name, input=dict(block.input or {})))
    # Raw blocks are kept so the assistant turn, including any thinking blocks,
    # goes back to the model unchanged on the next call.
    raw = [block.model_dump(mode="json", exclude_none=True) for block in resp.content]
    text = "".join(texts).strip() or None
    return LLMResponse(text=text, tool_calls=calls, stop_reason=str(resp.stop_reason), content=raw)


class QwenLLMClient:
    """Calls Qwen through its OpenAI-compatible Chat Completions endpoint.

    ``client`` lets tests pass a fake; when it is ``None`` the real
    ``openai.AsyncOpenAI`` client is built for ``base_url``. ``thinking`` turns
    Qwen's thinking mode on or off. It is off by default because thinking output
    counts toward ``MAX_TOKENS`` and makes each turn slower. Thinking text is
    never written to history.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        thinking: bool = False,
        client=None,
    ):
        if client is None:
            # Imported here so nothing fails at import time and tests that
            # never build the real client do not need the SDK.
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._client = client
        self._model = model
        self._thinking = thinking

    async def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResponse:
        kwargs: dict = {
            "model": self._model,
            "messages": to_openai_messages(system, messages),
            "max_tokens": MAX_TOKENS,
            # Not a standard OpenAI parameter, so the SDK must send it in the
            # request body as an extra field.
            "extra_body": {"enable_thinking": self._thinking},
        }
        if tools:
            kwargs["tools"] = to_openai_tools(tools)
        resp = await self._client.chat.completions.create(**kwargs)
        return from_openai_response(resp)


def to_openai_tools(tools: list[dict]) -> list[dict]:
    """Turn Anthropic tool definitions into OpenAI function tools."""
    return [
        {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool["input_schema"],
            },
        }
        for tool in tools
    ]


def _tool_result_text(block: dict) -> str:
    content = block.get("content")
    if isinstance(content, list):
        text = "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    else:
        text = content or ""
    if block.get("is_error"):
        text = f"Error: {text}"
    return text


def _user_to_openai(content) -> list[dict]:
    if isinstance(content, str):
        return [{"role": "user", "content": content}]
    out: list[dict] = []
    texts: list[str] = []
    for block in content:
        kind = block.get("type")
        if kind == "tool_result":
            out.append(
                {
                    "role": "tool",
                    "tool_call_id": block["tool_use_id"],
                    "content": _tool_result_text(block),
                }
            )
        elif kind == "text":
            texts.append(block.get("text", ""))
        # Any other block type has no OpenAI equivalent and is dropped.
    if texts:
        out.append({"role": "user", "content": "".join(texts)})
    return out


def _assistant_to_openai(content) -> dict:
    if isinstance(content, str):
        return {"role": "assistant", "content": content or None}
    texts: list[str] = []
    calls: list[dict] = []
    for block in content:
        kind = block.get("type")
        if kind == "text":
            texts.append(block.get("text", ""))
        elif kind == "tool_use":
            calls.append(
                {
                    "id": block["id"],
                    "type": "function",
                    "function": {
                        "name": block["name"],
                        "arguments": json.dumps(block.get("input") or {}),
                    },
                }
            )
        # Thinking, redacted thinking and unknown blocks are dropped. History
        # written while Anthropic was the provider can contain them.
    msg: dict = {"role": "assistant", "content": "".join(texts) or None}
    if calls:
        msg["tool_calls"] = calls
    return msg


def to_openai_messages(system: str, messages: list[dict]) -> list[dict]:
    """Translate an Anthropic-format history into OpenAI chat messages.

    The system prompt becomes the first message. Tool results become ``tool``
    messages, in block order, ahead of any text the same user turn carries.
    """
    out: list[dict] = [{"role": "system", "content": system}]
    for message in messages:
        role = message.get("role")
        content = message.get("content")
        if role == "user":
            out.extend(_user_to_openai(content))
        elif role == "assistant":
            out.append(_assistant_to_openai(content))
    return out


_FINISH_REASONS = {"tool_calls": "tool_use", "stop": "end_turn", "length": "max_tokens"}


def from_openai_response(resp) -> LLMResponse:
    """Map an OpenAI chat completion to ``LLMResponse``.

    ``content`` is rebuilt as Anthropic-format blocks so history stays in one
    format. Thinking output (``reasoning_content``) is never kept.
    """
    choice = resp.choices[0]
    message = choice.message
    text = (message.content or "").strip() or None
    calls: list[ToolCall] = []
    for call in message.tool_calls or []:
        try:
            parsed = json.loads(call.function.arguments or "")
        except (TypeError, ValueError):
            parsed = None
        # Keep the call even when its arguments are unusable: the agent loop
        # then returns the tool's own validation error to the model.
        calls.append(
            ToolCall(
                id=call.id,
                name=call.function.name,
                input=parsed if isinstance(parsed, dict) else {},
            )
        )
    finish = choice.finish_reason
    stop_reason = _FINISH_REASONS.get(finish, str(finish))
    content: list[dict] = []
    if text:
        content.append({"type": "text", "text": text})
    for call in calls:
        content.append({"type": "tool_use", "id": call.id, "name": call.name, "input": call.input})
    return LLMResponse(text=text, tool_calls=calls, stop_reason=stop_reason, content=content)


class ScriptedLLMClient:
    """Test fake: returns queued responses in order and records every call."""

    def __init__(self, responses: list[LLMResponse] | None = None):
        self.responses: list[LLMResponse] = list(responses or [])
        self.calls: list[dict] = []

    def queue(self, *responses: LLMResponse) -> None:
        self.responses.extend(responses)

    async def complete(self, system: str, messages: list[dict], tools: list[dict]) -> LLMResponse:
        self.calls.append(
            {"system": system, "messages": copy.deepcopy(messages), "tools": copy.deepcopy(tools)}
        )
        if not self.responses:
            raise RuntimeError("ScriptedLLMClient has no more responses queued")
        return self.responses.pop(0)
