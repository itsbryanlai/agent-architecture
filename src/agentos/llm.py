"""The LLM behind the agent loop.

``LLMClient`` is the only interface workflows' activities use. The real client
calls Claude through the Messages API with tool use; the scripted fake replays
queued responses for tests and never touches the network.
"""

from __future__ import annotations

import copy
from typing import Protocol

from agentos.models import LLMResponse, ToolCall

MAX_TOKENS = 1024


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
