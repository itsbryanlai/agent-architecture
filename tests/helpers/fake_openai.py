"""A fake OpenAI-compatible client and the response shapes it returns."""

from __future__ import annotations

from types import SimpleNamespace


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
