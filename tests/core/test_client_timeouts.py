"""The real model and embedding clients, through a mocked HTTP transport.

A call that times out must fail fast with a retryable error and be attempted once: Temporal,
not the SDK, owns retries, and a call must give up before its activity's timeout.
"""

from __future__ import annotations

import httpx
import httpx2
import pytest

from agentos.knowledge.embedder import OpenAICompatibleEmbedder
from agentos.llm import AnthropicLLMClient, QwenLLMClient
from agentos.workflows import INDEX_TIMEOUT, LLM_TIMEOUT, RETRY


class TimingOutTransport:
    """Every request times out. The Anthropic SDK is built on httpx2, the OpenAI SDK on httpx."""

    def __init__(self, library) -> None:
        self.requests: list = []
        self._library = library
        self.client = library.AsyncClient(transport=library.MockTransport(self._handle))

    def _handle(self, request):
        self.requests.append(request)
        raise self._library.ReadTimeout("no answer", request=request)


def anthropic(http):
    return AnthropicLLMClient("unused", "m", http_client=http), LLM_TIMEOUT


def qwen(http):
    return QwenLLMClient("unused", "http://model.invalid/v1", "m", http_client=http), LLM_TIMEOUT


async def call(client) -> None:
    if isinstance(client, OpenAICompatibleEmbedder):
        await client.embed(["a page"])
    else:
        await client.complete("system", [{"role": "user", "content": "hi"}], [])


def embedder(http):
    client = OpenAICompatibleEmbedder("unused", "http://embed.invalid/v1", "m", 8, http_client=http)
    return client, INDEX_TIMEOUT


@pytest.mark.parametrize(
    ("build", "library"), [(anthropic, httpx2), (qwen, httpx), (embedder, httpx)]
)
async def test_a_timed_out_call_is_tried_once_inside_its_activity_timeout(build, library):
    """Fails if the SDK retries a timed-out call, or lets it run past its activity's timeout."""
    transport = TimingOutTransport(library)
    client, activity_timeout = build(transport.client)

    with pytest.raises(Exception) as raised:
        await call(client)

    assert len(transport.requests) == 1
    assert type(raised.value).__name__ == "APITimeoutError"
    assert type(raised.value).__name__ not in RETRY.non_retryable_error_types
    read_timeout = transport.requests[0].extensions["timeout"]["read"]
    assert read_timeout < activity_timeout.total_seconds()
