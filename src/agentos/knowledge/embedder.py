"""Text embeddings behind one interface, so the provider can be swapped.

``OpenAICompatibleEmbedder`` calls any OpenAI-compatible embeddings endpoint
(Qwen's, a local Ollama, and so on) and imports the SDK only when it builds a real client, so importing this module
never needs a key or the network. ``FakeEmbedder`` is deterministic and local,
for tests. Keys are passed in by the caller and are never read, logged or
printed here.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Protocol

# The endpoint accepts at most this many inputs per request.
BATCH_SIZE = 10


class Embedder(Protocol):
    model: str
    dimensions: int

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one vector of ``dimensions`` floats per text, in order."""
        ...


class OpenAICompatibleEmbedder:
    """Embeds through an OpenAI-compatible endpoint, at most 10 texts per call.

    ``client`` lets tests pass a fake; when it is ``None`` the real
    ``openai.AsyncOpenAI`` client is built for ``base_url``.
    """

    def __init__(self, api_key: str, base_url: str, model: str, dimensions: int, client=None):
        if client is None:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._client = client
        self.model = model
        self.dimensions = dimensions

    async def embed(self, texts: list[str]) -> list[list[float]]:
        vectors: list[list[float]] = []
        for start in range(0, len(texts), BATCH_SIZE):
            batch = texts[start : start + BATCH_SIZE]
            resp = await self._client.embeddings.create(
                model=self.model,
                input=batch,
                dimensions=self.dimensions,
                encoding_format="float",
            )
            data = sorted(resp.data, key=lambda d: d.index)
            if len(data) != len(batch):
                raise ValueError(f"expected {len(batch)} embeddings, got {len(data)}")
            for item in data:
                vector = list(item.embedding)
                if len(vector) != self.dimensions:
                    raise ValueError(f"expected {self.dimensions} dimensions, got {len(vector)}")
                vectors.append(vector)
        return vectors


class FakeEmbedder:
    """Deterministic test embedder: lowercase words hashed into buckets, L2-normalized.

    Texts that share words end up close together. ``texts_embedded`` counts how many
    texts were embedded, so tests can see whether anything was re-embedded.
    """

    def __init__(self, dimensions: int = 1024, model: str = "fake-embed"):
        self.model = model
        self.dimensions = dimensions
        self.texts_embedded = 0

    async def embed(self, texts: list[str]) -> list[list[float]]:
        self.texts_embedded += len(texts)
        return [self._one(t) for t in texts]

    def _one(self, text: str) -> list[float]:
        vector = [0.0] * self.dimensions
        for word in re.findall(r"[a-z0-9]+", text.lower()):
            bucket = int.from_bytes(hashlib.sha256(word.encode()).digest()[:8], "big")
            vector[bucket % self.dimensions] += 1.0
        norm = math.sqrt(sum(v * v for v in vector))
        if norm == 0:
            return vector
        return [v / norm for v in vector]
