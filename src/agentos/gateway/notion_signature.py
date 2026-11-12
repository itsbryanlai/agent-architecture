"""Check the signature Notion puts on every webhook event.

Notion sends ``X-Notion-Signature: sha256=<hex>``, where ``<hex>`` is the
HMAC-SHA256 of the raw request body, keyed with the subscription's
verification token.
"""

from __future__ import annotations

import hashlib
import hmac

PREFIX = "sha256="


def verify_notion_signature(body: bytes, header: str | None, token: str) -> bool:
    """True only when ``header`` is the correct signature of ``body`` under ``token``.

    A missing or malformed header is False. The comparison takes the same time
    whatever the input.
    """
    if not header or not header.startswith(PREFIX) or not token:
        return False
    expected = PREFIX + hmac.new(token.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected.encode(), header.encode())
