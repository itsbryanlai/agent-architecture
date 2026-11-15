"""Check the signature Notion puts on every webhook event.

Notion sends ``X-Notion-Signature: sha256=<hex>``, where ``<hex>`` is the
HMAC-SHA256 of the raw request body, keyed with the subscription's
verification token.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Callable
from typing import Any

PREFIX = "sha256="


def verify_notion_signature(
    body: bytes, header: str | None, token: str, *, mac: Callable[..., Any] = hmac.new
) -> bool:
    """True only when ``header`` is the correct signature of ``body`` under ``token``.

    The HMAC is computed and compared in constant time for every input, a missing or
    mis-prefixed header and an empty token included, so the answer's timing does not
    say which part was wrong. ``mac`` is for tests to count the calls.
    """
    expected = PREFIX + mac(token.encode(), body, hashlib.sha256).hexdigest()
    given = (header or "").encode("utf-8", "replace")
    matches = hmac.compare_digest(expected.encode(), given)
    return matches and bool(header) and header.startswith(PREFIX) and bool(token)
