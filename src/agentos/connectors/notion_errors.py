"""How Notion's errors are told apart: the one table every caller uses.

Notion names every failure with an error code. The code, not the HTTP status class, says
whether trying again can help: a 409 ``conflict_error`` and a 429 ``rate_limited`` pass on
their own, while a revoked page (``object_not_found``) or a bad key (``unauthorized``) never
does. Activities, the read tools and the indexer all ask here.

* ``retryable`` errors are raised as they are, so Temporal's retry policy applies. A 429
  that carries ``Retry-After`` becomes an ``ApplicationError`` with ``next_retry_delay``, so
  the wait is the one Notion asked for.
* Final errors are non-retryable. A read tool shows the model ``Entry.text`` instead of failing the
  turn.
* A code the table does not know falls back on the status: a 4xx other than 408 and 429 is
  final, anything else is retried.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from notion_client.errors import HTTPResponseError
from temporalio.exceptions import ApplicationError

# Longest wait taken from a Retry-After header, so one odd header cannot park an activity.
MAX_RETRY_AFTER_SECONDS = 300


@dataclass(frozen=True)
class Entry:
    retryable: bool
    text: str  # what a read tool tells the model when the error is final


_NOT_AVAILABLE = "That Notion database or page is not available to me right now."
_KEY_REFUSED = "Notion refused the integration's access, so I can't read it right now."
_BAD_REQUEST = "Notion could not process that request, so I can't read it right now."

NOTION_ERRORS: dict[str, Entry] = {
    # Final: the same request fails the same way.
    "object_not_found": Entry(False, _NOT_AVAILABLE),
    "restricted_resource": Entry(False, _NOT_AVAILABLE),
    "unauthorized": Entry(False, _KEY_REFUSED),
    "validation_error": Entry(False, _BAD_REQUEST),
    "invalid_request": Entry(False, _BAD_REQUEST),
    "invalid_request_url": Entry(False, _BAD_REQUEST),
    "invalid_json": Entry(False, _BAD_REQUEST),
    # Retryable: Notion says try again, or the fault is Notion's.
    "conflict_error": Entry(True, ""),
    "rate_limited": Entry(True, ""),
    "internal_server_error": Entry(True, ""),
    "service_unavailable": Entry(True, ""),
    "gateway_timeout": Entry(True, ""),
}


def _code(err: BaseException) -> str | None:
    if not isinstance(err, HTTPResponseError):
        return None
    code = err.code
    return code.value if isinstance(code, Enum) else str(code)


def is_notion_error(err: BaseException) -> bool:
    """True for an error Notion's API answered with (not a timeout or a connection error)."""
    return isinstance(err, HTTPResponseError)


def entry_for(err: BaseException) -> Entry | None:
    """The table row for a Notion API error, or None when ``err`` is not one."""
    if not isinstance(err, HTTPResponseError):
        return None
    known = NOTION_ERRORS.get(_code(err) or "")
    if known is not None:
        return known
    final = 400 <= err.status < 500 and err.status not in (408, 429)
    return Entry(not final, _NOT_AVAILABLE if final else "")


def is_final(err: BaseException) -> bool:
    """True when ``err`` is a Notion API error that retrying cannot fix."""
    entry = entry_for(err)
    return entry is not None and not entry.retryable


def is_gone(err: BaseException) -> bool:
    """True when Notion says the object does not exist (or is not shared) any more."""
    return _code(err) == "object_not_found" or getattr(err, "status", None) == 404


def retry_after_seconds(err: BaseException) -> float | None:
    """Seconds from a ``Retry-After`` header in whole seconds, or None."""
    if not isinstance(err, HTTPResponseError):
        return None
    value = err.headers.get("retry-after") if err.headers is not None else None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return float(min(max(seconds, 0), MAX_RETRY_AFTER_SECONDS))


def retry_failure(err: BaseException) -> ApplicationError | None:
    """The failure to raise for a retryable Notion error that named its own wait, or None
    (raise the error as it is)."""
    entry = entry_for(err)
    seconds = retry_after_seconds(err)
    if entry is None or not entry.retryable or seconds is None:
        return None
    return ApplicationError(
        str(err), type=type(err).__name__, next_retry_delay=timedelta(seconds=seconds)
    )
