"""How Google Drive's errors are told apart: the one table every caller uses.

The Drive read surface (``google_iam``) raises ``GoogleError`` carrying the HTTP status
for a read. The status, not the short code, says whether trying again can help: a 429 or
a server error passes on its own, while a file that is gone (404) or no longer shared
(403) never does. Activities, the search re-check and the indexer all ask here.

* ``retryable`` errors are raised as they are, so Temporal's retry policy applies. A 429
  that carries ``Retry-After`` becomes an ``ApplicationError`` with ``next_retry_delay``,
  so the wait is the one Google asked for.
* Final errors are non-retryable. A read tool shows the model ``Entry.text`` instead of
  failing the turn.
* A ``GoogleError`` with no status (an IAM or settings error) is not a Drive read error:
  ``entry_for`` answers None and its own caller handles it.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.exceptions import ApplicationError

from agentos.connectors.base import Entry
from agentos.connectors.google_iam import MAX_RETRY_AFTER_SECONDS, GoogleError

_NOT_AVAILABLE = "That Google Drive file is not available to me right now."
_ACCESS_REFUSED = "Google Drive refused this account's access, so I can't read it right now."
_BAD_REQUEST = "Google Drive could not process that request, so I can't read it right now."

DRIVE_ERRORS: dict[int, Entry] = {
    # Final: the same request fails the same way.
    400: Entry(False, _BAD_REQUEST),
    401: Entry(False, _ACCESS_REFUSED),
    403: Entry(False, _NOT_AVAILABLE),
    404: Entry(False, _NOT_AVAILABLE),
    # Retryable: Google says try again, or the fault is Google's.
    408: Entry(True, ""),
    429: Entry(True, ""),
    500: Entry(True, ""),
    502: Entry(True, ""),
    503: Entry(True, ""),
    504: Entry(True, ""),
}


def is_drive_error(err: BaseException) -> bool:
    """True for an error the Drive read surface answered with (a status is set)."""
    return isinstance(err, GoogleError) and err.status is not None


def entry_for(err: BaseException) -> Entry | None:
    """The table row for a Drive read error, or None when ``err`` is not one."""
    if not is_drive_error(err):
        return None
    known = DRIVE_ERRORS.get(err.status)
    if known is not None:
        return known
    final = 400 <= err.status < 500 and err.status not in (408, 429)
    return Entry(not final, _NOT_AVAILABLE if final else "")


def is_final(err: BaseException) -> bool:
    """True when ``err`` is a Drive read error that retrying cannot fix."""
    entry = entry_for(err)
    return entry is not None and not entry.retryable


def is_gone(err: BaseException) -> bool:
    """True when Drive says the file does not exist (or is not shared) any more."""
    return is_drive_error(err) and err.status in (403, 404)


def retry_failure(err: BaseException) -> ApplicationError | None:
    """The failure to raise for a retryable Drive error that named its own wait, or None
    (raise the error as it is)."""
    entry = entry_for(err)
    if entry is None or not entry.retryable or err.retry_after is None:
        return None
    seconds = min(max(err.retry_after, 0), MAX_RETRY_AFTER_SECONDS)
    return ApplicationError(
        str(err), type=type(err).__name__, next_retry_delay=timedelta(seconds=seconds)
    )
