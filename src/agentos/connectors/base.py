"""The connector interface: what the runtime needs from any system an agent acts on.

One protocol per connector, described by its manifest (``agentos.connectors.manifest``):
the grants it holds, the read tools the model may call, and the write kinds its
idempotency methods declare. The runtime builds connectors and classifies their errors
through the registry (``agentos.connectors.registry``), keyed by the manifest's
connector name; Notion is the first adapter.

A connector checks its grants before any call that names a target, and resolves an
item reference to an item of a granted scope. Reads run by manifest tool name; the
requester's identity comes from the runtime, never from the model's input. Writes are
kind-keyed: ``find_write`` is the marker lookup for an idempotency key (the same lookup
reconcile uses), ``write`` makes the write carrying the marker. The platform orders
find and write behind a lease in Postgres (``AgentActivities._write_once``), so one
attempt at a time writes and a retry finds what an earlier attempt made.

Google Drive's writes map onto the same surface, each as a kind with its own
idempotency method in its manifest; the protocol's methods do not change:

* Comment on a file: the comment kind's shape — a marker in the comment text,
  ``find_write`` scans the file's comments for it (as Notion's ``add_comment``).
* Append a row to a Sheet: a new kind — developer metadata on the appended row is the
  marker; ``find_write`` looks up rows by it, ``write`` appends, reconcile looks again.
* Create a Doc from a template: the item kind's shape — the marker lives in the file's
  ``appProperties``; ``find_write`` looks up files by it, ``write`` creates.
* Replace a named range in a Doc: a new kind — the range at the revision the card
  showed; ``write`` refuses when the Doc moved since the read.

A connector brings its own error table (``ConnectorErrors``) and its own rate limit;
``bulk_priority`` is the hook that queues bulk reads behind interactive requests.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Protocol

from temporalio.exceptions import ApplicationError

from agentos.connectors.manifest import Manifest
from agentos.models import NotionItem

# A guard against a loop in a connector's parent links, far deeper than any real tree.
# An item at any depth the indexer reached is also found by a search and by a resolve.
MAX_WALK_DEPTH = 100


class ConnectorError(Exception):
    """Base for connector errors that retrying cannot fix."""


class GrantDenied(ConnectorError):
    """The call targets a scope outside the agent's grants."""


class UnknownRequester(ConnectorError):
    """The requester's email matches no user in the target system."""


class UnknownItem(ConnectorError):
    """The link or ID does not name an item the agent can open."""


@dataclass(frozen=True)
class Entry:
    retryable: bool
    text: str  # what a read tool tells the model when the error is final


class ConnectorErrors(Protocol):
    """How one connector's API errors are told apart: the table its callers ask.

    ``entry_for`` answers None for an error this connector did not raise, so a
    caller can try every registered table until one recognizes the error.
    """

    def is_final(self, err: BaseException) -> bool:
        """True when retrying the same request cannot help."""
        ...

    def is_gone(self, err: BaseException) -> bool:
        """True when the target object does not exist (or is not shared) any more."""
        ...

    def entry_for(self, err: BaseException) -> Entry | None:
        """The table row for this connector's API error, or None when it is not one."""
        ...

    def retry_failure(self, err: BaseException) -> ApplicationError | None:
        """The failure carrying the wait the connector named (Retry-After), or None."""
        ...


class Grant(Protocol):
    """One scope the agent was granted: its ID and the name people see."""

    @property
    def id(self) -> str: ...

    @property
    def name(self) -> str: ...


class Connector(Protocol):
    """One system an agent reads and writes, limited to the grants it holds.

    The result of a write surface call is the kind's: an item for an item kind, a
    comment ID for a comment kind. The caller knows its kind; a new connector adds
    kinds to its dispatch and its manifest, not methods here.
    """

    manifest: Manifest
    errors: ConnectorErrors

    @property
    def name(self) -> str:
        """The manifest's connector name, the registry's key."""
        ...

    @property
    def grants(self) -> list[Grant]:
        """The scopes this connector holds for the agent."""
        ...

    def tools_for_llm(self) -> list[dict]:
        """Tool definitions the LLM may call; requester fields are absent."""
        ...

    def grant_for(self, target_id: str) -> Grant:
        """The grant for a target, or raise before any API call."""
        ...

    async def read(
        self, tool: str, tool_input: dict, requester_email: str | None = None
    ) -> list[NotionItem]:
        """Run a manifest read tool by name.

        ``requester_email`` is the asker's, from the runtime; a requester-scoped
        tool reads it and ignores any person field in ``tool_input``.
        """
        ...

    async def resolve_item(self, item_ref: str) -> NotionItem:
        """The item a link or ID names, its scope checked against the grants.

        An item that is missing, unshared or outside the grants raises the same
        ``UnknownItem``, so the answer never reveals whether an ungranted item exists.
        """
        ...

    async def assignee_user_id(self, item_id: str) -> str | None:
        """The person responsible for an item, in the target system's user IDs, or None."""
        ...

    async def find_write(self, kind: str, target: str, idem_key: str) -> NotionItem | str | None:
        """The marker lookup of a write kind: what an earlier attempt made under
        ``idem_key``, if the target shows it yet. The reconcile lookup is the same one.
        """
        ...

    async def write(
        self,
        kind: str,
        target: str,
        payload: dict,
        idem_key: str,
        *,
        agent_name: str,
        requester: dict | None = None,
    ) -> NotionItem | str:
        """Make the write of ``kind``, carrying the marker ``find_write`` looks for.

        It does not look for an earlier write: the caller orders find and write
        behind its lease. ``payload`` is the approved revision, exactly what runs.
        ``requester`` is who asked, as the runtime resolved them for this connector
        (Notion: ``notion_user_id`` and ``line``); None when the write needs none.
        """
        ...

    def bulk_priority(self) -> AbstractContextManager[None]:
        """Bulk reads made inside this context queue behind interactive requests."""
        ...
