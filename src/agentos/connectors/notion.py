"""Notion connector.

The agent acts with its own access, never the asker's: every call uses the
tenant's single Notion integration token. A call that names a database (a
search, a create) is checked against the agent's grants (the database IDs it
was given) *before* anything is sent to Notion; outside the grants it raises
``GrantDenied`` and no request goes out. A call that names a page by link or
ID cannot be checked first, because Notion tells a page's database only when
the page is fetched. It is fetched, its database is checked, and a page that
is missing, not shared with the integration or not in a granted database all
raise the same ``UnknownItem``, so the answer never reveals whether an
ungranted page exists. Activities turn ``ConnectorError``s into non-retryable
failures.

Requester-scoped reads ("my items") take the requester's email from the
runtime, which got it from Slack. The LLM never supplies it.

Notion has no idempotency key, so a write is made findable instead: every item
the agent creates carries its key in a hidden text property, and every comment
ends with a reference derived from its key. The connector offers the pieces
(``find_item``, ``create_item``, ``find_comment``, ``create_comment``). The
write activities in ``agentos.activities`` order them, behind a lease in
Postgres, so that one attempt at a time writes and a retry finds what an
earlier attempt made. Notion's search can lag behind a write by seconds, which
is why the lease and the result stored under it, not the search alone, carry
the guarantee.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from agentos.config import ConfigError, NotionDatabaseGrant, NotionPageGrant
from agentos.connectors.manifest import MANIFEST_DIR, Manifest, load_manifest
from agentos.connectors.notion_limiter import NotionLimiter
from agentos.models import NOTION_TEXT_MAX, SIGNATURE_NAME_MAX, NotionItem

if TYPE_CHECKING:
    import httpx

LLM_TOOLS = ("search_items", "my_items", "create_item", "add_comment")

# One Notion request may take this long. It stays under the activity timeouts, so a
# slow Notion fails the attempt, and Temporal's retry policy decides what happens next.
REQUEST_TIMEOUT_MS = 10_000


class ConnectorError(Exception):
    """Base for connector errors that retrying cannot fix."""


class GrantDenied(ConnectorError):
    """The call targets a database or page outside the agent's grants."""


class UnknownRequester(ConnectorError):
    """The requester's email matches no user in the target system."""


class UnknownItem(ConnectorError):
    """The link or ID does not name a Notion page the agent can open."""


_DASHED_ID = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_PLAIN_ID = re.compile(
    r"(?<![0-9a-zA-Z])[0-9a-fA-F]{32}(?![0-9a-zA-Z])|(?<=-)[0-9a-fA-F]{32}(?![0-9a-zA-Z])"
)


def _page_id_from_ref(ref: str) -> str | None:
    """A page ID from a Notion page URL or a bare ID (dashed or not)."""
    path = (ref or "").strip().split("?")[0].split("#")[0]
    found = _DASHED_ID.findall(path) or _PLAIN_ID.findall(path)
    return normalize_id(found[-1]) if found else None


def comment_ref(idem_key: str) -> str:
    """The reference a comment ends with: the whole digest of its key, never a prefix."""
    return "ref " + hashlib.sha256(idem_key.encode()).hexdigest()


def normalize_id(notion_id: str) -> str:
    """Notion IDs appear with and without hyphens; compare them without."""
    return notion_id.replace("-", "").lower()


def is_unreachable(err: Exception) -> bool:
    """True when Notion says the page or block is gone or not shared (404 or 403).

    Such a page is left out rather than failing the whole call; any other error
    (a rate limit, a server error) is not this and is retried.
    """
    return getattr(err, "status", None) in (403, 404)


# --- Notion API surface -------------------------------------------------------


class NotionAPI(Protocol):
    """The only Notion calls the connector is allowed to make."""

    async def query_database(self, database_id: str, filter: dict | None) -> list[dict]: ...

    async def create_page(
        self, database_id: str, properties: dict, children: list[dict]
    ) -> dict: ...

    async def retrieve_page(self, page_id: str) -> dict: ...

    async def retrieve_block(self, block_id: str) -> dict:
        """One block, for its ``parent`` (a page can sit under a block)."""
        ...

    async def retrieve_database(self, database_id: str) -> dict:
        """The database object: its ``title`` and ``parent`` (read only)."""
        ...

    async def list_child_blocks(self, block_id: str) -> list[dict]:
        """Every child block of a page or block, following all pages of results."""
        ...

    async def list_users(self) -> list[dict]: ...

    async def bot_user_id(self) -> str:
        """The ID of the integration's own bot user (the author of its comments)."""
        ...

    async def create_comment(self, page_id: str, text: str) -> dict:
        """Add a comment to a page (needs the "Insert comments" capability)."""
        ...

    async def list_comments(self, page_id: str) -> list[dict]:
        """Every unresolved comment on a page, following all pages of results
        (needs the "Read comments" capability)."""
        ...

    async def retrieve_schema(self, database_id: str) -> dict[str, dict]:
        """Return ``{property name: {"id", "type", "options"}}`` for a database.

        ``options`` lists option names for ``status`` and ``select`` properties
        and is empty otherwise. This only reads the schema; the agent never
        changes a database's schema.
        """
        ...


class RealNotionAPI:
    """Wraps ``notion_client.AsyncClient``.

    The client speaks the Notion API version in which a database holds one or
    more data sources: queries and page creation go through the database's data
    sources, which are looked up once per database.

    One layer owns retries, and it is Temporal's. The SDK's own retries on 429
    and 5xx are off, and each request has a timeout well under the activity
    timeout, so a slow Notion fails the attempt instead of keeping it running.
    Every request first takes a token from the integration's ``NotionLimiter`` (see there
    for the rate and the two priorities); one tenant has one integration, so one limiter.
    ``http_client`` lets a test supply a client with a mocked transport.
    """

    def __init__(
        self,
        token: str,
        timeout_ms: int = REQUEST_TIMEOUT_MS,
        http_client: httpx.AsyncClient | None = None,
        limiter: NotionLimiter | None = None,
    ):
        import httpx
        from notion_client import AsyncClient

        # Every request this integration sends waits here first, whichever method sent it.
        self.limiter = limiter or NotionLimiter()
        http_client = http_client or httpx.AsyncClient()
        http_client.event_hooks["request"].append(self._wait_for_allowance)
        self._client = AsyncClient(
            auth=token, timeout_ms=timeout_ms, retry=False, client=http_client
        )
        self._data_sources: dict[str, list[str]] = {}

    async def _wait_for_allowance(self, request: httpx.Request) -> None:
        await self.limiter.acquire()

    async def _data_source_ids(self, database_id: str) -> list[str]:
        key = normalize_id(database_id)
        if key not in self._data_sources:
            db = await self._client.databases.retrieve(database_id=database_id)
            self._data_sources[key] = [ds["id"] for ds in db.get("data_sources", [])]
        return self._data_sources[key]

    async def query_database(self, database_id: str, filter: dict | None) -> list[dict]:
        results: list[dict] = []
        for ds_id in await self._data_source_ids(database_id):
            cursor = None
            while True:
                kwargs: dict = {"data_source_id": ds_id}
                if filter:
                    kwargs["filter"] = filter
                if cursor:
                    kwargs["start_cursor"] = cursor
                resp = await self._client.data_sources.query(**kwargs)
                results.extend(r for r in resp.get("results", []) if r.get("object") == "page")
                if not resp.get("has_more"):
                    break
                cursor = resp.get("next_cursor")
        return results

    async def create_page(self, database_id: str, properties: dict, children: list[dict]) -> dict:
        ds_ids = await self._data_source_ids(database_id)
        if not ds_ids:
            raise ConnectorError("The granted database has no data source to create items in.")
        return await self._client.pages.create(
            parent={"type": "data_source_id", "data_source_id": ds_ids[0]},
            properties=properties,
            children=children,
        )

    async def retrieve_page(self, page_id: str) -> dict:
        return await self._client.pages.retrieve(page_id=page_id)

    async def retrieve_block(self, block_id: str) -> dict:
        return await self._client.blocks.retrieve(block_id=block_id)

    async def retrieve_database(self, database_id: str) -> dict:
        return await self._client.databases.retrieve(database_id=database_id)

    async def list_child_blocks(self, block_id: str) -> list[dict]:
        blocks: list[dict] = []
        cursor = None
        while True:
            kwargs: dict = {"block_id": block_id}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = await self._client.blocks.children.list(**kwargs)
            blocks.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return blocks

    async def retrieve_schema(self, database_id: str) -> dict[str, dict]:
        """Read the property schema from the database's first data source.

        A read with the same token the agent already uses, on a granted
        database only. Nothing here edits the schema.
        """
        ds_ids = await self._data_source_ids(database_id)
        if not ds_ids:
            raise ConnectorError("The granted database has no data source.")
        ds = await self._client.data_sources.retrieve(data_source_id=ds_ids[0])
        schema: dict[str, dict] = {}
        for name, prop in (ds.get("properties") or {}).items():
            ptype = prop.get("type", "")
            options: list[str] = []
            if ptype in ("status", "select"):
                options = [o.get("name", "") for o in (prop.get(ptype) or {}).get("options", [])]
            schema[name] = {"id": prop.get("id", ""), "type": ptype, "options": options}
        return schema

    async def create_comment(self, page_id: str, text: str) -> dict:
        return await self._client.comments.create(
            parent={"page_id": page_id}, rich_text=[{"type": "text", "text": {"content": text}}]
        )

    async def list_comments(self, page_id: str) -> list[dict]:
        comments: list[dict] = []
        cursor = None
        while True:
            kwargs: dict = {"block_id": page_id}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = await self._client.comments.list(**kwargs)
            comments.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return comments

    async def bot_user_id(self) -> str:
        return (await self._client.users.me())["id"]

    async def list_users(self) -> list[dict]:
        users: list[dict] = []
        cursor = None
        while True:
            kwargs: dict = {}
            if cursor:
                kwargs["start_cursor"] = cursor
            resp = await self._client.users.list(**kwargs)
            users.extend(resp.get("results", []))
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")
        return users


class FakeNotionAPI:
    """In-memory Notion for tests, with call counters.

    Pages are stored per database in Notion's response shape: keyed by the
    schema's current property name, each value carrying its property ``id``.
    Filters and page creation take property IDs and resolve them through the
    schema set with ``set_schema``; an ID the schema does not know is an error,
    as it is in Notion. Only the filter forms the connector uses are understood.
    """

    def __init__(self, users: list[dict] | None = None):
        self.databases: dict[str, dict[str, dict]] = {}
        self.schemas: dict[str, dict[str, dict]] = {}
        self.users: list[dict] = list(users or [])
        self.calls: list[tuple[str, dict]] = []
        # Pages outside any database, by normalized ID (page grants and trees).
        self.free_pages: dict[str, dict] = {}
        # Child blocks by normalized parent ID (a page or a block), and every
        # block by normalized ID, for parent walks.
        self.children: dict[str, list[dict]] = {}
        self.blocks: dict[str, dict] = {}
        # Title and parent of a database, by normalized ID (see ``set_database_info``).
        self.database_info: dict[str, dict] = {}
        self.comments: dict[str, list[dict]] = {}
        self._fail_next_query: Exception | None = None
        # The body blocks of pages created through ``create_page``, by page ID.
        self.bodies: dict[str, list[dict]] = {}
        self.bot_id = "notion-bot"
        # Injection points for the failures a write meets in production. See the methods.
        self._fail_after_write = {"create": 0, "comment": 0}
        self._hang_after_write: dict[str, list[Hang]] = {"create": [], "comment": []}
        self._hang_reads: list[tuple[str | None, Hang]] = []
        self._visibility_delay = 0
        self._hidden: dict[str, int] = {}
        self._hidden_comments: dict[str, int] = {}

    # Test hooks.

    def fail_after_write(self, kind: str = "create", times: int = 1) -> None:
        """The next ``times`` writes of ``kind`` ("create" or "comment") take effect and
        then raise a timeout, as when the response is lost."""
        self._fail_after_write[kind] = times

    def hang_after_write(self, kind: str = "create") -> Hang:
        """The next write of ``kind`` takes effect, then waits until the returned ``Hang``
        is released. Another write meanwhile runs normally: this is how two attempts
        overlap."""
        hang = Hang()
        self._hang_after_write[kind].append(hang)
        return hang

    def hang_next_read(self, method: str | None = None) -> Hang:
        """The next read waits until the returned ``Hang`` is released, as a slow Notion
        read does. ``method`` names ``query_database`` or ``retrieve_page``; None hangs
        the next read of either. Other reads run normally."""
        hang = Hang()
        self._hang_reads.append((method, hang))
        return hang

    def _take_read_hang(self, method: str) -> Hang | None:
        for i, (want, hang) in enumerate(self._hang_reads):
            if want is None or want == method:
                del self._hang_reads[i]
                return hang
        return None

    def delay_visibility(self, calls: int) -> None:
        """A page or comment created from now on is left out of the next ``calls`` queries
        (``query_database``, ``list_comments``), as when Notion's search lags a write."""
        self._visibility_delay = calls

    def read_body(self, page_id: str) -> list[str]:
        """The text of each body block of a page created through ``create_page``."""
        return [
            "".join(t["text"]["content"] for t in block[block["type"]]["rich_text"])
            for block in self.bodies.get(normalize_id(page_id), [])
        ]

    def add_comment_by(self, page_id: str, text: str, author_id: str) -> dict:
        """Store a comment written by someone other than the integration."""
        comment = self._store_comment(page_id, text, author_id)
        return copy.deepcopy(comment)

    def fail_next_query(self, error: Exception) -> None:
        """Make the next query_database raise ``error``."""
        self._fail_next_query = error

    def add_database(self, database_id: str) -> None:
        self.databases.setdefault(normalize_id(database_id), {})

    def set_database_info(self, database_id: str, title: str, parent: dict | None = None) -> None:
        """Set what ``retrieve_database`` returns for a database: its title and parent."""
        self.database_info[normalize_id(database_id)] = {
            "title": title,
            "parent": parent or {"type": "workspace", "workspace": True},
        }

    def set_schema(self, database_id: str, schema: dict[str, dict]) -> None:
        """Set the property schema: ``{name: {"id", "type", "options"?}}``."""
        self.schemas[normalize_id(database_id)] = {
            name: {"options": [], **copy.deepcopy(prop)} for name, prop in schema.items()
        }

    def add_page(self, database_id: str, properties: dict) -> dict:
        """Store a page. Property keys may be IDs or current names."""
        db_key = normalize_id(database_id)
        page_id = str(uuid.uuid4())
        page = {
            "object": "page",
            "id": page_id,
            "url": f"https://www.notion.so/{page_id.replace('-', '')}",
            "parent": {
                "type": "data_source_id",
                "data_source_id": f"ds-{db_key}",
                "database_id": database_id,
            },
            "properties": self._to_response_properties(db_key, properties),
        }
        self.databases.setdefault(db_key, {})[page_id] = page
        return copy.deepcopy(page)

    def add_free_page(
        self, page_id: str, title: str = "Page", parent: dict | None = None, in_trash: bool = False
    ) -> None:
        """Store a page that is not in a database, such as a granted root page."""
        self.free_pages[normalize_id(page_id)] = {
            "object": "page",
            "id": page_id,
            "url": f"https://www.notion.so/{normalize_id(page_id)}",
            "in_trash": in_trash,
            "last_edited_time": "2026-01-01T00:00:00.000Z",
            "parent": parent or {"type": "workspace", "workspace": True},
            "properties": {
                "title": {
                    "id": "title",
                    "type": "title",
                    "title": [_rich({"text": {"content": title}})],
                }
            },
        }

    def set_blocks(self, parent_id: str, blocks: list[dict]) -> None:
        """Replace a page's or block's child blocks (see ``text_block``)."""
        key = normalize_id(parent_id)
        if key in self.children and key in self.free_pages:
            # Replacing a page's blocks is an edit: Notion moves the page's edit time.
            self.free_pages[key]["last_edited_time"] = datetime.now(UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
        self.children[key] = []
        for block in blocks:
            self._attach_block(key, copy.deepcopy(block))

    def _attach_block(self, parent_key: str, block: dict) -> None:
        parent_type = "page_id" if parent_key in self.free_pages else "block_id"
        block["parent"] = {"type": parent_type, parent_type: parent_key}
        block.setdefault("in_trash", False)
        nested = block.pop("_children", [])
        self.blocks[normalize_id(block["id"])] = block
        self.children.setdefault(parent_key, []).append(block)
        for child in nested:
            self._attach_block(normalize_id(block["id"]), copy.deepcopy(child))

    def add_page_tree(self, page_id: str, tree: dict, parent: dict | None = None) -> None:
        """Add a page and everything under it.

        ``tree`` is ``{"title": str, "blocks": [text_block(...)], "children":
        [{"id": str, ...tree of the child page}]}``. Each child page is added
        with a ``child_page`` block in its parent, after the parent's text.
        """
        self.add_free_page(page_id, tree.get("title", "Page"), parent=parent)
        self.set_blocks(page_id, tree.get("blocks", []))
        for child in tree.get("children", []):
            self.add_page_tree(
                child["id"], child, parent={"type": "page_id", "page_id": normalize_id(page_id)}
            )
            self._attach_block(
                normalize_id(page_id),
                {
                    "id": child["id"],
                    "type": "child_page",
                    "has_children": False,
                    "child_page": {"title": child.get("title", "Page")},
                },
            )

    def move_page(self, page_id: str, parent: dict) -> None:
        """Re-parent a page, as a person moving it in Notion would."""
        self.free_pages[normalize_id(page_id)]["parent"] = parent

    def trash_page(self, page_id: str) -> None:
        self.free_pages[normalize_id(page_id)]["in_trash"] = True

    def remove_child_page(self, parent_id: str, page_id: str) -> None:
        """Drop a page's ``child_page`` block from its parent and delete the page."""
        key = normalize_id(parent_id)
        self.children[key] = [
            b
            for b in self.children.get(key, [])
            if not (b["type"] == "child_page" and normalize_id(b["id"]) == normalize_id(page_id))
        ]
        self.free_pages.pop(normalize_id(page_id), None)

    def set_status(self, page_id: str, prop: str, status: str) -> None:
        for db_key, pages in self.databases.items():
            if page_id in pages:
                name, prop_id = self._lookup(db_key, prop)
                pages[page_id]["properties"][name] = {
                    "id": prop_id,
                    "type": "status",
                    "status": {"name": status},
                }
                return
        raise KeyError(page_id)

    def all_pages(self, database_id: str) -> list[dict]:
        return list(self.databases.get(normalize_id(database_id), {}).values())

    def count(self, method: str) -> int:
        return sum(1 for name, _ in self.calls if name == method)

    # NotionAPI.

    async def query_database(self, database_id: str, filter: dict | None) -> list[dict]:
        self.calls.append(("query_database", {"database_id": database_id, "filter": filter}))
        hang = self._take_read_hang("query_database")
        if hang is not None:
            await hang.wait()
        if self._fail_next_query is not None:
            error, self._fail_next_query = self._fail_next_query, None
            raise error
        db_key = normalize_id(database_id)
        name = self._name_for_id(db_key, filter["property"]) if filter else None
        pages = self.databases.get(db_key, {}).values()
        found = [
            copy.deepcopy(p)
            for p in pages
            if p["id"] not in self._hidden and _matches(p, name, filter)
        ]
        self._tick(self._hidden)
        return found

    async def create_page(self, database_id: str, properties: dict, children: list[dict]) -> dict:
        self.calls.append(("create_page", {"database_id": database_id, "properties": properties}))
        db_key = normalize_id(database_id)
        for key in properties:
            self._name_for_id(db_key, key)
        for value in properties.values():
            for item in value.get("title", []) + value.get("rich_text", []):
                _check_text_length(item["text"]["content"])
        for child in children:
            for item in child[child["type"]]["rich_text"]:
                _check_text_length(item["text"]["content"])
        page = self.add_page(database_id, properties)
        self.bodies[normalize_id(page["id"])] = copy.deepcopy(children)
        if self._visibility_delay:
            self._hidden[page["id"]] = self._visibility_delay
        await self._after_write("create")
        return page

    async def retrieve_page(self, page_id: str) -> dict:
        self.calls.append(("retrieve_page", {"page_id": page_id}))
        hang = self._take_read_hang("retrieve_page")
        if hang is not None:
            await hang.wait()
        for pages in self.databases.values():
            for stored_id, page in pages.items():
                if normalize_id(stored_id) == normalize_id(page_id):
                    return copy.deepcopy(page)
        if normalize_id(page_id) in self.free_pages:
            return copy.deepcopy(self.free_pages[normalize_id(page_id)])
        raise FakeNotFound(page_id)

    async def retrieve_block(self, block_id: str) -> dict:
        self.calls.append(("retrieve_block", {"block_id": block_id}))
        block = self.blocks.get(normalize_id(block_id))
        if block is None:
            raise FakeNotFound(block_id)
        return copy.deepcopy(block)

    async def retrieve_database(self, database_id: str) -> dict:
        self.calls.append(("retrieve_database", {"database_id": database_id}))
        key = normalize_id(database_id)
        if key not in self.databases and key not in self.database_info:
            raise FakeNotFound(database_id)
        info = self.database_info.get(key, {"title": "", "parent": {"type": "workspace"}})
        return {
            "object": "database",
            "id": database_id,
            "title": [_rich({"text": {"content": info["title"]}})],
            "parent": copy.deepcopy(info["parent"]),
        }

    async def list_child_blocks(self, block_id: str) -> list[dict]:
        self.calls.append(("list_child_blocks", {"block_id": block_id}))
        key = normalize_id(block_id)
        in_database = any(
            normalize_id(page_id) == key for pages in self.databases.values() for page_id in pages
        )
        if key not in self.free_pages and key not in self.blocks and not in_database:
            raise FakeNotFound(block_id)
        return copy.deepcopy(self.children.get(key, []))

    async def create_comment(self, page_id: str, text: str) -> dict:
        self.calls.append(("create_comment", {"page_id": page_id}))
        _check_text_length(text)
        comment = self._store_comment(page_id, text, self.bot_id)
        if self._visibility_delay:
            self._hidden_comments[comment["id"]] = self._visibility_delay
        await self._after_write("comment")
        return copy.deepcopy(comment)

    async def list_comments(self, page_id: str) -> list[dict]:
        self.calls.append(("list_comments", {"page_id": page_id}))
        found = [
            copy.deepcopy(c)
            for c in self.comments.get(normalize_id(page_id), [])
            if c["id"] not in self._hidden_comments
        ]
        self._tick(self._hidden_comments)
        return found

    async def bot_user_id(self) -> str:
        self.calls.append(("bot_user_id", {}))
        return self.bot_id

    def _store_comment(self, page_id: str, text: str, author_id: str) -> dict:
        comment = {
            "object": "comment",
            "id": str(uuid.uuid4()),
            "created_by": {"object": "user", "id": author_id},
            "rich_text": [_rich({"text": {"content": text}})],
        }
        self.comments.setdefault(normalize_id(page_id), []).append(comment)
        return comment

    async def _after_write(self, kind: str) -> None:
        """Run the injected failure of a write that has already taken effect."""
        if self._hang_after_write[kind]:
            await self._hang_after_write[kind].pop(0).wait()
        if self._fail_after_write[kind]:
            self._fail_after_write[kind] -= 1
            raise TimeoutError(f"simulated lost response after the {kind} took effect")

    @staticmethod
    def _tick(hidden: dict[str, int]) -> None:
        for key in list(hidden):
            hidden[key] -= 1
            if hidden[key] <= 0:
                del hidden[key]

    async def list_users(self) -> list[dict]:
        self.calls.append(("list_users", {}))
        return copy.deepcopy(self.users)

    async def retrieve_schema(self, database_id: str) -> dict[str, dict]:
        self.calls.append(("retrieve_schema", {"database_id": database_id}))
        return copy.deepcopy(self.schemas.get(normalize_id(database_id), {}))

    # Schema resolution.

    def _name_for_id(self, db_key: str, prop_id: str) -> str:
        """Resolve a property ID to its current name, as Notion would."""
        schema = self.schemas.get(db_key)
        if schema is None:
            return prop_id
        for name, prop in schema.items():
            if prop["id"] == prop_id:
                return name
        raise ValueError(f"Could not find property with id {prop_id!r}")

    def _lookup(self, db_key: str, key: str) -> tuple[str, str | None]:
        """Resolve an ID or a current name to ``(name, id)``."""
        schema = self.schemas.get(db_key) or {}
        for name, prop in schema.items():
            if prop["id"] == key:
                return name, prop["id"]
        if key in schema:
            return key, schema[key]["id"]
        return key, None

    def _to_response_properties(self, db_key: str, properties: dict) -> dict:
        """Turn request-shaped property values into the shape Notion returns."""
        out: dict = {}
        for key, value in properties.items():
            name, prop_id = self._lookup(db_key, key)
            if "title" in value:
                shaped = {"type": "title", "title": [_rich(t) for t in value["title"]]}
            elif "rich_text" in value:
                shaped = {"type": "rich_text", "rich_text": [_rich(t) for t in value["rich_text"]]}
            elif "people" in value:
                shaped = {"type": "people", "people": [{"id": p["id"]} for p in value["people"]]}
            elif "status" in value:
                shaped = {"type": "status", "status": value["status"]}
            else:
                shaped = copy.deepcopy(value)
            out[name] = {"id": prop_id, **shaped}
        return out


class Hang:
    """A call held open until a test lets it go — a Notion write or read, or a Slack edit.
    ``reached`` is set once it is waiting."""

    def __init__(self) -> None:
        self.reached = asyncio.Event()
        self._gate = asyncio.Event()

    def release(self) -> None:
        self._gate.set()

    async def wait(self) -> None:
        self.reached.set()
        await self._gate.wait()


class FakeNotFound(KeyError):
    """A missing page or block, carrying the HTTP status Notion would send."""

    status = 404


class FakeBadRequest(ValueError):
    """What Notion answers to a request it rejects, with its HTTP status."""

    status = 400


def _check_text_length(text: str) -> None:
    if len(text) > NOTION_TEXT_MAX:
        raise FakeBadRequest(f"text is {len(text)} characters; Notion allows {NOTION_TEXT_MAX}")


def text_block(
    kind: str,
    text: str = "",
    block_id: str | None = None,
    children: list[dict] | None = None,
    cells: list[str] | None = None,
) -> dict:
    """A Notion block in response shape, for the fake. ``kind`` is the block type.

    A ``table_row`` takes ``cells`` instead of ``text``. Nested ``children`` are
    attached by ``FakeNotionAPI.set_blocks`` when the block is a child of a page.
    """
    block_id = block_id or str(uuid.uuid4())
    if kind == "table_row":
        body: dict = {"cells": [[_rich({"text": {"content": c}})] for c in (cells or [])]}
    else:
        body = {"rich_text": [_rich({"text": {"content": text}})]}
    return {
        "object": "block",
        "id": block_id,
        "type": kind,
        kind: body,
        "has_children": bool(children),
        "_children": children or [],
    }


def _rich(t: dict) -> dict:
    content = t.get("plain_text") or t.get("text", {}).get("content", "")
    return {"type": "text", "text": {"content": content}, "plain_text": content}


def _plain(rich: list[dict]) -> str:
    return "".join(t.get("plain_text") or t.get("text", {}).get("content", "") for t in rich)


def _matches(page: dict, prop_name: str | None, filter: dict | None) -> bool:
    if not filter:
        return True
    prop = page["properties"].get(prop_name, {})
    if "title" in filter:
        return filter["title"]["contains"].lower() in _plain(prop.get("title", [])).lower()
    if "rich_text" in filter:
        return _plain(prop.get("rich_text", [])) == filter["rich_text"]["equals"]
    if "people" in filter:
        return any(p["id"] == filter["people"]["contains"] for p in prop.get("people", []))
    raise ValueError(f"FakeNotionAPI does not understand filter {filter}")


# --- Grant check --------------------------------------------------------------

# Notion property types each role may have.
EXPECTED_TYPES = {
    "title": ("title",),
    "status": ("status", "select"),
    "requested_by": ("people",),
    "request_id": ("rich_text",),
}


@dataclass
class GrantCheck:
    """Result of checking one grant's property mapping against the live schema.

    ``resolved`` maps each role to the Notion property ID found for it.
    ``problems`` stop the worker from starting; ``notes`` are worth knowing but
    do not (a property renamed in Notion is still found by its ID).
    """

    resolved: dict[str, str] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _grant_role_config(grant: NotionDatabaseGrant) -> dict[str, tuple[str, str | None]]:
    return {
        "title": (grant.title_prop, grant.title_prop_id),
        "status": (grant.status_prop, grant.status_prop_id),
        "requested_by": (grant.requested_by_prop, grant.requested_by_prop_id),
        "request_id": (grant.request_id_prop, grant.request_id_prop_id),
    }


def _find_by_name_loosely(schema: dict[str, dict], name: str) -> tuple[str, str] | None:
    """Find a near-match: same name ignoring case and surrounding whitespace."""
    wanted = name.strip().casefold()
    for actual in schema:
        if actual.strip().casefold() == wanted:
            return actual, _describe_difference(name, actual)
    return None


def _describe_difference(configured: str, actual: str) -> str:
    if actual.rstrip() != actual and actual.rstrip() == configured:
        return "trailing space"
    if actual.lstrip() != actual and actual.lstrip() == configured:
        return "leading space"
    if actual.strip() == configured.strip():
        return "surrounding whitespace differs"
    return "different letter case"


async def check_grant(api: NotionAPI, grant: NotionDatabaseGrant, require_ids: bool) -> GrantCheck:
    """Check a grant's property mapping against the database's live schema.

    This only reads the schema of the granted database with the agent's own
    token. It never creates, renames or edits a property: a missing property is
    reported as a problem for a person to fix in Notion.

    With ``require_ids`` true (the worker), a role without a configured property
    ID is a problem. With it false (the check command), a role without an ID is
    looked up by exact name so its ID can be printed; a near-match by name is
    still a problem, because the agent must not guess which property was meant.
    """
    schema = await api.retrieve_schema(grant.id)
    by_id = {prop["id"]: (name, prop) for name, prop in schema.items()}
    result = GrantCheck()

    for role, (configured_name, configured_id) in _grant_role_config(grant).items():
        found: tuple[str, dict] | None = None
        if configured_id:
            found = by_id.get(configured_id)
            if found is None:
                result.problems.append(
                    f"{role}: no property with ID '{configured_id}' in the database"
                )
                continue
            if found[0] != configured_name:
                result.notes.append(
                    f"{role}: configured name '{configured_name}' is now '{found[0]}' in Notion"
                )
        else:
            if require_ids:
                result.problems.append(
                    f"{role}: no property ID configured; run agentos.check_grants"
                )
            if configured_name in schema:
                found = (configured_name, schema[configured_name])
            else:
                near = _find_by_name_loosely(schema, configured_name)
                if near is None:
                    result.problems.append(
                        f"{role}: no property named '{configured_name}' in the database"
                    )
                else:
                    result.problems.append(
                        f"{role}: no property named '{configured_name}'; "
                        f"found '{near[0]}' ({near[1]})"
                    )
                continue

        name, prop = found
        result.resolved[role] = prop["id"]
        if prop["type"] not in EXPECTED_TYPES[role]:
            expected = " or ".join(EXPECTED_TYPES[role])
            result.problems.append(
                f"{role}: property '{name}' has type '{prop['type']}', expected {expected}"
            )
        if role == "status" and prop["type"] in ("status", "select"):
            for done in grant.done_values:
                if done not in prop.get("options", []):
                    result.problems.append(
                        f"status: done value '{done}' is not an option of property '{name}'"
                    )
    return result


@dataclass
class LiveObject:
    """What Notion says a granted database or page is called and where it sits.

    ``title`` is the object's own live title; ``parent`` reads like ``page 'Handbook'``,
    ``database 'Tasks'`` or ``workspace``. Either is None when Notion would not show it
    to the integration. Titles only, never page text.
    """

    title: str | None = None
    parent: str | None = None


_MAX_PARENT_HOPS = 6


def _page_title(page: dict) -> str:
    for prop in (page.get("properties") or {}).values():
        if prop.get("type") == "title":
            return _plain(prop.get("title", []))
    return ""


async def _parent_label(api: NotionAPI, parent: dict) -> str:
    """The first ancestor's kind and title; a block parent is followed up to a page."""
    for _ in range(_MAX_PARENT_HOPS):
        kind = parent.get("type")
        if kind == "workspace":
            return "workspace"
        try:
            if parent.get("database_id"):
                db = await api.retrieve_database(parent["database_id"])
                return f"database '{_plain(db.get('title', []))}'"
            if kind == "page_id":
                page = await api.retrieve_page(parent["page_id"])
                return f"page '{_page_title(page)}'"
            if kind == "block_id":
                parent = (await api.retrieve_block(parent["block_id"])).get("parent") or {}
                continue
        except Exception as err:
            if is_unreachable(err):
                return "not shared with the integration"
            raise
        break
    return "unknown"


async def describe_object(api: NotionAPI, kind: str, object_id: str) -> LiveObject:
    """Fetch the live title and first parent of a granted database (``kind`` ``database``)
    or page. An object or parent Notion refuses to show (403, 404) leaves that field None
    or says so; any other failure propagates for the caller to judge."""
    try:
        if kind == "database":
            obj = await api.retrieve_database(object_id)
            title = _plain(obj.get("title", []))
        else:
            obj = await api.retrieve_page(object_id)
            title = _page_title(obj)
    except Exception as err:
        if is_unreachable(err):
            return LiveObject()
        raise
    return LiveObject(title, await _parent_label(api, obj.get("parent") or {}))


async def check_page_grant(api: NotionAPI, grant: NotionPageGrant) -> list[str]:
    """Problems with a page grant: the page must be readable and not trashed.

    A read only, with the agent's own token. Returns an empty list when the
    page is fine. Error details are limited to the error type, never page text.
    """
    try:
        page = await api.retrieve_page(grant.id)
    except Exception as err:  # noqa: BLE001 - any failure to read means the grant is unusable
        return [f"page '{grant.name}' cannot be read with this token ({type(err).__name__})"]
    if page.get("in_trash") or page.get("archived"):
        return [f"page '{grant.name}' is in the trash or archived"]
    return []


# --- Per-tenant directory cache ------------------------------------------------

USER_CACHE_TTL_SECONDS = 600.0


class NotionUserCache:
    """What one tenant's Notion workspace says about people, kept for a while.

    One instance lives in the tenant's runtime, so every connector built for a call
    shares it. A lookup of a person reads the whole user list once and keeps it
    as an email-to-ID map until it expires. An email missing from the map is a
    cached "no match" for the same time, so a requester with no Notion account
    does not page the user list again on every call. The integration's own bot
    user ID is kept the same way.
    """

    def __init__(
        self,
        ttl_seconds: float = USER_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._ttl = ttl_seconds
        self._clock = clock
        self._users: dict[str, str] | None = None
        self._users_until = 0.0
        self._bot: str | None = None
        self._bot_until = 0.0
        self._lock = asyncio.Lock()

    async def user_id(self, api: NotionAPI, email: str | None) -> str | None:
        """The Notion user ID for an email, or None when the workspace has no such person."""
        if not email:
            return None
        key = email.strip().lower()
        async with self._lock:
            if self._users is None or self._clock() >= self._users_until:
                users: dict[str, str] = {}
                for user in await api.list_users():
                    person_email = ((user.get("person") or {}).get("email") or "").strip().lower()
                    if person_email:
                        users.setdefault(person_email, user["id"])
                self._users = users
                self._users_until = self._clock() + self._ttl
            return self._users.get(key)

    async def bot_id(self, api: NotionAPI) -> str:
        async with self._lock:
            if self._bot is None or self._clock() >= self._bot_until:
                self._bot = await api.bot_user_id()
                self._bot_until = self._clock() + self._ttl
            return self._bot


# --- Connector ----------------------------------------------------------------

UNKNOWN_PAGE_TEXT = "I couldn't open that Notion page."


class NotionConnector:
    def __init__(
        self,
        api: NotionAPI,
        grants: list[NotionDatabaseGrant],
        manifest: Manifest | None = None,
        users: NotionUserCache | None = None,
    ):
        for g in grants:
            missing = g.missing_property_ids()
            if missing:
                raise ConfigError(
                    f"grant '{g.name}': no property ID for {', '.join(missing)}; "
                    "run agentos.check_grants and add its property_ids block"
                )
        self._api = api
        self._grants = {normalize_id(g.id): g for g in grants}
        self.manifest = manifest or load_manifest(MANIFEST_DIR / "notion.yaml")
        self.users = users or NotionUserCache()

    @property
    def grants(self) -> list[NotionDatabaseGrant]:
        return list(self._grants.values())

    def tools_for_llm(self) -> list[dict]:
        """Tool definitions the LLM may call; requester fields are absent."""
        return [
            self.manifest.tools[name].llm_definition()
            for name in LLM_TOOLS
            if self.manifest.tools[name].offer_to_llm
        ]

    def grant_for(self, database_id: str) -> NotionDatabaseGrant:
        """Return the grant for a database, or raise before any API call."""
        grant = self._grants.get(normalize_id(database_id or ""))
        if grant is None:
            raise GrantDenied("This agent has not been granted access to that Notion database.")
        return grant

    async def search_items(self, query: str) -> list[NotionItem]:
        items: list[NotionItem] = []
        for grant in self.grants:
            pages = await self._api.query_database(
                grant.id, {"property": grant.title_prop_id, "title": {"contains": query}}
            )
            items.extend(self._to_item(grant, p) for p in pages)
        return items

    async def my_items(self, requester_email: str) -> list[NotionItem]:
        user_id = await self._notion_user_id(requester_email)
        if user_id is None:
            raise UnknownRequester(
                "I couldn't match your Slack email to a Notion user, so I can't look up your items."
            )
        items: list[NotionItem] = []
        for grant in self.grants:
            pages = await self._api.query_database(
                grant.id, {"property": grant.requested_by_prop_id, "people": {"contains": user_id}}
            )
            items.extend(self._to_item(grant, p) for p in pages)
        return items

    async def find_item(self, database_id: str, idem_key: str) -> NotionItem | None:
        """The item an earlier attempt created under ``idem_key``, if Notion shows it yet."""
        grant = self.grant_for(database_id)
        existing = await self._api.query_database(
            grant.id, {"property": grant.request_id_prop_id, "rich_text": {"equals": idem_key}}
        )
        return self._to_item(grant, existing[0]) if existing else None

    async def create_item(
        self,
        database_id: str,
        title: str,
        details: str,
        idem_key: str,
        notion_user_id: str | None = None,
        requester_line: str | None = None,
    ) -> NotionItem:
        """Create the page. It does not look for an earlier one: see ``find_item``.

        ``notion_user_id`` fills the requester property. ``requester_line`` is the
        sentence "Requested by <name> via <agent>" that goes into the body when the
        requester has no Notion user. Both come from the runtime, already shown on
        the approval card.
        """
        grant = self.grant_for(database_id)
        if max(len(title), len(details)) > NOTION_TEXT_MAX:
            raise ConnectorError(f"The title or details are over Notion's {NOTION_TEXT_MAX}.")
        properties: dict = {
            grant.title_prop_id: {"title": [{"text": {"content": title}}]},
            grant.request_id_prop_id: {"rich_text": [{"text": {"content": idem_key}}]},
        }
        if notion_user_id:
            properties[grant.requested_by_prop_id] = {"people": [{"id": notion_user_id}]}
        texts = [details] + ([requester_line] if requester_line else [])
        children = [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]},
            }
            for text in texts
        ]
        page = await self._api.create_page(grant.id, properties, children)
        return self._to_item(grant, page)

    async def get_item(self, page_id: str) -> NotionItem:
        """One item of a granted database; see ``resolve_item`` for what is hidden."""
        return (await self._granted_page(page_id))[1]

    async def resolve_item(self, item_ref: str) -> NotionItem:
        """Find the page a link or ID names, and check its database is granted.

        The grant is checked from the page's own parent database before the
        caller may write anything. A page that is not in a granted database
        (or not in a database at all) raises the same ``UnknownItem`` as a page
        that does not exist, so the answer does not say whether it exists.
        """
        page_id = _page_id_from_ref(item_ref)
        if page_id is None:
            raise UnknownItem("I couldn't find a Notion page in that link or ID.")
        grant, item = await self._granted_page(page_id)
        item.database_id = grant.id
        return item

    async def _granted_page(self, page_id: str) -> tuple[NotionDatabaseGrant, NotionItem]:
        try:
            page = await self._api.retrieve_page(page_id)
        except Exception as err:
            if is_unreachable(err):
                raise UnknownItem(UNKNOWN_PAGE_TEXT) from err
            raise
        grant = self._grants.get(normalize_id((page.get("parent") or {}).get("database_id") or ""))
        if grant is None:
            raise UnknownItem(UNKNOWN_PAGE_TEXT)
        if page.get("in_trash") or page.get("archived"):
            raise UnknownItem("The item was moved to the trash.")
        return grant, self._to_item(grant, page)

    async def find_comment(self, page_id: str, idem_key: str) -> str | None:
        """The ID of the comment an earlier attempt made under ``idem_key``, if visible.

        Only comments written by this integration count, and the whole reference must
        match: anyone else's comment that quotes the reference proves nothing.
        """
        ref = comment_ref(idem_key)
        bot = await self.users.bot_id(self._api)
        for comment in await self._api.list_comments(page_id):
            if (comment.get("created_by") or {}).get("id") != bot:
                continue
            if ref in _plain(comment.get("rich_text", [])):
                return comment["id"]
        return None

    async def create_comment(self, page_id: str, text: str, idem_key: str, agent_name: str) -> str:
        """Add the comment, ending with the agent's name and the key's reference."""
        body = f"{text}\n\n— {agent_name[:SIGNATURE_NAME_MAX]}, {comment_ref(idem_key)}"
        if len(body) > NOTION_TEXT_MAX:
            raise ConnectorError(
                f"The comment is {len(body)} characters with its signature; "
                f"Notion allows {NOTION_TEXT_MAX}."
            )
        created = await self._api.create_comment(page_id, body)
        return created["id"]

    async def _notion_user_id(self, email: str | None) -> str | None:
        return await self.users.user_id(self._api, email)

    @staticmethod
    def _to_item(grant: NotionDatabaseGrant, page: dict) -> NotionItem:
        by_id = {
            p.get("id"): p for p in (page.get("properties") or {}).values() if isinstance(p, dict)
        }
        title = _plain((by_id.get(grant.title_prop_id) or {}).get("title", []))
        status_prop = by_id.get(grant.status_prop_id) or {}
        status_val = status_prop.get("status") or status_prop.get("select")
        status = status_val.get("name") if isinstance(status_val, dict) else None
        return NotionItem(page_id=page["id"], url=page.get("url", ""), title=title, status=status)
