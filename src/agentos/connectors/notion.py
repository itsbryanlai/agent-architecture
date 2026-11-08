"""Notion connector.

The agent acts with its own access, never the asker's: every call uses the
tenant's single Notion integration token, and every call is checked against
the agent's grants (the database IDs it was given) *before* anything is sent to
Notion. A call outside the grants raises ``GrantDenied`` and no request goes
out. Activities turn ``ConnectorError``s into non-retryable failures.

Requester-scoped reads ("my items") take the requester's email from the
runtime, which got it from Slack. The LLM never supplies it.

Item creation is exactly-once. Notion has no idempotency key, so every item the
agent creates carries the key in a hidden text property, and the connector
queries for that key before creating. A retry after a lost response finds the
item it already made instead of making a second one.
"""

from __future__ import annotations

import copy
import uuid
from typing import Protocol

from agentos.config import NotionDatabaseGrant
from agentos.connectors.manifest import MANIFEST_DIR, Manifest, load_manifest
from agentos.models import NotionItem

LLM_TOOLS = ("search_items", "my_items", "create_item")


class ConnectorError(Exception):
    """Base for connector errors that retrying cannot fix."""


class GrantDenied(ConnectorError):
    """The call targets a database or page outside the agent's grants."""


class UnknownRequester(ConnectorError):
    """The requester's email matches no user in the target system."""


def normalize_id(notion_id: str) -> str:
    """Notion IDs appear with and without hyphens; compare them without."""
    return notion_id.replace("-", "").lower()


# --- Notion API surface -------------------------------------------------------


class NotionAPI(Protocol):
    """The only Notion calls the connector is allowed to make."""

    async def query_database(self, database_id: str, filter: dict | None) -> list[dict]: ...

    async def create_page(
        self, database_id: str, properties: dict, children: list[dict]
    ) -> dict: ...

    async def retrieve_page(self, page_id: str) -> dict: ...

    async def list_users(self) -> list[dict]: ...


class RealNotionAPI:
    """Wraps ``notion_client.AsyncClient``.

    The client speaks the Notion API version in which a database holds one or
    more data sources: queries and page creation go through the database's data
    sources, which are looked up once per database.
    """

    def __init__(self, token: str):
        from notion_client import AsyncClient

        self._client = AsyncClient(auth=token)
        self._data_sources: dict[str, list[str]] = {}

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

    Pages are stored per database in Notion's response shape. Only the filter
    forms the connector uses are understood.
    """

    def __init__(self, users: list[dict] | None = None):
        self.databases: dict[str, dict[str, dict]] = {}
        self.users: list[dict] = list(users or [])
        self.calls: list[tuple[str, dict]] = []
        self._fail_next_create = False

    # Test hooks.

    def fail_next_create_after_write(self) -> None:
        """Make the next create store the page, then raise a timeout."""
        self._fail_next_create = True

    def add_database(self, database_id: str) -> None:
        self.databases.setdefault(normalize_id(database_id), {})

    def add_page(self, database_id: str, properties: dict) -> dict:
        db_key = normalize_id(database_id)
        page_id = str(uuid.uuid4())
        page = {
            "object": "page",
            "id": page_id,
            "url": f"https://www.notion.so/{page_id.replace('-', '')}",
            "parent": {"type": "database_id", "database_id": database_id},
            "properties": _to_response_properties(properties),
        }
        self.databases.setdefault(db_key, {})[page_id] = page
        return copy.deepcopy(page)

    def set_status(self, page_id: str, prop: str, status: str) -> None:
        for pages in self.databases.values():
            if page_id in pages:
                pages[page_id]["properties"][prop] = {"type": "status", "status": {"name": status}}
                return
        raise KeyError(page_id)

    def all_pages(self, database_id: str) -> list[dict]:
        return list(self.databases.get(normalize_id(database_id), {}).values())

    def count(self, method: str) -> int:
        return sum(1 for name, _ in self.calls if name == method)

    # NotionAPI.

    async def query_database(self, database_id: str, filter: dict | None) -> list[dict]:
        self.calls.append(("query_database", {"database_id": database_id, "filter": filter}))
        pages = self.databases.get(normalize_id(database_id), {}).values()
        return [copy.deepcopy(p) for p in pages if _matches(p, filter)]

    async def create_page(self, database_id: str, properties: dict, children: list[dict]) -> dict:
        self.calls.append(("create_page", {"database_id": database_id, "properties": properties}))
        page = self.add_page(database_id, properties)
        if self._fail_next_create:
            self._fail_next_create = False
            raise TimeoutError("simulated lost response after the page was created")
        return page

    async def retrieve_page(self, page_id: str) -> dict:
        self.calls.append(("retrieve_page", {"page_id": page_id}))
        for pages in self.databases.values():
            if page_id in pages:
                return copy.deepcopy(pages[page_id])
        raise KeyError(page_id)

    async def list_users(self) -> list[dict]:
        self.calls.append(("list_users", {}))
        return copy.deepcopy(self.users)


def _to_response_properties(properties: dict) -> dict:
    """Turn request-shaped property values into the shape Notion returns."""
    out: dict = {}
    for name, value in properties.items():
        if "title" in value:
            out[name] = {"type": "title", "title": [_rich(t) for t in value["title"]]}
        elif "rich_text" in value:
            out[name] = {"type": "rich_text", "rich_text": [_rich(t) for t in value["rich_text"]]}
        elif "people" in value:
            out[name] = {"type": "people", "people": [{"id": p["id"]} for p in value["people"]]}
        elif "status" in value:
            out[name] = {"type": "status", "status": value["status"]}
        else:
            out[name] = copy.deepcopy(value)
    return out


def _rich(t: dict) -> dict:
    content = t.get("plain_text") or t.get("text", {}).get("content", "")
    return {"type": "text", "text": {"content": content}, "plain_text": content}


def _plain(rich: list[dict]) -> str:
    return "".join(t.get("plain_text") or t.get("text", {}).get("content", "") for t in rich)


def _matches(page: dict, filter: dict | None) -> bool:
    if not filter:
        return True
    prop = page["properties"].get(filter["property"], {})
    if "title" in filter:
        return filter["title"]["contains"].lower() in _plain(prop.get("title", [])).lower()
    if "rich_text" in filter:
        return _plain(prop.get("rich_text", [])) == filter["rich_text"]["equals"]
    if "people" in filter:
        return any(p["id"] == filter["people"]["contains"] for p in prop.get("people", []))
    raise ValueError(f"FakeNotionAPI does not understand filter {filter}")


# --- Connector ----------------------------------------------------------------


class NotionConnector:
    def __init__(
        self,
        api: NotionAPI,
        grants: list[NotionDatabaseGrant],
        manifest: Manifest | None = None,
    ):
        self._api = api
        self._grants = {normalize_id(g.id): g for g in grants}
        self.manifest = manifest or load_manifest(MANIFEST_DIR / "notion.yaml")
        self._user_ids: dict[str, str | None] = {}

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
                grant.id, {"property": grant.title_prop, "title": {"contains": query}}
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
                grant.id, {"property": grant.requested_by_prop, "people": {"contains": user_id}}
            )
            items.extend(self._to_item(grant, p) for p in pages)
        return items

    async def create_item(
        self,
        database_id: str,
        title: str,
        details: str,
        requester_email: str | None,
        idem_key: str,
    ) -> NotionItem:
        grant = self.grant_for(database_id)
        existing = await self._api.query_database(
            grant.id, {"property": grant.request_id_prop, "rich_text": {"equals": idem_key}}
        )
        if existing:
            return self._to_item(grant, existing[0])

        properties: dict = {
            grant.title_prop: {"title": [{"text": {"content": title}}]},
            grant.request_id_prop: {"rich_text": [{"text": {"content": idem_key}}]},
        }
        user_id = await self._notion_user_id(requester_email) if requester_email else None
        if user_id:
            properties[grant.requested_by_prop] = {"people": [{"id": user_id}]}
        children = [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": details}}]},
            }
        ]
        page = await self._api.create_page(grant.id, properties, children)
        return self._to_item(grant, page)

    async def get_item(self, page_id: str) -> NotionItem:
        page = await self._api.retrieve_page(page_id)
        parent_db = (page.get("parent") or {}).get("database_id") or ""
        grant = self.grant_for(parent_db)
        return self._to_item(grant, page)

    async def _notion_user_id(self, email: str | None) -> str | None:
        if not email:
            return None
        key = email.strip().lower()
        if key not in self._user_ids:
            match = None
            for user in await self._api.list_users():
                person_email = ((user.get("person") or {}).get("email") or "").strip().lower()
                if person_email and person_email == key:
                    match = user["id"]
                    break
            self._user_ids[key] = match
        return self._user_ids[key]

    @staticmethod
    def _to_item(grant: NotionDatabaseGrant, page: dict) -> NotionItem:
        props = page.get("properties") or {}
        title = _plain((props.get(grant.title_prop) or {}).get("title", []))
        status_prop = props.get(grant.status_prop) or {}
        status_val = status_prop.get("status") or status_prop.get("select")
        status = status_val.get("name") if isinstance(status_val, dict) else None
        return NotionItem(page_id=page["id"], url=page.get("url", ""), title=title, status=status)
