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
from dataclasses import dataclass, field
from typing import Protocol

from agentos.config import ConfigError, NotionDatabaseGrant
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
        self._fail_next_create = False

    # Test hooks.

    def fail_next_create_after_write(self) -> None:
        """Make the next create store the page, then raise a timeout."""
        self._fail_next_create = True

    def add_database(self, database_id: str) -> None:
        self.databases.setdefault(normalize_id(database_id), {})

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
            "parent": {"type": "database_id", "database_id": database_id},
            "properties": self._to_response_properties(db_key, properties),
        }
        self.databases.setdefault(db_key, {})[page_id] = page
        return copy.deepcopy(page)

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
        db_key = normalize_id(database_id)
        name = self._name_for_id(db_key, filter["property"]) if filter else None
        pages = self.databases.get(db_key, {}).values()
        return [copy.deepcopy(p) for p in pages if _matches(p, name, filter)]

    async def create_page(self, database_id: str, properties: dict, children: list[dict]) -> dict:
        self.calls.append(("create_page", {"database_id": database_id, "properties": properties}))
        db_key = normalize_id(database_id)
        for key in properties:
            self._name_for_id(db_key, key)
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


# --- Connector ----------------------------------------------------------------


class NotionConnector:
    def __init__(
        self,
        api: NotionAPI,
        grants: list[NotionDatabaseGrant],
        manifest: Manifest | None = None,
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
            grant.id, {"property": grant.request_id_prop_id, "rich_text": {"equals": idem_key}}
        )
        if existing:
            return self._to_item(grant, existing[0])

        properties: dict = {
            grant.title_prop_id: {"title": [{"text": {"content": title}}]},
            grant.request_id_prop_id: {"rich_text": [{"text": {"content": idem_key}}]},
        }
        user_id = await self._notion_user_id(requester_email) if requester_email else None
        if user_id:
            properties[grant.requested_by_prop_id] = {"people": [{"id": user_id}]}
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
        by_id = {
            p.get("id"): p for p in (page.get("properties") or {}).values() if isinstance(p, dict)
        }
        title = _plain((by_id.get(grant.title_prop_id) or {}).get("title", []))
        status_prop = by_id.get(grant.status_prop_id) or {}
        status_val = status_prop.get("status") or status_prop.get("select")
        status = status_val.get("name") if isinstance(status_val, dict) else None
        return NotionItem(page_id=page["id"], url=page.get("url", ""), title=title, status=status)
