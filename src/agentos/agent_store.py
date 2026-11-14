"""Agents in the database: numbered versions with a publish lifecycle.

A version is a ``draft`` while it is edited, ``pending`` once it is submitted
for approval, ``published`` when an approver publishes it, and ``superseded``
when a later version is published. At most one version of an agent is
published, and a worker loads that one on each turn. A published version never
changes. An edit starts a new draft, so a narrowed grant or a new prompt takes
effect only when the draft is published, and then on the next call.

Moves a version may make: draft to pending, pending to draft (withdrawn or
rejected), pending to published, published to superseded. The database
enforces them with a trigger for every role; the in-memory store mirrors them.

``definition`` is one agent block in the YAML shape, without ``version`` (the
store assigns it as the agent's highest number plus one). It is parsed by
``config.parse_agent``, the same function that reads the YAML file, and the
front-door rules (``config.check_front_doors``) hold for a tenant's published
set: a publish or import that would break them is refused.

The store records who approved a publish and does not decide who may. The
check for who is allowed to approve belongs in ``control.py``, the one service
layer that calls the write methods; this module is below it. The store does
refuse a publish of a version lower than the one already published, so no
caller can roll an agent back by publishing an old version. A rollback is a new
draft copied from the old version, then submitted and published.

Tenant settings (``default_agent``, ``channels``, workspace, secrets) stay in
the YAML file. The YAML ``agents`` section is only the source of a one-time
import::

    uv run python -m agentos.agent_store import
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agentos.config import (
    AgentsConfig,
    AgentVersion,
    ConfigError,
    TenantsConfig,
    agent_definition,
    check_front_doors,
    parse_agent,
)
from agentos.stores import tenant_transaction

IMPORT_USER = "import:yaml"

DRAFT = "draft"
PENDING = "pending"
PUBLISHED = "published"
SUPERSEDED = "superseded"

# The moves the database trigger allows. The in-memory store applies the same table.
ALLOWED_MOVES = {
    (DRAFT, PENDING),
    (PENDING, DRAFT),
    (PENDING, PUBLISHED),
    (PUBLISHED, SUPERSEDED),
}


class AgentStoreError(ValueError):
    """The change is not allowed or names nothing that exists."""


class AgentNotFound(AgentStoreError):
    """The change names an agent or a version that does not exist."""


@dataclass(frozen=True)
class AgentVersionRecord:
    """One stored version of an agent, with its lifecycle."""

    tenant_id: str
    agent_id: str
    version: int
    status: str
    definition: dict[str, Any]
    created_by: str
    created_at: datetime | None = None
    submitted_by: str | None = None
    submitted_at: datetime | None = None
    approved_by: str | None = None
    published_at: datetime | None = None

    def agent(self) -> AgentVersion:
        """The definition parsed and validated like a YAML agent block."""
        return parse_agent(self.agent_id, self.definition, self.version)


@dataclass(frozen=True)
class AgentRecord:
    """An agent as a whole: who created it. Its versions are ``AgentVersionRecord``."""

    tenant_id: str
    agent_id: str
    created_by: str
    created_at: datetime | None = None


@dataclass
class ImportResult:
    imported: list[str] = dataclasses.field(default_factory=list)
    skipped: list[str] = dataclasses.field(default_factory=list)


class AgentStore(Protocol):
    # --- reads ---------------------------------------------------------------

    async def published(self, tenant_id: str, agent_id: str) -> AgentVersion | None:
        """The agent's published version, or None when it has none."""
        ...

    async def published_all(self, tenant_id: str) -> dict[str, AgentVersion]:
        """The published version of every agent of the tenant that has one."""
        ...

    async def get_version(
        self, tenant_id: str, agent_id: str, version: int
    ) -> AgentVersionRecord | None: ...

    async def versions(self, tenant_id: str, agent_id: str) -> list[AgentVersionRecord]:
        """Every version of one agent, newest first."""
        ...

    async def get_agent(self, tenant_id: str, agent_id: str) -> AgentRecord | None: ...

    async def list_agents(self, tenant_id: str) -> list[AgentRecord]:
        """Every agent of the tenant, drafts included, by agent ID."""
        ...

    # --- writes --------------------------------------------------------------

    async def create_agent(
        self, tenant_id: str, agent_id: str, definition: dict[str, Any], created_by: str
    ) -> AgentVersionRecord:
        """Create the agent with version 1 as a draft."""
        ...

    async def create_draft(
        self, tenant_id: str, agent_id: str, created_by: str, from_version: int | None = None
    ) -> AgentVersionRecord:
        """A new draft copied from ``from_version``, or from the published version."""
        ...

    async def replace_draft(
        self, tenant_id: str, agent_id: str, version: int, definition: dict[str, Any]
    ) -> AgentVersionRecord:
        """Replace a draft's definition. Any other status is refused."""
        ...

    async def submit(
        self, tenant_id: str, agent_id: str, version: int, submitted_by: str
    ) -> AgentVersionRecord:
        """Draft to pending."""
        ...

    async def withdraw(self, tenant_id: str, agent_id: str, version: int) -> AgentVersionRecord:
        """Pending back to draft: the submitter withdraws it or an approver rejects it."""
        ...

    async def publish(
        self, tenant_id: str, agent_id: str, version: int, approved_by: str
    ) -> AgentVersionRecord:
        """Pending to published; the agent's previous published version becomes superseded.

        Refused with ``AgentStoreError`` when a higher version is already
        published, and with ``ConfigError`` when the tenant's published set
        would break the front-door rules.
        """
        ...

    async def import_agents(
        self, tenant_id: str, agents: dict[str, AgentVersion], imported_by: str = IMPORT_USER
    ) -> ImportResult:
        """Insert each agent that has no row yet as version 1, published.

        An agent that already exists is skipped. The published set after the
        import is checked like a publish.
        """
        ...


def check_publish(published: dict[str, AgentVersion], new: AgentVersion) -> None:
    """Raise ``ConfigError`` when publishing ``new`` would break the front-door rules."""
    try:
        check_front_doors({**published, new.agent_id: new})
    except ConfigError as err:
        raise ConfigError(f"cannot publish agent '{new.agent_id}': {err}") from err


def _without_version(definition: dict[str, Any]) -> dict[str, Any]:
    """The block as stored. The store assigns the version number, so a given one is dropped."""
    return {k: v for k, v in definition.items() if k != "version"}


def _validated(agent_id: str, definition: dict[str, Any]) -> dict[str, Any]:
    """The definition to store, after parsing it exactly as a YAML block is parsed."""
    if not isinstance(definition, dict):
        raise ConfigError(f"agent '{agent_id}': the definition must be a mapping")
    stored = _without_version(definition)
    try:
        parse_agent(agent_id, stored, 1)
    except ConfigError:
        raise
    except (ValueError, TypeError, AttributeError) as err:
        # A value of the wrong type (a word where a number goes) is a bad definition too.
        raise ConfigError(f"agent '{agent_id}': the definition is invalid ({err})") from err
    return stored


def _stale(agent_id: str, version: int, published: int) -> AgentStoreError:
    return AgentStoreError(
        f"agent '{agent_id}' version {version} is older than the published version "
        f"{published}; copy it into a new draft to roll back"
    )


def _no_such(agent_id: str, version: int) -> AgentNotFound:
    return AgentNotFound(f"agent '{agent_id}' has no version {version}")


def _wrong_status(agent_id: str, version: int, status: str, wanted: str) -> AgentStoreError:
    return AgentStoreError(
        f"agent '{agent_id}' version {version} is {status}; this change needs {wanted}"
    )


# --- Postgres -----------------------------------------------------------------

_COLUMNS = (
    "tenant_id, agent_id, version, status, definition, created_by, created_at, "
    "submitted_by, submitted_at, approved_by, published_at"
)
_COLUMN_NAMES = [c.strip() for c in _COLUMNS.split(",")]
_AGENT_COLUMNS = "tenant_id, agent_id, created_by, created_at"


def _record(row) -> AgentVersionRecord:
    return AgentVersionRecord(**dict(zip(_COLUMN_NAMES, row)))


class PostgresAgentStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def published(self, tenant_id: str, agent_id: str) -> AgentVersion | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM agent_versions
                WHERE tenant_id = %s AND agent_id = %s AND status = 'published'
                """,
                (tenant_id, agent_id),
            )
            row = await cur.fetchone()
        return None if row is None else _record(row).agent()

    async def published_all(self, tenant_id: str) -> dict[str, AgentVersion]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            return await self._published_set(conn, tenant_id)

    async def _published_set(self, conn, tenant_id: str) -> dict[str, AgentVersion]:
        cur = await conn.execute(
            f"""
            SELECT {_COLUMNS} FROM agent_versions
            WHERE tenant_id = %s AND status = 'published'
            ORDER BY agent_id
            """,
            (tenant_id,),
        )
        agents = [_record(row).agent() for row in await cur.fetchall()]
        return {agent.agent_id: agent for agent in agents}

    async def get_version(
        self, tenant_id: str, agent_id: str, version: int
    ) -> AgentVersionRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            return await self._get(conn, tenant_id, agent_id, version)

    async def _get(
        self, conn, tenant_id: str, agent_id: str, version: int
    ) -> AgentVersionRecord | None:
        cur = await conn.execute(
            f"""
            SELECT {_COLUMNS} FROM agent_versions
            WHERE tenant_id = %s AND agent_id = %s AND version = %s
            """,
            (tenant_id, agent_id, version),
        )
        row = await cur.fetchone()
        return None if row is None else _record(row)

    async def versions(self, tenant_id: str, agent_id: str) -> list[AgentVersionRecord]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM agent_versions
                WHERE tenant_id = %s AND agent_id = %s
                ORDER BY version DESC
                """,
                (tenant_id, agent_id),
            )
            return [_record(row) for row in await cur.fetchall()]

    async def get_agent(self, tenant_id: str, agent_id: str) -> AgentRecord | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_AGENT_COLUMNS} FROM agents WHERE tenant_id = %s AND agent_id = %s",
                (tenant_id, agent_id),
            )
            row = await cur.fetchone()
        return None if row is None else AgentRecord(*row)

    async def list_agents(self, tenant_id: str) -> list[AgentRecord]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_AGENT_COLUMNS} FROM agents WHERE tenant_id = %s ORDER BY agent_id",
                (tenant_id,),
            )
            return [AgentRecord(*row) for row in await cur.fetchall()]

    async def create_agent(
        self, tenant_id: str, agent_id: str, definition: dict[str, Any], created_by: str
    ) -> AgentVersionRecord:
        stored = _validated(agent_id, definition)
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%s, %s, %s)
                ON CONFLICT (tenant_id, agent_id) DO NOTHING
                RETURNING agent_id
                """,
                (tenant_id, agent_id, created_by),
            )
            if await cur.fetchone() is None:
                raise AgentStoreError(f"agent '{agent_id}' already exists")
            cur = await conn.execute(
                f"""
                INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition,
                                            created_by)
                VALUES (%s, %s, 1, 'draft', %s, %s)
                RETURNING {_COLUMNS}
                """,
                (tenant_id, agent_id, Jsonb(stored), created_by),
            )
            return _record(await cur.fetchone())

    async def create_draft(
        self, tenant_id: str, agent_id: str, created_by: str, from_version: int | None = None
    ) -> AgentVersionRecord:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await self._lock(conn, tenant_id)
            cur = await conn.execute(
                f"""
                INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition,
                                            created_by)
                SELECT %s, %s,
                       (SELECT COALESCE(MAX(version), 0) + 1 FROM agent_versions
                        WHERE tenant_id = %s AND agent_id = %s),
                       'draft', definition, %s
                FROM agent_versions
                WHERE tenant_id = %s AND agent_id = %s
                  AND CASE WHEN %s::integer IS NULL THEN status = 'published'
                           ELSE version = %s::integer END
                RETURNING {_COLUMNS}
                """,
                (
                    tenant_id,
                    agent_id,
                    tenant_id,
                    agent_id,
                    created_by,
                    tenant_id,
                    agent_id,
                    from_version,
                    from_version,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                if from_version is None:
                    raise AgentStoreError(f"agent '{agent_id}' has no published version to copy")
                raise _no_such(agent_id, from_version)
            return _record(row)

    async def replace_draft(
        self, tenant_id: str, agent_id: str, version: int, definition: dict[str, Any]
    ) -> AgentVersionRecord:
        stored = _validated(agent_id, definition)
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                UPDATE agent_versions SET definition = %s
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND status = 'draft'
                RETURNING {_COLUMNS}
                """,
                (Jsonb(stored), tenant_id, agent_id, version),
            )
            return await self._moved(conn, cur, tenant_id, agent_id, version, DRAFT)

    async def submit(
        self, tenant_id: str, agent_id: str, version: int, submitted_by: str
    ) -> AgentVersionRecord:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                UPDATE agent_versions
                SET status = 'pending', submitted_by = %s, submitted_at = now()
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND status = 'draft'
                RETURNING {_COLUMNS}
                """,
                (submitted_by, tenant_id, agent_id, version),
            )
            return await self._moved(conn, cur, tenant_id, agent_id, version, DRAFT)

    async def withdraw(self, tenant_id: str, agent_id: str, version: int) -> AgentVersionRecord:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"""
                UPDATE agent_versions SET status = 'draft'
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND status = 'pending'
                RETURNING {_COLUMNS}
                """,
                (tenant_id, agent_id, version),
            )
            return await self._moved(conn, cur, tenant_id, agent_id, version, PENDING)

    async def publish(
        self, tenant_id: str, agent_id: str, version: int, approved_by: str
    ) -> AgentVersionRecord:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await self._lock(conn, tenant_id)
            target = await self._get(conn, tenant_id, agent_id, version)
            if target is None:
                raise _no_such(agent_id, version)
            if target.status != PENDING:
                raise _wrong_status(agent_id, version, target.status, PENDING)
            cur = await conn.execute(
                f"""
                SELECT {_COLUMNS} FROM agent_versions
                WHERE tenant_id = %s AND agent_id = %s AND status = 'published'
                """,
                (tenant_id, agent_id),
            )
            current = await cur.fetchone()
            if current is not None and version < _record(current).version:
                raise _stale(agent_id, version, _record(current).version)
            check_publish(await self._published_set(conn, tenant_id), target.agent())
            await conn.execute(
                """
                UPDATE agent_versions SET status = 'superseded'
                WHERE tenant_id = %s AND agent_id = %s AND status = 'published'
                """,
                (tenant_id, agent_id),
            )
            cur = await conn.execute(
                f"""
                UPDATE agent_versions
                SET status = 'published', approved_by = %s, published_at = now()
                WHERE tenant_id = %s AND agent_id = %s AND version = %s AND status = 'pending'
                RETURNING {_COLUMNS}
                """,
                (approved_by, tenant_id, agent_id, version),
            )
            return _record(await cur.fetchone())

    async def import_agents(
        self, tenant_id: str, agents: dict[str, AgentVersion], imported_by: str = IMPORT_USER
    ) -> ImportResult:
        result = ImportResult()
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await self._lock(conn, tenant_id)
            cur = await conn.execute(
                "SELECT agent_id FROM agents WHERE tenant_id = %s", (tenant_id,)
            )
            existing = {row[0] for row in await cur.fetchall()}
            new = {aid: a for aid, a in agents.items() if aid not in existing}
            result.skipped = [aid for aid in agents if aid in existing]
            published = await self._published_set(conn, tenant_id)
            check_front_doors({**published, **new})
            for agent_id, agent in new.items():
                await conn.execute(
                    "INSERT INTO agents (tenant_id, agent_id, created_by) VALUES (%s, %s, %s)",
                    (tenant_id, agent_id, imported_by),
                )
                await conn.execute(
                    """
                    INSERT INTO agent_versions (tenant_id, agent_id, version, status, definition,
                                                created_by, approved_by, published_at)
                    VALUES (%s, %s, 1, 'published', %s, %s, %s, now())
                    """,
                    (
                        tenant_id,
                        agent_id,
                        Jsonb(agent_definition(agent)),
                        imported_by,
                        imported_by,
                    ),
                )
                result.imported.append(agent_id)
        return result

    # --- helpers -------------------------------------------------------------

    @staticmethod
    async def _lock(conn, tenant_id: str) -> None:
        """Serialise the writes that read or change a tenant's published set.

        The lock is held to the end of the transaction. An advisory lock is
        used because the app role has no UPDATE on ``agents`` for ``FOR UPDATE``.
        """
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (tenant_id,))

    async def _moved(
        self, conn, cur, tenant_id: str, agent_id: str, version: int, wanted: str
    ) -> AgentVersionRecord:
        """The row an UPDATE returned, or the reason it matched nothing."""
        row = await cur.fetchone()
        if row is not None:
            return _record(row)
        current = await self._get(conn, tenant_id, agent_id, version)
        if current is None:
            raise _no_such(agent_id, version)
        raise _wrong_status(agent_id, version, current.status, wanted)


# --- In memory ----------------------------------------------------------------


class InMemoryAgentStore:
    """The same lifecycle in memory, for tests. It mirrors the database trigger."""

    def __init__(self) -> None:
        self._agents: dict[tuple[str, str], list[AgentVersionRecord]] = {}

    @classmethod
    def seeded(cls, *configs: AgentsConfig, by: str = IMPORT_USER) -> InMemoryAgentStore:
        """A store with every agent of each parsed config published at its own version number."""
        store = cls()
        for config in configs:
            now = datetime.now(UTC)
            for agent in config.agents.values():
                store._agents[(config.tenant_id, agent.agent_id)] = [
                    AgentVersionRecord(
                        tenant_id=config.tenant_id,
                        agent_id=agent.agent_id,
                        version=agent.version,
                        status=PUBLISHED,
                        definition=agent_definition(agent),
                        created_by=by,
                        created_at=now,
                        approved_by=by,
                        published_at=now,
                    )
                ]
        return store

    # Test hooks. They skip the lifecycle and the front-door check on purpose.

    def set_published_unchecked(
        self, tenant_id: str, agent_id: str, definition: dict[str, Any] | None
    ) -> None:
        """Replace an agent's published definition, or remove its published version with None."""
        rows = self._agents.setdefault((tenant_id, agent_id), [])
        for i, row in enumerate(rows):
            if row.status == PUBLISHED:
                if definition is None:
                    rows[i] = replace(row, status=SUPERSEDED)
                else:
                    rows[i] = replace(row, definition=_without_version(definition))
                return
        if definition is not None:
            now = datetime.now(UTC)
            rows.append(
                AgentVersionRecord(
                    tenant_id,
                    agent_id,
                    max((r.version for r in rows), default=0) + 1,
                    PUBLISHED,
                    _without_version(definition),
                    IMPORT_USER,
                    now,
                    approved_by=IMPORT_USER,
                    published_at=now,
                )
            )

    # reads

    def _rows(self, tenant_id: str, agent_id: str) -> list[AgentVersionRecord]:
        return self._agents.get((tenant_id, agent_id), [])

    def _find(self, tenant_id: str, agent_id: str, version: int) -> AgentVersionRecord:
        for row in self._rows(tenant_id, agent_id):
            if row.version == version:
                return row
        raise _no_such(agent_id, version)

    def _set(self, row: AgentVersionRecord, **changes: Any) -> AgentVersionRecord:
        """Apply a change as the database trigger would allow it."""
        if row.status in (PUBLISHED, SUPERSEDED):
            only_status = set(changes) == {"status"}
            if not (only_status and (row.status, changes["status"]) in ALLOWED_MOVES):
                raise AgentStoreError(f"a {row.status} version is immutable")
        if "status" in changes and (row.status, changes["status"]) not in ALLOWED_MOVES:
            raise AgentStoreError(f"{row.status} -> {changes['status']} is not allowed")
        if "definition" in changes and row.status != DRAFT:
            raise AgentStoreError("the definition changes only while a version is a draft")
        new = replace(row, **changes)
        rows = self._agents[(row.tenant_id, row.agent_id)]
        rows[rows.index(row)] = new
        return new

    async def published(self, tenant_id: str, agent_id: str) -> AgentVersion | None:
        for row in self._rows(tenant_id, agent_id):
            if row.status == PUBLISHED:
                return row.agent()
        return None

    async def published_all(self, tenant_id: str) -> dict[str, AgentVersion]:
        found = {}
        for (tenant, agent_id), rows in sorted(self._agents.items()):
            if tenant == tenant_id:
                for row in rows:
                    if row.status == PUBLISHED:
                        found[agent_id] = row.agent()
        return found

    async def get_version(
        self, tenant_id: str, agent_id: str, version: int
    ) -> AgentVersionRecord | None:
        try:
            return self._find(tenant_id, agent_id, version)
        except AgentStoreError:
            return None

    async def versions(self, tenant_id: str, agent_id: str) -> list[AgentVersionRecord]:
        return sorted(self._rows(tenant_id, agent_id), key=lambda r: -r.version)

    async def get_agent(self, tenant_id: str, agent_id: str) -> AgentRecord | None:
        rows = self._rows(tenant_id, agent_id)
        if not rows:
            return None
        # The agent's creator is the creator of its first version, as in the database.
        first = min(rows, key=lambda r: r.version)
        return AgentRecord(tenant_id, agent_id, first.created_by, first.created_at)

    async def list_agents(self, tenant_id: str) -> list[AgentRecord]:
        found = [await self.get_agent(t, a) for t, a in sorted(self._agents) if t == tenant_id]
        return [record for record in found if record is not None]

    # writes

    def _add(
        self, tenant_id: str, agent_id: str, definition: dict[str, Any], created_by: str
    ) -> AgentVersionRecord:
        rows = self._agents[(tenant_id, agent_id)]
        row = AgentVersionRecord(
            tenant_id,
            agent_id,
            max((r.version for r in rows), default=0) + 1,
            DRAFT,
            _without_version(definition),
            created_by,
            datetime.now(UTC),
        )
        rows.append(row)
        return row

    async def create_agent(
        self, tenant_id: str, agent_id: str, definition: dict[str, Any], created_by: str
    ) -> AgentVersionRecord:
        stored = _validated(agent_id, definition)
        if (tenant_id, agent_id) in self._agents:
            raise AgentStoreError(f"agent '{agent_id}' already exists")
        self._agents[(tenant_id, agent_id)] = []
        return self._add(tenant_id, agent_id, stored, created_by)

    async def create_draft(
        self, tenant_id: str, agent_id: str, created_by: str, from_version: int | None = None
    ) -> AgentVersionRecord:
        if from_version is not None:
            source = self._find(tenant_id, agent_id, from_version)
        else:
            published = [r for r in self._rows(tenant_id, agent_id) if r.status == PUBLISHED]
            if not published:
                raise AgentStoreError(f"agent '{agent_id}' has no published version to copy")
            source = published[0]
        return self._add(tenant_id, agent_id, source.definition, created_by)

    async def replace_draft(
        self, tenant_id: str, agent_id: str, version: int, definition: dict[str, Any]
    ) -> AgentVersionRecord:
        stored = _validated(agent_id, definition)
        row = self._find(tenant_id, agent_id, version)
        if row.status != DRAFT:
            raise _wrong_status(agent_id, version, row.status, DRAFT)
        return self._set(row, definition=stored)

    async def submit(
        self, tenant_id: str, agent_id: str, version: int, submitted_by: str
    ) -> AgentVersionRecord:
        row = self._find(tenant_id, agent_id, version)
        if row.status != DRAFT:
            raise _wrong_status(agent_id, version, row.status, DRAFT)
        return self._set(
            row, status=PENDING, submitted_by=submitted_by, submitted_at=datetime.now(UTC)
        )

    async def withdraw(self, tenant_id: str, agent_id: str, version: int) -> AgentVersionRecord:
        row = self._find(tenant_id, agent_id, version)
        if row.status != PENDING:
            raise _wrong_status(agent_id, version, row.status, PENDING)
        return self._set(row, status=DRAFT)

    async def publish(
        self, tenant_id: str, agent_id: str, version: int, approved_by: str
    ) -> AgentVersionRecord:
        row = self._find(tenant_id, agent_id, version)
        if row.status != PENDING:
            raise _wrong_status(agent_id, version, row.status, PENDING)
        for other in self._rows(tenant_id, agent_id):
            if other.status == PUBLISHED and row.version < other.version:
                raise _stale(agent_id, row.version, other.version)
        check_publish(await self.published_all(tenant_id), row.agent())
        for other in self._rows(tenant_id, agent_id):
            if other.status == PUBLISHED:
                self._set(other, status=SUPERSEDED)
        return self._set(
            self._find(tenant_id, agent_id, version),
            status=PUBLISHED,
            approved_by=approved_by,
            published_at=datetime.now(UTC),
        )

    async def import_agents(
        self, tenant_id: str, agents: dict[str, AgentVersion], imported_by: str = IMPORT_USER
    ) -> ImportResult:
        result = ImportResult()
        new = {aid: a for aid, a in agents.items() if (tenant_id, aid) not in self._agents}
        result.skipped = [aid for aid in agents if aid not in new]
        check_front_doors({**await self.published_all(tenant_id), **new})
        now = datetime.now(UTC)
        for agent_id, agent in new.items():
            self._agents[(tenant_id, agent_id)] = [
                AgentVersionRecord(
                    tenant_id,
                    agent_id,
                    1,
                    PUBLISHED,
                    agent_definition(agent),
                    imported_by,
                    now,
                    approved_by=imported_by,
                    published_at=now,
                )
            ]
            result.imported.append(agent_id)
        return result


def seeded_agent_store(*configs: AgentsConfig) -> InMemoryAgentStore:
    """The in-memory store the tests use: every agent of the configs, published."""
    return InMemoryAgentStore.seeded(*configs)


# --- What the runtime needs at startup ---------------------------------------


async def with_published_agents(store: AgentStore, config: AgentsConfig) -> AgentsConfig:
    """The tenant's config with ``agents`` replaced by its published set.

    The tenant's settings stay as the YAML file has them. Code that walks a
    tenant's agents (grant checks, index schedules, the embedder check) reads
    this view, so none of it reads the YAML ``agents`` section.
    """
    return replace(config, agents=await store.published_all(config.tenant_id))


async def published_tenants(store: AgentStore, tenants: TenantsConfig) -> TenantsConfig:
    """Every tenant's config with its published agents (see ``with_published_agents``)."""
    return TenantsConfig(
        {tid: await with_published_agents(store, config) for tid, config in tenants.tenants.items()}
    )


async def startup_agent_failure(store: AgentStore, tenants: TenantsConfig) -> str | None:
    """A message when a tenant's default agent or a channel's agent has no published version.

    ``default_agent`` and ``channels`` stay in the YAML file and name agents,
    so a process that would route a message to an unpublished agent must not
    start. The message names the tenant and the agent.
    """
    problems = []
    for tenant_id, config in tenants.tenants.items():
        published = await store.published_all(tenant_id)
        named = [("default_agent", config.default_agent)]
        named += [(f"channel '{channel}'", agent) for channel, agent in config.channels.items()]
        for label, agent_id in named:
            if agent_id not in published:
                problems.append(
                    f"tenant '{tenant_id}': {label} names agent '{agent_id}', which has no "
                    "published version. Run `uv run python -m agentos.agent_store import` "
                    "or publish it."
                )
    return "\n".join(problems) or None


# --- One-time import ----------------------------------------------------------


async def run_import() -> int:
    from agentos.config import load_settings, load_tenants_config

    settings = load_settings()
    tenants = load_tenants_config(settings.config_path)
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    failed = False
    try:
        store = PostgresAgentStore(pool)
        for tenant_id, config in tenants.tenants.items():
            try:
                result = await store.import_agents(tenant_id, config.agents)
            except ConfigError as err:
                print(f"tenant '{tenant_id}': import refused: {err}")
                failed = True
                continue
            for agent_id in result.imported:
                print(f"tenant '{tenant_id}': imported agent '{agent_id}' as version 1")
            for agent_id in result.skipped:
                print(f"tenant '{tenant_id}': skipped agent '{agent_id}', already in the database")
    finally:
        await pool.close()
    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the agents stored in the database.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "import",
        help="insert each YAML agent that is not in the database yet as a published version 1",
    )
    parser.parse_args()
    sys.exit(asyncio.run(run_import()))


if __name__ == "__main__":
    main()
