"""Kill switches: an admin turns an agent, every agent, a connector or all writes off.

A switch is one row in ``kill_switches``; a row present means off, and turning it on
deletes the row (the audit log keeps the history). There are four scopes:

* ``agent``: one agent answers every turn with a fixed text and calls no model.
* ``all_agents``: the same for every agent of the tenant.
* ``connector``: one connector (Notion) is unavailable to the tenant. Its tool calls
  return an error the model passes on, knowledge search refuses, indexing and
  follow-up polling do nothing until it is back on.
* ``writes``: read-only mode. Drafting still works; Submit is refused, and a write that
  was approved but has not run yet is refused too, so nothing is changed.

Every check reads the table again, with no cache, so "off" takes effect on the next step
without a restart. The checks live only in activities and the gateway, never in workflow
code. A switch that cannot be read raises, so the step fails and Temporal retries it; an
unreadable switch never lets a step through unchecked.

A switch holds IDs and names, never chat or document text. The host operator can use the
same switches with the console down::

    uv run python -m agentos.switches --tenant acme list
    uv run python -m agentos.switches --tenant acme off --agent it-helper
    uv run python -m agentos.switches --tenant acme off --everything
    uv run python -m agentos.switches --tenant acme on --writes

The command is audited as ``operator:<login of the OS account running it>``, a value the
runtime reads and nobody types.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from psycopg_pool import AsyncConnectionPool

from agentos import ids
from agentos.agent_store import AgentNotFound, AgentStore
from agentos.audit import AuditStore
from agentos.models import AuditEvent
from agentos.stores import tenant_transaction
from agentos.usage import utcnow

log = logging.getLogger("agentos.switches")

AGENT = "agent"
ALL_AGENTS = "all_agents"
CONNECTOR = "connector"
WRITES = "writes"
SCOPES = (AGENT, ALL_AGENTS, CONNECTOR, WRITES)

NOTION = "notion"
CONNECTORS = (NOTION,)

SWITCHED_OFF = "switched_off"


def agent_off_text(agent_name: str) -> str:
    return f"{agent_name} is turned off by an admin."


WRITES_OFF_TEXT = "Writes are turned off by your admin; nothing was changed."
# What a task tells its requester when the write was approved but writes went off before it ran.
WRITE_REFUSED_TEXT = f"{WRITES_OFF_TEXT} Ask again once they are turned back on."


def connector_off_text(connector: str) -> str:
    return f"{connector.capitalize()} is turned off by an admin, so this can't be done now."


class SwitchError(ValueError):
    """The change names a switch that does not exist."""


class InvalidSwitch(SwitchError):
    """The scope and target do not name a switch."""


class WritesOff(Exception):
    """Writes are turned off for the tenant."""


class ConnectorOff(Exception):
    """A connector is turned off for the tenant."""


# --- rows -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Switch:
    tenant_id: str
    scope: str
    target: str  # an agent ID or a connector name; empty for the tenant-wide scopes
    turned_off_by: str
    turned_off_at: datetime | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "scope": self.scope,
            "target": self.target or None,
            "turned_off_by": self.turned_off_by,
            "turned_off_at": None if self.turned_off_at is None else self.turned_off_at.isoformat(),
        }


def check_switch(scope: str, target: str | None) -> str:
    """The stored target of a switch, or ``InvalidSwitch`` when the pair names none."""
    text = (target or "").strip()
    if scope not in SCOPES:
        raise InvalidSwitch(f"unknown switch '{scope}'; one of {', '.join(SCOPES)}")
    if scope == AGENT:
        if not text:
            raise InvalidSwitch("an agent switch names an agent")
    elif scope == CONNECTOR:
        if text not in CONNECTORS:
            raise InvalidSwitch(f"unknown connector '{text}'; one of {', '.join(CONNECTORS)}")
    elif text:
        raise InvalidSwitch(f"the '{scope}' switch takes no target")
    return text


@dataclass(frozen=True)
class SwitchState:
    """Every switch that is off for one tenant, as read just now."""

    tenant_id: str
    rows: tuple[Switch, ...] = ()

    def _has(self, scope: str, target: str = "") -> bool:
        return any(r.scope == scope and r.target == target for r in self.rows)

    def agent_stopped_by(self, agent_id: str) -> str | None:
        """``"agent"`` or ``"all_agents"`` when that switch stops the agent, else None."""
        if self._has(AGENT, agent_id):
            return AGENT
        if self._has(ALL_AGENTS):
            return ALL_AGENTS
        return None

    @property
    def writes_off(self) -> bool:
        return self._has(WRITES)

    def connector_off(self, connector: str) -> bool:
        return self._has(CONNECTOR, connector)

    def of(self, scope: str, target: str = "") -> Switch | None:
        return next((r for r in self.rows if r.scope == scope and r.target == target), None)


class SwitchStore(Protocol):
    async def list(self, tenant_id: str) -> list[Switch]:
        """Every switch of the tenant that is off."""
        ...

    async def switch_off(
        self, tenant_id: str, scope: str, target: str, by: str, at: datetime
    ) -> bool:
        """Turn a switch off. False when it was already off (nothing changes)."""
        ...

    async def switch_on(self, tenant_id: str, scope: str, target: str) -> bool:
        """Turn a switch back on. False when it was not off."""
        ...


class InMemorySwitchStore:
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str], Switch] = {}

    async def list(self, tenant_id: str) -> list[Switch]:
        return sorted(
            (s for (t, _, _), s in self._rows.items() if t == tenant_id),
            key=lambda s: (SCOPES.index(s.scope), s.target),
        )

    async def switch_off(
        self, tenant_id: str, scope: str, target: str, by: str, at: datetime
    ) -> bool:
        key = (tenant_id, scope, target)
        if key in self._rows:
            return False
        self._rows[key] = Switch(tenant_id, scope, target, by, at)
        return True

    async def switch_on(self, tenant_id: str, scope: str, target: str) -> bool:
        return self._rows.pop((tenant_id, scope, target), None) is not None


_COLUMNS = "tenant_id, scope, target, turned_off_by, turned_off_at"


class PostgresSwitchStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def list(self, tenant_id: str) -> list[Switch]:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                f"SELECT {_COLUMNS} FROM kill_switches WHERE tenant_id = %s ORDER BY scope, target",
                (tenant_id,),
            )
            return [Switch(*row) for row in await cur.fetchall()]

    async def switch_off(
        self, tenant_id: str, scope: str, target: str, by: str, at: datetime
    ) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "INSERT INTO kill_switches (tenant_id, scope, target, turned_off_by, turned_off_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (tenant_id, scope, target) DO NOTHING RETURNING tenant_id",
                (tenant_id, scope, target, by, at),
            )
            return await cur.fetchone() is not None

    async def switch_on(self, tenant_id: str, scope: str, target: str) -> bool:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                "DELETE FROM kill_switches "
                "WHERE tenant_id = %s AND scope = %s AND target = %s RETURNING tenant_id",
                (tenant_id, scope, target),
            )
            return await cur.fetchone() is not None


@dataclass
class SwitchLine:
    """One switch and whether it is off. ``target`` is empty for the tenant-wide scopes."""

    scope: str
    target: str
    switch: Switch | None  # None when it is on

    @property
    def off(self) -> bool:
        return self.switch is not None

    def as_dict(self) -> dict:
        return {"scope": self.scope, "target": self.target or None, "off": self.off} | (
            {} if self.switch is None else self.switch.as_dict()
        )


@dataclass
class SwitchesView:
    """What the console and API show: every switch with its state.

    ``agents`` lists the agents the reader may see, each with the switch that stops it
    (its own, or the tenant-wide one), so a page can say why an agent is not answering.
    """

    all_agents: SwitchLine
    writes: SwitchLine
    connectors: list[SwitchLine]
    agents: list[SwitchLine]
    stopped_by: dict[str, str]  # agent ID -> "agent" or "all_agents", for agents that are off

    @classmethod
    def build(cls, state: SwitchState, agent_ids: list[str]) -> SwitchesView:
        return cls(
            all_agents=SwitchLine(ALL_AGENTS, "", state.of(ALL_AGENTS)),
            writes=SwitchLine(WRITES, "", state.of(WRITES)),
            connectors=[SwitchLine(CONNECTOR, c, state.of(CONNECTOR, c)) for c in CONNECTORS],
            agents=[SwitchLine(AGENT, a, state.of(AGENT, a)) for a in agent_ids],
            stopped_by={a: by for a in agent_ids if (by := state.agent_stopped_by(a)) is not None},
        )

    def line_of(self, agent_id: str) -> SwitchLine | None:
        return next((x for x in self.agents if x.target == agent_id), None)

    def as_dict(self) -> dict:
        return {
            "all_agents": self.all_agents.as_dict(),
            "writes": self.writes.as_dict(),
            "connectors": [c.as_dict() for c in self.connectors],
            "agents": [a.as_dict() for a in self.agents],
        }


async def read_state(store: SwitchStore, tenant_id: str) -> SwitchState:
    """The tenant's switches, read from the store now. One indexed query, no cache."""
    return SwitchState(tenant_id, tuple(await store.list(tenant_id)))


def log_stop(tenant_id: str, agent_id: str, switch: str) -> None:
    """One line for a turn stopped by a switch: IDs and the switch, never content."""
    log.info(
        "turn stopped by switch tenant_id=%s agent_id=%s switch=%s", tenant_id, agent_id, switch
    )


# --- changing a switch, for the console and the command line ----------------------------


async def set_switch(
    store: SwitchStore,
    audit: AuditStore,
    agents: AgentStore,
    tenant_id: str,
    scope: str,
    target: str | None,
    *,
    off: bool,
    actor: str,
    identity: str,
    at: datetime | None = None,
    request_id: str | None = None,
) -> bool:
    """Turn a switch off or on and audit the change. True when something changed.

    ``actor`` and ``identity`` come from the caller's verified identity: the control
    plane's actor, or the operator of the command line. Turning off what is off, or on
    what is on, changes nothing and records nothing. The event is recorded after the
    change, as the control plane does for every change.
    """
    stored = check_switch(scope, target)
    if scope == AGENT and await agents.get_agent(tenant_id, stored) is None:
        raise AgentNotFound(f"agent '{stored}' does not exist")
    if off:
        changed = await store.switch_off(tenant_id, scope, stored, actor, at or utcnow())
    else:
        changed = await store.switch_on(tenant_id, scope, stored)
    if changed:
        kind = "switch_off" if off else "switch_on"
        await audit.record(
            AuditEvent(
                tenant_id=tenant_id,
                event_key=ids.audit_control_key(tenant_id, kind, request_id or uuid.uuid4().hex),
                kind=kind,
                agent_id=stored if scope == AGENT else None,
                agent_version=None,
                requester_user_id=actor,
                identity=identity,
                tool=scope,
                target=stored or None,
            )
        )
        log.info(
            "switch %s tenant_id=%s scope=%s target=%s",
            "off" if off else "on",
            tenant_id,
            scope,
            stored or "-",
        )
    return changed


# --- the command line -----------------------------------------------------------------


def operator_identity() -> str:
    """Who runs the command: the login of the OS account, read by the runtime."""
    return f"operator:{getpass.getuser()}"


def _targets(args: argparse.Namespace) -> list[tuple[str, str | None]]:
    if args.everything:
        return [(ALL_AGENTS, None), (WRITES, None)] + [(CONNECTOR, c) for c in CONNECTORS]
    if args.agent:
        return [(AGENT, args.agent)]
    if args.all_agents:
        return [(ALL_AGENTS, None)]
    if args.connector:
        return [(CONNECTOR, args.connector)]
    return [(WRITES, None)]


async def run(args: argparse.Namespace) -> int:
    from agentos.agent_store import PostgresAgentStore
    from agentos.audit import PostgresAuditStore
    from agentos.config import load_settings
    from agentos.tenant_store import load_stored_tenants

    settings = load_settings()
    tenants = await load_stored_tenants(settings.database_url)
    if args.tenant not in tenants.tenants:
        print(f"unknown tenant '{args.tenant}'; configured: {', '.join(tenants.tenants)}")
        return 2
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        store = PostgresSwitchStore(pool)
        if args.command == "list":
            rows = await store.list(args.tenant)
            for row in rows:
                print(f"off: {row.scope} {row.target or '-'} (by {row.turned_off_by})")
            if not rows:
                print("nothing is turned off")
            return 0
        identity = operator_identity()
        for scope, target in _targets(args):
            changed = await set_switch(
                store,
                PostgresAuditStore(pool),
                PostgresAgentStore(pool),
                args.tenant,
                scope,
                target,
                off=args.command == "off",
                actor=identity,
                identity=identity,
            )
            word = "turned " + args.command if changed else f"already {args.command}"
            print(f"{scope} {target or '-'}: {word}")
    except (AgentNotFound, SwitchError) as err:
        print(f"refused: {err}")
        return 1
    finally:
        await pool.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Turn agents, connectors or writes off and on without the console."
    )
    parser.add_argument("--tenant", required=True, help="the tenant to change")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="show what is turned off")
    for name, text in (("off", "turn a switch off"), ("on", "turn a switch back on")):
        sub = commands.add_parser(name, help=text)
        group = sub.add_mutually_exclusive_group(required=True)
        group.add_argument("--agent", help="one agent, by ID")
        group.add_argument("--all-agents", action="store_true", help="every agent of the tenant")
        group.add_argument("--connector", choices=CONNECTORS, help="one connector")
        group.add_argument("--writes", action="store_true", help="every write (read-only mode)")
        group.add_argument(
            "--everything", action="store_true", help="all agents, writes and every connector"
        )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    sys.exit(asyncio.run(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
