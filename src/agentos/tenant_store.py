"""Tenants in the database, and the live view of them every process serves from.

The ``tenants`` table is the source of truth for tenant settings: the Slack workspace,
whether the tenant is enabled, the default agent, channel routing, admins and the
*names* of the environment variables that hold its secrets. A tenant enters through
the one-time import (``uv run python -m agentos.tenant_store import``) or through the
Slack OAuth install (``agentos.slack_install``). A secret value is never stored here;
connector credentials live encrypted in ``tenant_credentials``. Agents live in the
agents table.

Rules kept here:

* Row-level security hides every tenant's row from the others. The two reads that
  must cross tenants (a Slack workspace to its tenant, and the list of tenants) go
  through narrow database functions that return IDs, the enabled flag and a change
  time only. A tenant's full settings are read inside that tenant's own transaction.
* A ``TenantDirectory`` serves the enabled tenants from a short cache, so a tenant
  added, changed or disabled in the database reaches the gateway, the worker and the
  console within ``DEFAULT_TTL_SECONDS`` without a restart. A disabled tenant is not
  in what it returns, so every caller treats it as unknown.
* A tenant's settings are never copied back from the YAML file once imported.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Protocol

from psycopg.errors import UniqueViolation
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from agentos.config import (
    TENANT_ID_PATTERN,
    AgentsConfig,
    ConfigError,
    TenantsConfig,
    TenantSecrets,
    load_settings,
)
from agentos.stores import tenant_transaction

log = logging.getLogger("agentos.tenants")

# How long a process serves from what it last read. A change in the database reaches
# every process within this long; a tenant first seen in a Slack event is read at once.
DEFAULT_TTL_SECONDS = 15.0
# A forced re-read (an unknown workspace or tenant ID) happens at most this often, so
# a burst of unknown requests costs one read, not one each.
FORCED_REFRESH_GAP_SECONDS = 1.0

# A tenant ID found to be unknown or disabled is answered from memory for this long, so a
# caller who keeps asking for one costs no database read. The set is capped so that
# many different made-up IDs cannot grow it without bound.
UNKNOWN_TENANT_TTL_SECONDS = 10.0
MAX_UNKNOWN_TENANTS = 1024

IMPORT_COMMAND = "uv run python -m agentos.tenant_store import"


@dataclass(frozen=True)
class TenantRef:
    """What the narrow functions return: no settings, only enough to route and to cache."""

    tenant_id: str
    enabled: bool
    updated_at: datetime


@dataclass(frozen=True)
class TenantRow:
    """One tenant's stored settings. ``config.agents`` is empty: agents are in their own table."""

    config: AgentsConfig
    enabled: bool
    updated_at: datetime


class TenantStore(Protocol):
    async def ref_for_slack_team(self, team_id: str) -> TenantRef | None:
        """The tenant of a Slack workspace. Needs no tenant context."""
        ...

    async def list_refs(self) -> list[TenantRef]:
        """Every tenant, enabled or not. Needs no tenant context."""
        ...

    async def load(self, tenant_id: str) -> TenantRow | None:
        """One tenant's settings, read as that tenant."""
        ...

    async def insert(self, config: AgentsConfig) -> bool:
        """Store a tenant that is not stored yet. False, and nothing changes, if it is."""
        ...

    async def set_enabled(self, tenant_id: str, enabled: bool) -> None: ...


def _secret_env(config: AgentsConfig) -> dict[str, str]:
    return config.secrets.names()


def _config_from_row(
    tenant_id: str,
    slack_team_id: str,
    default_agent: str,
    notion_workspace_id: str | None,
    channels: dict,
    admins: list,
    secret_env: dict,
) -> AgentsConfig:
    known = TenantSecrets().names()
    unknown = sorted(set(secret_env) - set(known))
    if unknown:
        raise ConfigError(
            f"tenant '{tenant_id}': stored secret field(s) {', '.join(unknown)} are not known"
        )
    return AgentsConfig(
        tenant_id=tenant_id,
        slack_team_id=slack_team_id,
        agents={},
        default_agent=default_agent,
        notion_workspace_id=notion_workspace_id,
        secrets=TenantSecrets(**secret_env),
        channels={str(k): str(v) for k, v in channels.items()},
        admins=tuple(str(a) for a in admins),
    )


# --- Postgres -----------------------------------------------------------------


class PostgresTenantStore:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool

    async def ref_for_slack_team(self, team_id: str) -> TenantRef | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT tenant_id, enabled, updated_at FROM tenant_for_slack_team(%s)",
                (team_id,),
            )
            row = await cur.fetchone()
        return None if row is None else TenantRef(*row)

    async def list_refs(self) -> list[TenantRef]:
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT tenant_id, enabled, updated_at FROM list_tenants()")
            return [TenantRef(*row) for row in await cur.fetchall()]

    async def load(self, tenant_id: str) -> TenantRow | None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            cur = await conn.execute(
                """
                SELECT tenant_id, slack_team_id, default_agent, notion_workspace_id,
                       channels, admins, secret_env, enabled, updated_at
                FROM tenants WHERE tenant_id = %s
                """,
                (tenant_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return TenantRow(_config_from_row(*row[:7]), row[7], row[8])

    async def insert(self, config: AgentsConfig) -> bool:
        try:
            async with tenant_transaction(self._pool, config.tenant_id) as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO tenants (tenant_id, slack_team_id, default_agent,
                                         notion_workspace_id, channels, admins, secret_env)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id) DO NOTHING
                    RETURNING tenant_id
                    """,
                    (
                        config.tenant_id,
                        config.slack_team_id,
                        config.default_agent,
                        config.notion_workspace_id,
                        Jsonb(config.channels),
                        Jsonb(list(config.admins)),
                        Jsonb(_secret_env(config)),
                    ),
                )
                return await cur.fetchone() is not None
        except UniqueViolation as err:
            raise ConfigError(
                f"tenant '{config.tenant_id}': another tenant already uses its Slack workspace "
                "or Notion workspace"
            ) from err

    async def set_enabled(self, tenant_id: str, enabled: bool) -> None:
        async with tenant_transaction(self._pool, tenant_id) as conn:
            await conn.execute(
                "UPDATE tenants SET enabled = %s, updated_at = now() WHERE tenant_id = %s",
                (enabled, tenant_id),
            )


# --- In memory (tests) --------------------------------------------------------


class InMemoryTenantStore:
    """The same behaviour as Postgres, minus the database: one row per tenant ID."""

    def __init__(self, *configs: AgentsConfig):
        self._rows: dict[str, TenantRow] = {}
        self._tick = datetime(2026, 1, 1, tzinfo=UTC)
        for config in configs:
            self._put(config, True)

    def snapshot(self) -> tuple[dict[str, TenantRow], datetime]:
        """The rows as they are now, for ``InMemoryUnitOfWork``."""
        return dict(self._rows), self._tick

    def restore(self, state: tuple[dict[str, TenantRow], datetime]) -> None:
        rows, tick = state
        self._rows, self._tick = dict(rows), tick

    def _put(self, config: AgentsConfig, enabled: bool) -> None:
        self._tick += timedelta(microseconds=1)
        self._rows[config.tenant_id] = TenantRow(replace(config, agents={}), enabled, self._tick)

    async def ref_for_slack_team(self, team_id: str) -> TenantRef | None:
        for row in self._rows.values():
            if row.config.slack_team_id == team_id:
                return TenantRef(row.config.tenant_id, row.enabled, row.updated_at)
        return None

    async def list_refs(self) -> list[TenantRef]:
        return [
            TenantRef(row.config.tenant_id, row.enabled, row.updated_at)
            for _, row in sorted(self._rows.items())
        ]

    async def load(self, tenant_id: str) -> TenantRow | None:
        return self._rows.get(tenant_id)

    async def insert(self, config: AgentsConfig) -> bool:
        if config.tenant_id in self._rows:
            return False
        for row in self._rows.values():
            if row.config.slack_team_id == config.slack_team_id or (
                config.notion_workspace_id
                and row.config.notion_workspace_id == config.notion_workspace_id
            ):
                raise ConfigError(
                    f"tenant '{config.tenant_id}': another tenant already uses its Slack "
                    "workspace or Notion workspace"
                )
        self._put(config, True)
        return True

    async def set_enabled(self, tenant_id: str, enabled: bool) -> None:
        self._put(self._rows[tenant_id].config, enabled)

    def replace_config(self, config: AgentsConfig) -> None:
        """Test hook: change a stored tenant's settings, as an admin tool will."""
        self._put(config, self._rows[config.tenant_id].enabled)


# --- The live view ------------------------------------------------------------


class TenantSource(Protocol):
    """What the gateway, worker and console ask for the tenants they serve.

    Only enabled tenants are visible: a disabled tenant is answered as unknown.
    """

    async def snapshot(self) -> TenantsConfig: ...

    async def for_slack_team(self, team_id: str | None) -> str | None: ...

    async def get(self, tenant_id: str) -> AgentsConfig | None: ...

    async def only_tenant(self) -> str | None: ...

    def latest(self) -> TenantsConfig:
        """The tenants as last read, with no read of its own. For sync code that runs
        right after one of the calls above."""
        ...


class StaticTenants:
    """A fixed set of tenants, for tests and for code that builds one set by hand."""

    def __init__(self, tenants: TenantsConfig):
        self._tenants = tenants

    async def snapshot(self) -> TenantsConfig:
        return self._tenants

    async def for_slack_team(self, team_id: str | None) -> str | None:
        return self._tenants.by_slack_team(team_id)

    async def get(self, tenant_id: str) -> AgentsConfig | None:
        return self._tenants.get(tenant_id)

    async def only_tenant(self) -> str | None:
        return self._tenants.only_tenant()

    def latest(self) -> TenantsConfig:
        return self._tenants


def as_source(tenants: TenantsConfig | TenantSource | None) -> TenantSource | None:
    """A ``TenantsConfig`` becomes a ``StaticTenants``; a live source and None pass through."""
    return StaticTenants(tenants) if isinstance(tenants, TenantsConfig) else tenants


class TenantDirectory:
    """Enabled tenants read from a ``TenantStore`` through a short cache.

    A tenant's settings are re-read only when its change time moved, so a refresh is
    one cheap list plus a read per changed tenant. If a refresh fails (two tenants
    now share a secret variable, or the database is down) the last good set keeps
    being served and the failure is logged; the first refresh has nothing to fall
    back on and raises.
    """

    def __init__(
        self,
        store: TenantStore,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._store = store
        self._ttl = ttl_seconds
        self._clock = clock
        self._rows: dict[str, TenantRow] = {}
        self._snapshot: TenantsConfig | None = None
        self._refreshed_at: float | None = None
        self._attempted_at: float | None = None
        self._lock = asyncio.Lock()
        self._unknown: dict[str, float] = {}
        self.stored_count = 0

    def latest(self) -> TenantsConfig:
        if self._snapshot is None:
            raise RuntimeError("the tenant directory has not been read yet")
        return self._snapshot

    async def refresh(self) -> TenantsConfig:
        """Read the tenants now. Raises ``ConfigError`` if the stored set is inconsistent."""
        async with self._lock:
            self._attempted_at = self._clock()
            refs = await self._store.list_refs()
            rows: dict[str, TenantRow] = {}
            for ref in refs:
                cached = self._rows.get(ref.tenant_id)
                if cached is not None and cached.updated_at == ref.updated_at:
                    rows[ref.tenant_id] = cached
                    continue
                row = await self._store.load(ref.tenant_id)
                if row is not None:
                    rows[ref.tenant_id] = row
            snapshot = TenantsConfig.of(*(r.config for r in rows.values() if r.enabled))
            self._rows, self._snapshot, self.stored_count = rows, snapshot, len(rows)
            self._refreshed_at = self._clock()
            return snapshot

    async def _refresh_or_keep(self) -> TenantsConfig:
        try:
            return await self.refresh()
        except Exception as err:
            if self._snapshot is None:
                raise
            # Serve the last good set; try again after one more interval.
            self._refreshed_at = self._clock()
            log.error("tenant refresh failed, serving the previous set: %s", err)
            return self._snapshot

    async def snapshot(self) -> TenantsConfig:
        fresh = self._refreshed_at is not None and self._clock() - self._refreshed_at < self._ttl
        if fresh and self._snapshot is not None:
            return self._snapshot
        return await self._refresh_or_keep()

    def _may_force(self) -> bool:
        return (
            self._attempted_at is None
            or self._clock() - self._attempted_at >= FORCED_REFRESH_GAP_SECONDS
        )

    async def for_slack_team(self, team_id: str | None) -> str | None:
        if not team_id:
            return None
        tenant_id = (await self.snapshot()).by_slack_team(team_id)
        if tenant_id is not None:
            return tenant_id
        # Not in the cache: a tenant added since the last read is served at once.
        ref = await self._store.ref_for_slack_team(team_id)
        if ref is None or not ref.enabled:
            return None
        if self._may_force():
            await self._refresh_or_keep()
        return self.latest().by_slack_team(team_id)

    async def get(self, tenant_id: str) -> AgentsConfig | None:
        config = (await self.snapshot()).get(tenant_id)
        if config is not None or not TENANT_ID_PATTERN.fullmatch(tenant_id):
            return config
        if self._unknown.get(tenant_id, 0.0) > self._clock():
            return None
        ref = next((r for r in await self._store.list_refs() if r.tenant_id == tenant_id), None)
        if ref is None or not ref.enabled:
            self._remember_unknown(tenant_id)
            return None
        if self._may_force():
            await self._refresh_or_keep()
        return self.latest().get(tenant_id)

    def _remember_unknown(self, tenant_id: str) -> None:
        now = self._clock()
        if len(self._unknown) >= MAX_UNKNOWN_TENANTS:
            self._unknown = {t: until for t, until in self._unknown.items() if until > now}
        if len(self._unknown) < MAX_UNKNOWN_TENANTS:
            self._unknown[tenant_id] = now + UNKNOWN_TENANT_TTL_SECONDS

    async def only_tenant(self) -> str | None:
        return (await self.snapshot()).only_tenant()


async def startup_tenants(directory: TenantDirectory) -> TenantsConfig:
    """The enabled tenants a process starts with; an inconsistent table is ``SystemExit``.

    An empty table (or one where every tenant is disabled) starts the process serving
    none: a tenant installed from Slack appears on the next directory refresh, without
    a restart. There is no fallback to the YAML file — that would leave two sources of
    truth; the one-time import stays the way a YAML tenant enters the database.
    """
    try:
        tenants = await directory.refresh()
    except ConfigError as err:
        raise SystemExit(f"tenants in the database are inconsistent: {err}") from err
    if not tenants.tenants:
        log.info(
            "no enabled tenants in the database yet; serving none until a tenant is "
            "installed or imported (stored rows: %d)",
            directory.stored_count,
        )
    return tenants


async def load_stored_tenants(database_url: str) -> TenantsConfig:
    """The enabled tenants in the database, for a command-line tool that runs once."""
    pool = AsyncConnectionPool(database_url, open=False, **load_settings().pool_args())
    await pool.open()
    try:
        return await startup_tenants(TenantDirectory(PostgresTenantStore(pool)))
    finally:
        await pool.close()


# --- One-time import ----------------------------------------------------------


async def import_tenant(store: TenantStore, config: AgentsConfig) -> bool:
    """Store a YAML tenant that is not in the database yet.

    A tenant already stored is left exactly as it is, so a re-run never overwrites a
    setting changed since the import. A tenant that would share a Slack workspace,
    Notion workspace or secret variable with a stored one raises ``ConfigError``.
    """
    stored = []
    for ref in await store.list_refs():
        row = await store.load(ref.tenant_id)
        if row is not None:
            stored.append(row.config)
    if any(c.tenant_id == config.tenant_id for c in stored):
        return False
    TenantsConfig.of(*stored, config)
    return await store.insert(config)


async def run_import() -> int:
    from agentos.config import load_tenants_config

    settings = load_settings()
    try:
        tenants = load_tenants_config(settings.config_path)
    except (ConfigError, OSError) as err:
        print(f"cannot read the tenant YAML: {err}")
        return 1
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    failed = False
    try:
        store = PostgresTenantStore(pool)
        for tenant_id, config in tenants.tenants.items():
            try:
                created = await import_tenant(store, config)
            except ConfigError as err:
                print(f"tenant '{tenant_id}': import refused: {err}")
                failed = True
                continue
            if created:
                print(f"tenant '{tenant_id}': imported")
            else:
                print(
                    f"tenant '{tenant_id}': skipped, already in the database "
                    "(the database is the source of truth; the YAML file is not applied again)"
                )
    finally:
        await pool.close()
    return 1 if failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the tenants stored in the database.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "import",
        help="insert each YAML tenant that is not in the database yet; stored tenants are skipped",
    )
    parser.parse_args()
    sys.exit(asyncio.run(run_import()))


if __name__ == "__main__":
    main()
