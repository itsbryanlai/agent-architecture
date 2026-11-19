"""The tenants one deployment serves, and each tenant's own clients.

Rules kept here:

* A tenant's tokens are used only for that tenant. Each tenant has its own
  Slack client, built from the token ``TenantCredentials`` resolves for it (its
  encrypted credential row first, else the environment variable its config
  names), and its own Notion client when it has a Notion token. One tenant's
  client never serves another tenant's work.
* An unknown tenant is never answered with a default tenant: ``get`` raises.
* A tenant with no Notion token is served Slack-only: its runtime carries no
  Notion client, and nothing calls one for it (its agents hold no Notion grants
  until an admin connects Notion and publishes).
* A missing secret fails with a message naming the variable and the tenant,
  never a value, and no value is kept anywhere but inside the client that needs
  it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace

from agentos import ids
from agentos.config import (
    AgentsConfig,
    ConfigError,
    Settings,
    TenantsConfig,
    read_secret,
)
from agentos.connectors.notion import NotionAPI, NotionUserCache, normalize_id
from agentos.slack_client import SlackPoster
from agentos.tenant_store import TenantSource

log = logging.getLogger("agentos.tenants")


class UnknownTenant(Exception):
    """The tenant ID names no configured tenant."""


@dataclass
class TenantRuntime:
    """One tenant's settings and the clients that act for it, and only it.

    ``config.agents`` is empty when the settings come from the tenants table: the
    agents a turn runs are the published versions in the agent store.
    """

    config: AgentsConfig
    # The clients hold tokens, so they stay out of repr.
    slack: SlackPoster = field(repr=False)
    # None for a Slack-only tenant (no Notion token): no activity of its agents calls Notion.
    notion_api: NotionAPI | None = field(default=None, repr=False)
    # What this tenant's Notion workspace says about people. Shared by every connector built
    # for the tenant, so a requester is looked up once per cache lifetime, not once per call.
    notion_users: NotionUserCache = field(default_factory=NotionUserCache, repr=False)
    # The token the Slack client was built with, so a sync rebuilds it when a reinstall
    # replaces the credential. In memory only, like the client's own copy; never logged.
    slack_token: str = field(default="", repr=False)


def runtime_for(
    config: AgentsConfig,
    slack: SlackPoster,
    notion_api: NotionAPI | None = None,
    slack_token: str = "",
) -> TenantRuntime:
    """Build a tenant's runtime."""
    return TenantRuntime(config, slack, notion_api, slack_token=slack_token)


class TenantRegistry:
    """Looks tenants up by ID. An unknown ID raises ``UnknownTenant``."""

    def __init__(self, runtimes: dict[str, TenantRuntime]):
        self._runtimes = dict(runtimes)

    def get(self, tenant_id: str) -> TenantRuntime:
        try:
            return self._runtimes[tenant_id]
        except KeyError:
            raise UnknownTenant(f"unknown tenant '{tenant_id}'") from None

    def tenants(self) -> list[str]:
        return list(self._runtimes)

    def set(self, tenant_id: str, runtime: TenantRuntime) -> None:
        self._runtimes[tenant_id] = runtime

    def remove(self, tenant_id: str) -> None:
        self._runtimes.pop(tenant_id, None)

    def only_tenant(self) -> str | None:
        """The tenant ID when exactly one tenant is configured, else None."""
        if len(self._runtimes) == 1:
            return next(iter(self._runtimes))
        return None


class RegistrySync:
    """Keeps a worker's ``TenantRegistry`` equal to the directory's enabled tenants.

    ``sync`` adds a tenant added to the database (building its clients from the token
    the resolver returns, or the variables its settings name without one), swaps in
    changed settings, rebuilds a client whose credential was replaced, and drops a
    tenant that was disabled or removed, so the worker serves a tenant without a
    restart. A new tenant that cannot be admitted (its Slack token is unavailable, or
    ``admit`` returns a reason) is left out and reported once per reason; the next
    ``sync`` tries again. A tenant with no Notion token is served Slack-only.
    """

    def __init__(
        self,
        directory: TenantSource,
        registry: TenantRegistry,
        slack_factory: Callable[[str], SlackPoster],
        notion_factory: Callable[[str], NotionAPI],
        admit: Callable[[AgentsConfig], Awaitable[str | None]] | None = None,
        slack_token: Callable[[AgentsConfig], Awaitable[str]] | None = None,
    ):
        self._directory = directory
        self._registry = registry
        self._slack_factory = slack_factory
        self._notion_factory = notion_factory
        self._admit = admit
        # Resolves a tenant's Slack bot token (credential row first, env fallback).
        # None reads only the environment variable the tenant's settings name.
        self._slack_token = slack_token
        self._reported: set[tuple[str, str]] = set()

    def _report(self, tenant_id: str, reason: str) -> None:
        if (tenant_id, reason) not in self._reported:
            self._reported.add((tenant_id, reason))
            log.error("tenant_id=%s not served yet: %s", tenant_id, reason)

    async def sync(self) -> list[str]:
        """Bring the registry up to date; the IDs of tenants added by this call."""
        wanted = (await self._directory.snapshot()).tenants
        for tenant_id in self._registry.tenants():
            if tenant_id not in wanted:
                self._registry.remove(tenant_id)
                log.info("tenant_id=%s no longer served (disabled or removed)", tenant_id)
        added = []
        for tenant_id, config in wanted.items():
            try:
                current = self._registry.get(tenant_id)
            except UnknownTenant:
                current = None
            try:
                if self._slack_token is not None:
                    token = await self._slack_token(config)
                else:
                    token = require_tenant_secret(config, "slack_bot_token_env")
            except ConfigError as err:
                self._report(tenant_id, str(err))
                continue
            if current is not None and current.config == config and current.slack_token == token:
                continue
            if (
                current is not None
                and current.config.secrets == config.secrets
                and current.slack_token == token
            ):
                self._registry.set(tenant_id, replace(current, config=config))
                continue
            try:
                if current is None and self._admit is not None:
                    failure = await self._admit(config)
                    if failure:
                        self._report(tenant_id, failure)
                        continue
                slack = self._slack_factory(token)
                notion_token = read_secret(config.secrets.notion_token_env)
                notion = self._notion_factory(notion_token) if notion_token else None
            except ConfigError as err:
                self._report(tenant_id, str(err))
                continue
            self._registry.set(tenant_id, runtime_for(config, slack, notion, slack_token=token))
            if current is None:
                added.append(tenant_id)
                log.info("tenant_id=%s now served", tenant_id)
        return added

    async def run(self, interval_seconds: float) -> None:
        """Sync forever, every ``interval_seconds``. A failed pass is logged, not fatal."""
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                await self.sync()
            except Exception as err:  # noqa: BLE001 - the loop must outlive one failed read
                log.error("tenant sync failed: %s", type(err).__name__)


def require_tenant_secret(config: AgentsConfig, field_name: str) -> str:
    """Read one of a tenant's secrets, naming the variable and tenant if it is unset.

    ``field_name`` is a ``TenantSecrets`` field such as ``notion_token_env``.
    The error names the variable and the tenant only, never a value.
    """
    var = config.secrets.names()[field_name]
    value = read_secret(var)
    if not value:
        raise ConfigError(
            f"tenant '{config.tenant_id}': environment variable {var} is not set. "
            "Export it in the shell that runs this process."
        )
    return value


async def build_registry(
    tenants_config: TenantsConfig,
    settings: Settings,
    slack_factory: Callable[[str], SlackPoster],
    notion_factory: Callable[[str], NotionAPI],
    slack_token: Callable[[AgentsConfig], Awaitable[str]] | None = None,
) -> TenantRegistry:
    """Build every tenant's clients from its own resolved credentials.

    Production passes ``RealSlackPoster`` and ``RealNotionAPI`` and the credential
    resolver; tests pass factories that return fakes. ``settings`` carries the
    platform-wide settings; the tenants' secrets are not in it. A tenant with no
    Notion token gets a Slack-only runtime.
    """
    runtimes = {}
    for tenant_id, config in tenants_config.tenants.items():
        if slack_token is not None:
            token = await slack_token(config)
        else:
            token = require_tenant_secret(config, "slack_bot_token_env")
        slack = slack_factory(token)
        notion_token = read_secret(config.secrets.notion_token_env)
        notion = notion_factory(notion_token) if notion_token else None
        runtimes[tenant_id] = runtime_for(config, slack, notion, slack_token=token)
    return TenantRegistry(runtimes)


def granted_roots(agents: AgentsConfig) -> dict[str, str]:
    """Scope to root page ID, for every distinct page granted to any agent of a tenant.

    Pass a config whose ``agents`` are the tenant's published set
    (``agent_store.with_published_agents``).
    """
    roots: dict[str, str] = {}
    for agent in agents.agents.values():
        for grant in agent.grants.notion_pages:
            roots.setdefault(ids.knowledge_scope(grant.id), normalize_id(grant.id))
    return roots
