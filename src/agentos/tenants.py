"""The tenants one deployment serves, and each tenant's own clients.

Rules kept here:

* A tenant's tokens are used only for that tenant. Each tenant has its own
  Slack client and its own Notion client, built from the environment variables
  its config names. One tenant's client never serves another tenant's work.
* An unknown tenant is never answered with a default tenant: ``get`` raises.
* Secrets come only from environment variables. A missing one fails with a
  message naming the variable and the tenant, never a value, and no value is
  kept anywhere but inside the client that needs it.
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
    notion_api: NotionAPI = field(repr=False)
    # What this tenant's Notion workspace says about people. Shared by every connector built
    # for the tenant, so a requester is looked up once per cache lifetime, not once per call.
    notion_users: NotionUserCache = field(default_factory=NotionUserCache, repr=False)


def runtime_for(config: AgentsConfig, slack: SlackPoster, notion_api: NotionAPI) -> TenantRuntime:
    """Build a tenant's runtime."""
    return TenantRuntime(config, slack, notion_api)


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

    ``sync`` adds a tenant added to the database (building its clients from the
    variables its settings name), swaps in changed settings, and drops a tenant that
    was disabled or removed, so the worker serves a tenant without a restart. A new
    tenant that cannot be admitted (a secret variable is unset, or ``admit`` returns a
    reason) is left out and reported once per reason; the next ``sync`` tries again.
    """

    def __init__(
        self,
        directory: TenantSource,
        registry: TenantRegistry,
        slack_factory: Callable[[str], SlackPoster],
        notion_factory: Callable[[str], NotionAPI],
        admit: Callable[[AgentsConfig], Awaitable[str | None]] | None = None,
    ):
        self._directory = directory
        self._registry = registry
        self._slack_factory = slack_factory
        self._notion_factory = notion_factory
        self._admit = admit
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
            if current is not None and current.config == config:
                continue
            if current is not None and current.config.secrets == config.secrets:
                self._registry.set(tenant_id, replace(current, config=config))
                continue
            try:
                if current is None and self._admit is not None:
                    failure = await self._admit(config)
                    if failure:
                        self._report(tenant_id, failure)
                        continue
                slack = self._slack_factory(require_tenant_secret(config, "slack_bot_token_env"))
                notion = self._notion_factory(require_tenant_secret(config, "notion_token_env"))
            except ConfigError as err:
                self._report(tenant_id, str(err))
                continue
            self._registry.set(tenant_id, runtime_for(config, slack, notion))
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


def notion_key_failure(tenants_config: TenantsConfig) -> str | None:
    """Why the control plane may not start, or None.

    Every tenant's agents can be granted Notion pages and databases, and the grant
    check (also run inside every publish) reads them with that tenant's own
    Notion key. A tenant whose variable is unset is named by variable, never value.
    """
    failures = []
    for config in tenants_config.tenants.values():
        try:
            require_tenant_secret(config, "notion_token_env")
        except ConfigError as err:
            failures.append(str(err))
    return "\n".join(failures) or None


def build_registry(
    tenants_config: TenantsConfig,
    settings: Settings,
    slack_factory: Callable[[str], SlackPoster],
    notion_factory: Callable[[str], NotionAPI],
) -> TenantRegistry:
    """Build every tenant's clients from the variables its config names.

    Production passes ``RealSlackPoster`` and ``RealNotionAPI``; tests pass
    factories that return fakes. ``settings`` carries the platform-wide
    settings; the tenants' secrets are not in it.
    """
    runtimes = {}
    for tenant_id, config in tenants_config.tenants.items():
        slack = slack_factory(require_tenant_secret(config, "slack_bot_token_env"))
        notion = notion_factory(require_tenant_secret(config, "notion_token_env"))
        runtimes[tenant_id] = runtime_for(config, slack, notion)
    return TenantRegistry(runtimes)


def build_slack_clients(
    tenants_config: TenantsConfig, slack_factory: Callable[[str], SlackPoster]
) -> dict[str, SlackPoster]:
    """One Slack client per tenant, each with that tenant's own bot token."""
    return {
        tenant_id: slack_factory(require_tenant_secret(config, "slack_bot_token_env"))
        for tenant_id, config in tenants_config.tenants.items()
    }


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
