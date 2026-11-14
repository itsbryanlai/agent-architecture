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

from collections.abc import Callable
from dataclasses import dataclass, field

from agentos import ids
from agentos.config import (
    AgentsConfig,
    ConfigError,
    Settings,
    TenantsConfig,
    read_secret,
)
from agentos.connectors.notion import NotionAPI, normalize_id
from agentos.slack_client import SlackPoster


class UnknownTenant(Exception):
    """The tenant ID names no configured tenant."""


@dataclass
class TenantRuntime:
    """One tenant's settings and the clients that act for it, and only it.

    ``config.agents`` is the YAML section, the source of the one-time import.
    The agents a turn runs are the published versions in the agent store.
    """

    config: AgentsConfig
    # The clients hold tokens, so they stay out of repr.
    slack: SlackPoster = field(repr=False)
    notion_api: NotionAPI = field(repr=False)


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

    def only_tenant(self) -> str | None:
        """The tenant ID when exactly one tenant is configured, else None."""
        if len(self._runtimes) == 1:
            return next(iter(self._runtimes))
        return None


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
