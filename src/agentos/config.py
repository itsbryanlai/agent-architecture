"""Settings from the environment and agent definitions from YAML.

Secrets come only from environment variables that the operator exports in their
own shell. Nothing here writes, prints or logs a secret, and nothing fails at
import time when one is unset: a missing secret only becomes an error when the
real client that needs it is constructed (see ``Settings.require``).

This module is the only place that reads environment variables. Each tenant
names, in its own config, the variables that hold its secrets; ``read_secret``
reads one of them when it is needed and nothing keeps the value.

The two infrastructure variables carry the AGENTOS_ prefix
(AGENTOS_DATABASE_URL, AGENTOS_TEMPORAL_ADDRESS) and never fall back to the
generic names DATABASE_URL and TEMPORAL_ADDRESS. A generic name is often set in
the shell for some other project, and the platform must never be redirected to
that project's database or server by accident.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MODEL = "claude-sonnet-5-5"
DEFAULT_QWEN_MODEL = "qwen3.7-plus"
DEFAULT_TEMPORAL_ADDRESS = "localhost:7233"
# The app connects as agentos_app, which cannot bypass row-level security. The
# password is a local development value, like the Compose one.
DEFAULT_DATABASE_URL = "postgresql://agentos_app:agentos_app@localhost:55432/agentos"
DEFAULT_CONFIG_PATH = "config/agents.yaml"
# The directory where the gateway writes each tenant's Notion verification token.
DEFAULT_NOTION_TOKEN_DIR = "~/.agentos"
# The variable names a tenant without a ``secrets`` block uses. Only one tenant
# may use them.
DEFAULT_SLACK_BOT_TOKEN_ENV = "SLACK_BOT_TOKEN"
DEFAULT_NOTION_TOKEN_ENV = "NOTION_TOKEN"
DEFAULT_NOTION_WEBHOOK_TOKEN_ENV = "NOTION_WEBHOOK_VERIFICATION_TOKEN"
DEFAULT_EMBED_MODEL = "text-embedding-v4"
# Must match the vector(1024) column in the knowledge_chunks table.
EMBED_DIMENSIONS = 1024

TASK_QUEUE = "agentos-main"

# Which model provider the worker uses. Anthropic is the default; Qwen is a
# second provider selectable for testing.
PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_QWEN = "qwen"
LLM_PROVIDERS = (PROVIDER_ANTHROPIC, PROVIDER_QWEN)
DEFAULT_LLM_PROVIDER = PROVIDER_ANTHROPIC
# Model used when AGENTOS_MODEL is unset, per provider.
DEFAULT_MODELS = {PROVIDER_ANTHROPIC: DEFAULT_MODEL, PROVIDER_QWEN: DEFAULT_QWEN_MODEL}

# A tenant ID is the text before the first ":" of every workflow ID and part of
# a file name, so it may hold only lowercase letters, digits and hyphens.
TENANT_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")

_TRUE_VALUES = {"1", "true", "yes", "on"}


class ConfigError(ValueError):
    """The agents file or a setting is malformed or incomplete."""


@dataclass(frozen=True)
class Settings:
    # Secret values are excluded from repr so that logging a Settings object
    # can never leak them.
    # There is one Slack app, so its signing secret is shared by every tenant.
    # Each tenant's own tokens are named in its config and read with
    # ``read_secret``; they are not settings.
    slack_signing_secret: str | None = field(default=None, repr=False)
    # Where the gateway writes the token each tenant's Notion subscription
    # handshake delivers, one file per tenant. A leading "~" is expanded when
    # the file is used, not when settings load.
    notion_token_dir: str = DEFAULT_NOTION_TOKEN_DIR
    anthropic_api_key: str | None = field(default=None, repr=False)
    qwen_api_key: str | None = field(default=None, repr=False)
    llm_provider: str = DEFAULT_LLM_PROVIDER
    model: str = DEFAULT_MODEL
    # The OpenAI-compatible endpoint that serves Qwen; not a secret.
    qwen_base_url: str | None = None
    # Qwen thinking is off by default: thinking output counts toward the
    # response token limit and makes each turn slower.
    qwen_thinking: bool = False
    # The embedding model served by whichever embedding endpoint is configured.
    embed_model: str = DEFAULT_EMBED_MODEL
    # An OpenAI-compatible embeddings endpoint of its own (for example a local
    # Ollama). Unset means the Qwen settings are used.
    embed_base_url: str | None = None
    embed_api_key: str | None = field(default=None, repr=False)
    temporal_address: str = DEFAULT_TEMPORAL_ADDRESS
    database_url: str = field(default=DEFAULT_DATABASE_URL, repr=False)
    config_path: str = DEFAULT_CONFIG_PATH

    def require(self, name: str) -> str:
        """Return a required setting, or fail with a message naming the variable.

        The message names the environment variable only, never a value.
        """
        value = getattr(self, name)
        if not value:
            env_name = name.upper()
            raise RuntimeError(
                f"{env_name} is not set. Export it in the shell that runs this process."
            )
        return value


def _parse_provider(raw: str | None) -> str:
    provider = (raw or DEFAULT_LLM_PROVIDER).strip().lower()
    if provider not in LLM_PROVIDERS:
        raise ConfigError(
            f"AGENTOS_LLM_PROVIDER must be one of {', '.join(LLM_PROVIDERS)}; got {raw!r}"
        )
    return provider


def _parse_flag(raw: str | None) -> bool:
    return (raw or "").strip().lower() in _TRUE_VALUES


def load_settings() -> Settings:
    env = os.environ
    provider = _parse_provider(env.get("AGENTOS_LLM_PROVIDER"))
    return Settings(
        slack_signing_secret=env.get("SLACK_SIGNING_SECRET"),
        notion_token_dir=env.get("AGENTOS_NOTION_TOKEN_DIR") or DEFAULT_NOTION_TOKEN_DIR,
        anthropic_api_key=env.get("ANTHROPIC_API_KEY"),
        qwen_api_key=env.get("QWEN_API_KEY"),
        llm_provider=provider,
        model=env.get("AGENTOS_MODEL") or DEFAULT_MODELS[provider],
        qwen_base_url=env.get("QWEN_BASE_URL") or None,
        qwen_thinking=_parse_flag(env.get("AGENTOS_QWEN_THINKING")),
        embed_model=env.get("AGENTOS_EMBED_MODEL") or DEFAULT_EMBED_MODEL,
        embed_base_url=env.get("AGENTOS_EMBED_BASE_URL") or None,
        embed_api_key=env.get("EMBED_API_KEY") or None,
        temporal_address=env.get("AGENTOS_TEMPORAL_ADDRESS") or DEFAULT_TEMPORAL_ADDRESS,
        database_url=env.get("AGENTOS_DATABASE_URL") or DEFAULT_DATABASE_URL,
        config_path=env.get("AGENTOS_CONFIG") or DEFAULT_CONFIG_PATH,
    )


def read_secret(env_name: str) -> str | None:
    """The value of the environment variable a tenant's config names, or None.

    An empty value counts as unset. The caller must never log or store what
    this returns; error messages name the variable only.
    """
    return os.environ.get(env_name) or None


# The dataclasses below travel through Temporal (``load_agent`` returns an
# AgentVersion), so they hold only primitives, lists and nested dataclasses.


@dataclass
class NotionDatabaseGrant:
    """One Notion database the agent may use, with its property mapping.

    Property names differ per database, so the creator maps them when granting
    the database. ``request_id_prop`` is the hidden text property that holds the
    idempotency key of items the agent creates.

    The ``*_prop`` fields are names, kept as labels for people. The runtime
    addresses Notion properties only by the ``*_prop_id`` fields, which are
    stable across renames. They may be missing in a names-only config so the
    check command can read it and print the IDs; the worker refuses to start
    until all four are set.
    """

    id: str
    name: str
    title_prop: str
    status_prop: str
    done_values: list[str]
    requested_by_prop: str
    request_id_prop: str
    title_prop_id: str | None = None
    status_prop_id: str | None = None
    requested_by_prop_id: str | None = None
    request_id_prop_id: str | None = None

    def missing_property_ids(self) -> list[str]:
        """Roles that have no property ID configured."""
        ids = {
            "title": self.title_prop_id,
            "status": self.status_prop_id,
            "requested_by": self.requested_by_prop_id,
            "request_id": self.request_id_prop_id,
        }
        return [role for role, value in ids.items() if not value]


@dataclass
class NotionPageGrant:
    """A Notion page the agent may read, with everything under it.

    Everything under the page becomes the agent's knowledge. The page is one
    scope that searches are filtered by.
    """

    id: str
    name: str


@dataclass
class Grants:
    notion_databases: list[NotionDatabaseGrant] = field(default_factory=list)
    notion_pages: list[NotionPageGrant] = field(default_factory=list)


@dataclass
class FollowUpPolicy:
    poll_interval_minutes: int = 15
    stale_after_days: int = 3
    max_watch_days: int = 30


@dataclass
class ApprovalPolicy:
    expires_after_hours: int = 72
    reminder_after_hours: int = 24


@dataclass
class AgentVersion:
    agent_id: str
    version: int
    name: str
    system_prompt: str
    max_steps: int
    grants: Grants
    follow_up: FollowUpPolicy
    approval: ApprovalPolicy


@dataclass
class TenantSecrets:
    """The names of the environment variables that hold one tenant's secrets.

    These are configuration, not secrets: only names are kept here, never a
    value, so they are safe in ``repr`` and logs.
    """

    slack_bot_token_env: str = DEFAULT_SLACK_BOT_TOKEN_ENV
    notion_token_env: str = DEFAULT_NOTION_TOKEN_ENV
    notion_webhook_token_env: str = DEFAULT_NOTION_WEBHOOK_TOKEN_ENV

    def names(self) -> dict[str, str]:
        """Field name to variable name."""
        return {
            "slack_bot_token_env": self.slack_bot_token_env,
            "notion_token_env": self.notion_token_env,
            "notion_webhook_token_env": self.notion_webhook_token_env,
        }


@dataclass
class AgentsConfig:
    tenant_id: str
    slack_team_id: str
    agents: dict[str, AgentVersion]
    default_agent: str
    # The Notion workspace this tenant's webhook events must come from.
    notion_workspace_id: str | None = None
    secrets: TenantSecrets = field(default_factory=TenantSecrets)


_PROPERTY_FIELDS = {
    "title": "title_prop",
    "status": "status_prop",
    "done_values": "done_values",
    "requested_by": "requested_by_prop",
    "request_id": "request_id_prop",
}


_PROPERTY_ID_FIELDS = {
    "title": "title_prop_id",
    "status": "status_prop_id",
    "requested_by": "requested_by_prop_id",
    "request_id": "request_id_prop_id",
}


def _require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if not isinstance(mapping, dict) or key not in mapping or mapping[key] in (None, ""):
        raise ConfigError(f"{where}: missing required field '{key}'")
    return mapping[key]


def _parse_database_grant(agent_id: str, raw: dict[str, Any]) -> NotionDatabaseGrant:
    where = f"agent '{agent_id}' notion database grant"
    db_id = str(_require(raw, "id", where))
    name = str(_require(raw, "name", where))
    props = raw.get("properties")
    if not isinstance(props, dict):
        raise ConfigError(f"agent '{agent_id}' grant '{name}': missing required field 'properties'")
    values: dict[str, Any] = {}
    for key, attr in _PROPERTY_FIELDS.items():
        if key not in props or props[key] in (None, "", []):
            raise ConfigError(
                f"agent '{agent_id}' grant '{name}': missing property mapping 'properties.{key}'"
            )
        values[attr] = props[key]
    ids_raw = raw.get("property_ids")
    if ids_raw is None:
        ids_raw = {}
    if not isinstance(ids_raw, dict):
        raise ConfigError(
            f"agent '{agent_id}' grant '{name}': 'property_ids' must be a mapping of role to ID"
        )
    id_values: dict[str, str | None] = {attr: None for attr in _PROPERTY_ID_FIELDS.values()}
    for key, value in ids_raw.items():
        if key not in _PROPERTY_ID_FIELDS:
            raise ConfigError(
                f"agent '{agent_id}' grant '{name}': unknown key 'property_ids.{key}'"
            )
        if value not in (None, ""):
            id_values[_PROPERTY_ID_FIELDS[key]] = str(value)
    done_values = values["done_values"]
    if not isinstance(done_values, list):
        done_values = [done_values]
    return NotionDatabaseGrant(
        id=db_id,
        name=name,
        title_prop=str(values["title_prop"]),
        status_prop=str(values["status_prop"]),
        done_values=[str(v) for v in done_values],
        requested_by_prop=str(values["requested_by_prop"]),
        request_id_prop=str(values["request_id_prop"]),
        **id_values,
    )


def _parse_page_grant(agent_id: str, raw: dict[str, Any]) -> NotionPageGrant:
    where = f"agent '{agent_id}' notion page grant"
    return NotionPageGrant(
        id=str(_require(raw, "id", where)), name=str(_require(raw, "name", where))
    )


def _parse_agent(agent_id: str, raw: dict[str, Any]) -> AgentVersion:
    where = f"agent '{agent_id}'"
    grants_raw = raw.get("grants") or {}
    notion = grants_raw.get("notion") or {}
    databases = [_parse_database_grant(agent_id, db) for db in notion.get("databases") or []]
    pages = [_parse_page_grant(agent_id, pg) for pg in notion.get("pages") or []]
    follow = raw.get("follow_up") or {}
    approval = raw.get("approval") or {}
    return AgentVersion(
        agent_id=agent_id,
        version=int(_require(raw, "version", where)),
        name=str(_require(raw, "name", where)),
        system_prompt=str(_require(raw, "system_prompt", where)),
        max_steps=int(_require(raw, "max_steps", where)),
        grants=Grants(notion_databases=databases, notion_pages=pages),
        follow_up=FollowUpPolicy(**{k: int(v) for k, v in follow.items()}),
        approval=ApprovalPolicy(**{k: int(v) for k, v in approval.items()}),
    )


def parse_agents_config(raw: dict[str, Any]) -> AgentsConfig:
    if not isinstance(raw, dict):
        raise ConfigError("agents config: expected a mapping at the top level")
    agents_raw = _require(raw, "agents", "agents config")
    agents = {aid: _parse_agent(aid, a or {}) for aid, a in agents_raw.items()}
    default_agent = str(_require(raw, "default_agent", "agents config"))
    if default_agent not in agents:
        raise ConfigError(f"agents config: default_agent '{default_agent}' is not defined")
    tenant_id = str(_require(raw, "tenant_id", "agents config"))
    if not TENANT_ID_PATTERN.fullmatch(tenant_id):
        raise ConfigError(
            f"tenant_id {tenant_id!r} is invalid: use 1 to 63 lowercase letters, digits and "
            "hyphens, starting with a letter or digit"
        )
    workspace = raw.get("notion_workspace_id")
    return AgentsConfig(
        tenant_id=tenant_id,
        slack_team_id=str(_require(raw, "slack_team_id", "agents config")),
        agents=agents,
        default_agent=default_agent,
        notion_workspace_id=str(workspace) if workspace not in (None, "") else None,
        secrets=_parse_secrets(tenant_id, raw.get("secrets")),
    )


def _parse_secrets(tenant_id: str, raw: Any) -> TenantSecrets:
    """The ``secrets`` block: names of variables. Missing names use the defaults."""
    if raw is None:
        return TenantSecrets()
    where = f"tenant '{tenant_id}' secrets"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a mapping of field to environment variable name")
    known = TenantSecrets().names()
    for key in raw:
        if key not in known:
            raise ConfigError(f"{where}: unknown field '{key}'")
    values = {}
    for key, value in raw.items():
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f"{where}: '{key}' must be an environment variable name")
        values[key] = value.strip()
    return TenantSecrets(**values)


def load_agents_config(path: str | Path) -> AgentsConfig:
    with open(path, encoding="utf-8") as fh:
        try:
            return parse_agents_config(yaml.safe_load(fh))
        except ConfigError as err:
            raise ConfigError(f"{path}: {err}") from err


@dataclass
class TenantsConfig:
    """Every tenant this deployment serves, by tenant ID.

    Built only through ``TenantsConfig.of`` or ``load_tenants_config``, which
    reject two tenants that share an ID, a Slack workspace, a Notion workspace
    or a secret variable, so each lookup below has at most one answer.
    """

    tenants: dict[str, AgentsConfig]

    @classmethod
    def of(cls, *configs: AgentsConfig) -> TenantsConfig:
        """Check the tenants against each other and collect them."""
        _check_unique(configs)
        return cls({c.tenant_id: c for c in configs})

    def get(self, tenant_id: str) -> AgentsConfig | None:
        return self.tenants.get(tenant_id)

    def by_slack_team(self, team_id: str | None) -> str | None:
        """The tenant of a Slack workspace, or None when none is configured."""
        for tenant in self.tenants.values():
            if team_id and tenant.slack_team_id == team_id:
                return tenant.tenant_id
        return None

    def only_tenant(self) -> str | None:
        """The tenant ID when exactly one tenant is configured, else None."""
        if len(self.tenants) == 1:
            return next(iter(self.tenants))
        return None


def _check_unique(configs: tuple[AgentsConfig, ...]) -> None:
    """Raise ``ConfigError`` naming the tenants and the field they share."""
    seen: dict[tuple[str, str], str] = {}

    def claim(field_name: str, value: str, tenant_id: str, label: str = "") -> None:
        key = (field_name, value)
        if key in seen:
            raise ConfigError(
                f"tenants '{seen[key]}' and '{tenant_id}' share {label or field_name} '{value}'; "
                "each tenant needs its own"
            )
        seen[key] = tenant_id

    for config in configs:
        claim("tenant_id", config.tenant_id, config.tenant_id)
        claim("slack_team_id", config.slack_team_id, config.tenant_id)
        if config.notion_workspace_id:
            claim("notion_workspace_id", config.notion_workspace_id, config.tenant_id)
    defaults = TenantSecrets().names()
    for config in configs:
        for field_name, var in config.secrets.names().items():
            key = ("secret", var)
            if key in seen:
                other = seen[key]
                relied = var == defaults[field_name]
                hint = (
                    " (the default name; at most one tenant may omit it, so give the others a "
                    "secrets block)"
                    if relied
                    else ""
                )
                raise ConfigError(
                    f"tenants '{other}' and '{config.tenant_id}' both use the secret variable "
                    f"'{var}' for {field_name}{hint}"
                )
            seen[key] = config.tenant_id


def load_tenants_config(path: str | Path) -> TenantsConfig:
    """Load tenants from one file (a single tenant) or a directory of ``*.yaml``.

    A file is today's single-tenant format. In a directory every ``*.yaml``
    directly inside it is one tenant; files are read in name order.
    """
    p = Path(path)
    if p.is_dir():
        files = sorted(p.glob("*.yaml"))
        if not files:
            raise ConfigError(f"{p}: no *.yaml tenant files found")
        return TenantsConfig.of(*(load_agents_config(f) for f in files))
    return TenantsConfig.of(load_agents_config(p))
