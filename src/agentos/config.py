"""Settings from the environment and agent definitions from YAML.

Secrets come only from environment variables that the operator exports in their
own shell. Nothing here writes, prints or logs a secret, and nothing fails at
import time when one is unset: a missing secret only becomes an error when the
real client that needs it is constructed (see ``Settings.require``).

This module is the only place that reads environment variables.

The two infrastructure variables carry the AGENTOS_ prefix
(AGENTOS_DATABASE_URL, AGENTOS_TEMPORAL_ADDRESS) and never fall back to the
generic names DATABASE_URL and TEMPORAL_ADDRESS. A generic name is often set in
the shell for some other project, and the platform must never be redirected to
that project's database or server by accident.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MODEL = "claude-sonnet-5-5"
DEFAULT_QWEN_MODEL = "qwen3.7-plus"
DEFAULT_TEMPORAL_ADDRESS = "localhost:7233"
DEFAULT_DATABASE_URL = "postgresql://agentos:agentos@localhost:55432/agentos"
DEFAULT_CONFIG_PATH = "config/agents.yaml"
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

_TRUE_VALUES = {"1", "true", "yes", "on"}


class ConfigError(ValueError):
    """The agents file or a setting is malformed or incomplete."""


@dataclass(frozen=True)
class Settings:
    # Secret values are excluded from repr so that logging a Settings object
    # can never leak them.
    slack_bot_token: str | None = field(default=None, repr=False)
    slack_signing_secret: str | None = field(default=None, repr=False)
    notion_token: str | None = field(default=None, repr=False)
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
        slack_bot_token=env.get("SLACK_BOT_TOKEN"),
        slack_signing_secret=env.get("SLACK_SIGNING_SECRET"),
        notion_token=env.get("NOTION_TOKEN"),
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
class AgentsConfig:
    tenant_id: str
    slack_team_id: str
    agents: dict[str, AgentVersion]
    default_agent: str


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
    return AgentsConfig(
        tenant_id=str(_require(raw, "tenant_id", "agents config")),
        slack_team_id=str(_require(raw, "slack_team_id", "agents config")),
        agents=agents,
        default_agent=default_agent,
    )


def load_agents_config(path: str | Path) -> AgentsConfig:
    with open(path, encoding="utf-8") as fh:
        return parse_agents_config(yaml.safe_load(fh))
