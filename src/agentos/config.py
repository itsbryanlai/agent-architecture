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

import dataclasses
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_MODEL = "claude-sonnet-5-5"
DEFAULT_QWEN_MODEL = "qwen3.7-plus"
DEFAULT_TEMPORAL_ADDRESS = "localhost:7233"
DEFAULT_CONFIG_PATH = "config/agents.yaml"
# Model prices in US dollars per million tokens, used to cost each model call.
DEFAULT_PRICES_PATH = "config/prices.yaml"
# The directory where the gateway writes each tenant's Notion verification token.
DEFAULT_NOTION_TOKEN_DIR = "~/.agentos"
# The directory of per-tenant payload encryption key files, <tenant_id>.key.
DEFAULT_KEY_DIR = "~/.agentos/keys"
# Where payload keys come from: owner-only files (the default, unchanged local
# runs), host variables named AGENTOS_PAYLOAD_KEY_<TENANT>_V<n>, or the database:
# tenant keys wrapped by master keys from AGENTOS_MASTER_KEY_V<n> variables.
KEY_SOURCE_FILES = "files"
KEY_SOURCE_ENV = "env"
KEY_SOURCE_DB = "db"
KEY_SOURCES = (KEY_SOURCE_FILES, KEY_SOURCE_ENV, KEY_SOURCE_DB)
DEFAULT_KEY_SOURCE = KEY_SOURCE_FILES
# The prefix of the variables that hold payload keys when the source is env.
PAYLOAD_KEY_ENV_PREFIX = "AGENTOS_PAYLOAD_KEY_"
# The prefix of the variables that hold master keys when the source is db.
MASTER_KEY_ENV_PREFIX = "AGENTOS_MASTER_KEY_V"
# The variable names a tenant without a ``secrets`` block uses. Only one tenant
# may use them.
DEFAULT_SLACK_BOT_TOKEN_ENV = "SLACK_BOT_TOKEN"
DEFAULT_NOTION_TOKEN_ENV = "NOTION_TOKEN"
DEFAULT_NOTION_WEBHOOK_TOKEN_ENV = "NOTION_WEBHOOK_VERIFICATION_TOKEN"
DEFAULT_EMBED_MODEL = "text-embedding-v4"
DEFAULT_DB_POOL_MIN = 2
DEFAULT_DB_POOL_MAX = 32
DEFAULT_MAX_CONCURRENT_ACTIVITIES = 32
# Must match the vector(1024) column in the knowledge_chunks table.
EMBED_DIMENSIONS = 1024
# The farthest a chunk may be from the question (cosine distance: 0 identical, 1 unrelated,
# 2 opposite) and still be shown to the model, per embedding model. A model not listed has
# no cut-off, because its distances have not been looked at. ``bge-m3`` is the local model
# the maintainer runs: published scores put related text mostly above 0.5 similarity and
# unrelated text below 0.4, so a distance of 0.6 cuts the unrelated and keeps the related.
# The value is a starting point from those scores, not measured on this deployment's pages;
# ``AGENTOS_EMBED_MAX_DISTANCE`` overrides it. ``fake-embed`` is the test embedder, whose
# unrelated texts are at distance 1.
EMBED_MAX_DISTANCE: dict[str, float] = {"bge-m3": 0.6, "fake-embed": 0.9}


def embed_max_distance(model: str, override: float | None = None) -> float | None:
    """The cut-off for chunks of ``model``, or None when there is none."""
    return override if override is not None else EMBED_MAX_DISTANCE.get(model)


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


# Settings whose variable is not the upper-cased field name.
_ENV_NAMES = {"database_url": "AGENTOS_DATABASE_URL"}


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
    # Where each tenant's payload encryption key file lives. A leading "~" is
    # expanded when the key is read. Holds a directory name, never a key.
    key_dir: str = DEFAULT_KEY_DIR
    # Where payload keys are read from: "files" (key_dir) or "env" (the
    # AGENTOS_PAYLOAD_KEY_<TENANT>_V<n> variables). Every process chooses its
    # key store through agentos.crypto_keys.build_key_store, so they agree.
    key_source: str = DEFAULT_KEY_SOURCE
    # Lets the payload codec read payloads that were never encrypted (a history
    # from before encryption). Off by default: with Temporal reachable, a
    # plaintext payload would otherwise be accepted as genuine.
    allow_plaintext_payloads: bool = False
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
    # Overrides the cut-off in ``EMBED_MAX_DISTANCE`` for the configured embedding model.
    embed_max_distance: float | None = None
    temporal_address: str = DEFAULT_TEMPORAL_ADDRESS
    # The app connects as agentos_app, which cannot bypass row-level security. There is no
    # default: the URL holds the role's password, which the operator exports and no file
    # in the repository carries.
    database_url: str | None = field(default=None, repr=False)
    config_path: str = DEFAULT_CONFIG_PATH
    prices_path: str = DEFAULT_PRICES_PATH
    # Sign in with Slack for the web console: the Slack app's client ID and secret,
    # and the public base URL the browser reaches the console at. The console is
    # configured only when all three are set (see ``slack_signin``).
    slack_client_id: str | None = None
    slack_client_secret: str | None = field(default=None, repr=False)
    console_url: str | None = None
    # The Notion public integration behind "Choose from Notion" (see ``notion_picker``), and
    # whether a new grant must have been chosen there before a version can be submitted.
    notion_oauth_client_id: str | None = None
    notion_oauth_client_secret: str | None = field(default=None, repr=False)
    require_grant_picker: bool = False
    # The pilot allowlist for the Slack OAuth install: the Slack team IDs that may
    # install. Empty (the default) refuses every install.
    install_allowed_teams: tuple[str, ...] = ()
    # Database connections per process (min and max of the pool) and how many activities the
    # worker runs at once. An activity holds a connection only while it runs a query, and a
    # model call makes about six, so a pool smaller than the activity count makes activities
    # queue for a connection. The worker therefore refuses to start with a pool max below
    # ``max_concurrent_activities`` (see ``validate_worker_capacity``). The defaults suit one
    # worker next to the gateway and the console on Postgres's default 100 connections:
    # 32 + 32 + 32 stays under it. Raise both together, and the server's limit with them.
    db_pool_min: int = DEFAULT_DB_POOL_MIN
    db_pool_max: int = DEFAULT_DB_POOL_MAX
    max_concurrent_activities: int = DEFAULT_MAX_CONCURRENT_ACTIVITIES

    def pool_args(self) -> dict[str, int]:
        """Keyword arguments for ``AsyncConnectionPool``: its minimum and maximum size."""
        return {"min_size": self.db_pool_min, "max_size": self.db_pool_max}

    def require(self, name: str) -> str:
        """Return a required setting, or fail with a message naming the variable.

        The message names the environment variable only, never a value.
        """
        value = getattr(self, name)
        if not value:
            env_name = _ENV_NAMES.get(name, name.upper())
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


def _parse_key_source(raw: str | None) -> str:
    source = (raw or DEFAULT_KEY_SOURCE).strip().lower()
    if source not in KEY_SOURCES:
        raise ConfigError(
            f"AGENTOS_KEY_SOURCE must be one of {', '.join(KEY_SOURCES)}; got {raw!r}"
        )
    return source


def _parse_flag(raw: str | None) -> bool:
    return (raw or "").strip().lower() in _TRUE_VALUES


def _parse_install_teams(raw: str | None) -> tuple[str, ...]:
    """Comma-separated Slack team IDs; unset or empty means no team may install."""
    return tuple(part.strip() for part in (raw or "").split(",") if part.strip())


def _parse_distance(raw: str | None) -> float | None:
    if not raw or not raw.strip():
        return None
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if not 0 < value <= 2:
        raise ConfigError(
            f"AGENTOS_EMBED_MAX_DISTANCE must be a number above 0 up to 2; got {raw!r}"
        )
    return value


def _parse_positive_int(env: Any, name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        value = 0
    if value < 1:
        raise ConfigError(f"{name} must be a whole number of at least 1; got {raw!r}")
    return value


def validate_worker_capacity(settings: Settings) -> None:
    """Refuse a connection pool that cannot serve the activities the worker runs at once."""
    if settings.db_pool_max < settings.db_pool_min:
        raise ConfigError(
            f"AGENTOS_DB_POOL_MAX ({settings.db_pool_max}) is below "
            f"AGENTOS_DB_POOL_MIN ({settings.db_pool_min})"
        )
    if settings.db_pool_max < settings.max_concurrent_activities:
        raise ConfigError(
            f"AGENTOS_DB_POOL_MAX ({settings.db_pool_max}) is below "
            f"AGENTOS_MAX_CONCURRENT_ACTIVITIES ({settings.max_concurrent_activities}): "
            "activities would wait for a connection. Raise the pool or lower the activities."
        )


def load_settings() -> Settings:
    env = os.environ
    provider = _parse_provider(env.get("AGENTOS_LLM_PROVIDER"))
    return Settings(
        slack_signing_secret=env.get("SLACK_SIGNING_SECRET"),
        notion_token_dir=env.get("AGENTOS_NOTION_TOKEN_DIR") or DEFAULT_NOTION_TOKEN_DIR,
        key_dir=env.get("AGENTOS_KEY_DIR") or DEFAULT_KEY_DIR,
        key_source=_parse_key_source(env.get("AGENTOS_KEY_SOURCE")),
        allow_plaintext_payloads=_parse_flag(env.get("AGENTOS_ALLOW_PLAINTEXT_PAYLOADS")),
        anthropic_api_key=env.get("ANTHROPIC_API_KEY"),
        qwen_api_key=env.get("QWEN_API_KEY"),
        llm_provider=provider,
        model=env.get("AGENTOS_MODEL") or DEFAULT_MODELS[provider],
        qwen_base_url=env.get("QWEN_BASE_URL") or None,
        qwen_thinking=_parse_flag(env.get("AGENTOS_QWEN_THINKING")),
        embed_model=env.get("AGENTOS_EMBED_MODEL") or DEFAULT_EMBED_MODEL,
        embed_base_url=env.get("AGENTOS_EMBED_BASE_URL") or None,
        embed_api_key=env.get("EMBED_API_KEY") or None,
        embed_max_distance=_parse_distance(env.get("AGENTOS_EMBED_MAX_DISTANCE")),
        temporal_address=env.get("AGENTOS_TEMPORAL_ADDRESS") or DEFAULT_TEMPORAL_ADDRESS,
        database_url=env.get("AGENTOS_DATABASE_URL") or None,
        config_path=env.get("AGENTOS_CONFIG") or DEFAULT_CONFIG_PATH,
        prices_path=env.get("AGENTOS_PRICES") or DEFAULT_PRICES_PATH,
        slack_client_id=env.get("SLACK_CLIENT_ID") or None,
        slack_client_secret=env.get("SLACK_CLIENT_SECRET") or None,
        console_url=env.get("AGENTOS_CONSOLE_URL") or None,
        notion_oauth_client_id=env.get("NOTION_OAUTH_CLIENT_ID") or None,
        notion_oauth_client_secret=env.get("NOTION_OAUTH_CLIENT_SECRET") or None,
        require_grant_picker=_parse_flag(env.get("AGENTOS_REQUIRE_GRANT_PICKER")),
        install_allowed_teams=_parse_install_teams(env.get("AGENTOS_INSTALL_ALLOWED_TEAMS")),
        db_pool_min=_parse_positive_int(env, "AGENTOS_DB_POOL_MIN", DEFAULT_DB_POOL_MIN),
        db_pool_max=_parse_positive_int(env, "AGENTOS_DB_POOL_MAX", DEFAULT_DB_POOL_MAX),
        max_concurrent_activities=_parse_positive_int(
            env, "AGENTOS_MAX_CONCURRENT_ACTIVITIES", DEFAULT_MAX_CONCURRENT_ACTIVITIES
        ),
    )


def read_secret(env_name: str) -> str | None:
    """The value of the environment variable a tenant's config names, or None.

    An empty value counts as unset. The caller must never log or store what
    this returns; error messages name the variable only.
    """
    return os.environ.get(env_name) or None


def payload_key_env() -> dict[str, str]:
    """The payload key variables and their values, for the variable key store.

    Values are key material: the store keeps them in memory only, and nothing
    logs, prints or ``repr``s them.
    """
    return {
        name: value for name, value in os.environ.items() if name.startswith(PAYLOAD_KEY_ENV_PREFIX)
    }


def master_key_env() -> dict[str, str]:
    """The master key variables and their values, for the database key store.

    Values are key material: the store keeps them in memory only, and nothing
    logs, prints or ``repr``s them.
    """
    return {
        name: value for name, value in os.environ.items() if name.startswith(MASTER_KEY_ENV_PREFIX)
    }


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
    check command can read it and print the IDs; a published agent whose grant
    lacks them is unavailable until all four are set.
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


# The most an agent definition may ask for. A task lives at most one approval window plus one
# watch, so these bound how long a row a live task reads must be kept (see ``agentos.retention``).
MAX_APPROVAL_EXPIRES_HOURS = 168
MAX_WATCH_DAYS = 60


@dataclass
class FollowUpPolicy:
    poll_interval_minutes: int = 15
    stale_after_days: int = 3
    max_watch_days: int = 30


@dataclass
class ApprovalPolicy:
    expires_after_hours: int = 72
    reminder_after_hours: int = 24


DEFAULT_MAX_WRITES_PER_TURN = 2
MAX_WRITES_PER_TURN_RANGE = (1, 10)

# Slack IDs: C for a public channel, G for a private one, S for a user group.
CHANNEL_ID = re.compile(r"[CG][A-Z0-9_]{2,}")
_USER_GROUP_ID = re.compile(r"S[A-Z0-9_]{2,}")


@dataclass
class Audience:
    """Who may use an agent and where, as the creator wrote it.

    An agent answers in the listed channels, and in a direct message to everyone
    (``dm_everyone``) or to members of the listed user groups. A guest (single or
    multi-channel) is never let in unless ``guests`` is set, and a person from
    another Slack workspace (Slack Connect) never is. A direct message is
    allowed to nobody when neither ``dm_everyone`` nor a user group is set.
    Only IDs, taken from the Slack event, are ever compared with these.
    """

    channels: list[str] = field(default_factory=list)
    dm_everyone: bool = False
    dm_user_groups: list[str] = field(default_factory=list)
    guests: bool = False


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
    # What the agent is for, read by a front door when it picks a specialist.
    description: str = ""
    # Agents this one may hand a thread to. An agent with specialists is a
    # front door.
    specialists: list[str] = field(default_factory=list)
    # None means the definition sets no audience: the legacy default applies (see
    # ``agentos.audience.effective_audience``). Published versions never change,
    # so an agent published before audiences existed keeps this.
    audience: Audience | None = None
    # An optional label the usage page groups cost by. Empty means no team.
    team: str = ""
    # How many writes (each an approval card) one model reply may start; the rest of its
    # write calls get an error result.
    max_writes_per_turn: int = DEFAULT_MAX_WRITES_PER_TURN


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
    # None for a tenant installed from Slack that has no agent yet: the gateway
    # answers its messages with the fixed not-set-up reply until an admin sets one.
    default_agent: str | None
    # The Notion workspace this tenant's webhook events must come from.
    notion_workspace_id: str | None = None
    # The connected workspace's name, for display; set by Connect Notion, not the YAML file.
    notion_workspace_name: str | None = None
    secrets: TenantSecrets = field(default_factory=TenantSecrets)
    # Slack channel ID to the agent that answers mentions there.
    channels: dict[str, str] = field(default_factory=dict)
    # Slack user IDs of the tenant's admins. They come from this file only, so
    # nobody can remove or demote one through the app.
    admins: tuple[str, ...] = ()


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


AGENT_ID_PATTERN = re.compile(r"[a-z][a-z0-9-]{1,39}")


def check_agent_id(agent_id: object) -> str:
    """The agent ID, or ``ConfigError``.

    Lower-case ASCII letters, digits and ``-``, starting with a letter, 2 to 40 characters.
    A look-alike letter, a ``/`` that the console cannot route, or a ``:`` that makes an
    audit key ambiguous cannot get in.
    """
    if not isinstance(agent_id, str) or not AGENT_ID_PATTERN.fullmatch(agent_id):
        raise ConfigError(
            f"agent ID {agent_id!r} is invalid: use 2 to 40 lowercase ASCII letters, digits "
            "and hyphens, starting with a letter"
        )
    return agent_id


# What an agent definition may contain, at each level. A key that is not listed is an
# error, so a misspelt ``audiance`` is refused instead of reading as "no audience".
_AGENT_KEYS = frozenset(
    {
        "version",
        "name",
        "description",
        "system_prompt",
        "max_steps",
        "max_writes_per_turn",
        "grants",
        "follow_up",
        "approval",
        "specialists",
        "audience",
        "team",
    }
)
_GRANT_KEYS = frozenset({"notion"})
_NOTION_KEYS = frozenset({"databases", "pages"})
_DATABASE_KEYS = frozenset({"id", "name", "properties", "property_ids"})
_PAGE_KEYS = frozenset({"id", "name"})

# Inclusive bounds on the numbers a definition sets.
MAX_STEPS_RANGE = (1, 50)
_FOLLOW_UP_RANGES = {
    "poll_interval_minutes": (1, 1440),
    "stale_after_days": (1, 365),
    "max_watch_days": (1, MAX_WATCH_DAYS),
}
_APPROVAL_RANGES = {
    "expires_after_hours": (1, MAX_APPROVAL_EXPIRES_HOURS),
    "reminder_after_hours": (1, MAX_APPROVAL_EXPIRES_HOURS),
}


def _mapping(where: str, raw: Any, allowed: frozenset[str]) -> dict[str, Any]:
    """``raw`` as a mapping with only the allowed keys; None is an empty mapping."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a mapping")
    for key in raw:
        if key not in allowed:
            raise ConfigError(f"{where}: unknown field '{key}'")
    return raw


def _text(where: str, key: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}: '{key}' must be non-empty text")
    return value


def _whole(where: str, key: str, value: Any, low: int, high: int) -> int:
    """An int (a bool is not one) within the bounds."""
    if type(value) is not int or not low <= value <= high:
        raise ConfigError(f"{where}: '{key}' must be a whole number from {low} to {high}")
    return value


def _require(mapping: dict[str, Any], key: str, where: str) -> Any:
    if not isinstance(mapping, dict) or key not in mapping or mapping[key] in (None, ""):
        raise ConfigError(f"{where}: missing required field '{key}'")
    return mapping[key]


def _list_of(where: str, raw: Any) -> list[Any]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError(f"{where}: expected a list")
    return raw


def _parse_database_grant(
    agent_id: str, raw: dict[str, Any], index: int = 0
) -> NotionDatabaseGrant:
    where = f"agent '{agent_id}' grants.notion.databases[{index}]"
    _mapping(where, raw, _DATABASE_KEYS)
    db_id = _text(where, "id", _require(raw, "id", where))
    name = _text(where, "name", _require(raw, "name", where))
    props = raw.get("properties")
    if not isinstance(props, dict):
        raise ConfigError(f"agent '{agent_id}' grant '{name}': missing required field 'properties'")
    pwhere = f"agent '{agent_id}' grant '{name}' properties"
    _mapping(pwhere, props, frozenset(_PROPERTY_FIELDS))
    values: dict[str, Any] = {}
    for key, attr in _PROPERTY_FIELDS.items():
        if key not in props or props[key] in (None, "", []):
            raise ConfigError(
                f"agent '{agent_id}' grant '{name}': missing property mapping 'properties.{key}'"
            )
        if key == "done_values":
            done = props[key]
            if not isinstance(done, list) or not all(isinstance(v, str) and v for v in done):
                raise ConfigError(f"{pwhere}: 'done_values' must be a list of status names")
            values[attr] = done
        else:
            values[attr] = _text(pwhere, key, props[key])
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
            id_values[_PROPERTY_ID_FIELDS[key]] = _text(
                f"agent '{agent_id}' grant '{name}' property_ids", key, value
            )
    return NotionDatabaseGrant(
        id=db_id,
        name=name,
        title_prop=values["title_prop"],
        status_prop=values["status_prop"],
        done_values=list(values["done_values"]),
        requested_by_prop=values["requested_by_prop"],
        request_id_prop=values["request_id_prop"],
        **id_values,
    )


def _parse_page_grant(agent_id: str, raw: dict[str, Any], index: int = 0) -> NotionPageGrant:
    where = f"agent '{agent_id}' grants.notion.pages[{index}]"
    _mapping(where, raw, _PAGE_KEYS)
    return NotionPageGrant(
        id=_text(where, "id", _require(raw, "id", where)),
        name=_text(where, "name", _require(raw, "name", where)),
    )


def _parse_policy(where: str, raw: Any, ranges: dict[str, tuple[int, int]], cls: type) -> Any:
    block = _mapping(where, raw, frozenset(ranges))
    return cls(**{k: _whole(where, k, v, *ranges[k]) for k, v in block.items()})


def parse_agent(agent_id: str, raw: dict[str, Any], version: int | None = None) -> AgentVersion:
    """Parse and validate one agent block, from the YAML file or from the database.

    Both sources go through this function, so an agent loaded from the
    database is checked exactly like one read from YAML. ``version`` overrides
    the block's own ``version`` field: the database assigns version numbers, so
    a stored definition has none. A key the definition may not have, a value of the
    wrong type and a number out of range are each an error that names where.
    """
    check_agent_id(agent_id)
    where = f"agent '{agent_id}'"
    raw = _mapping(where, raw, _AGENT_KEYS)
    grants_raw = _mapping(f"{where} grants", raw.get("grants"), _GRANT_KEYS)
    notion = _mapping(f"{where} grants.notion", grants_raw.get("notion"), _NOTION_KEYS)
    databases = [
        _parse_database_grant(agent_id, db, i)
        for i, db in enumerate(
            _list_of(f"{where} grants.notion.databases", notion.get("databases"))
        )
    ]
    pages = [
        _parse_page_grant(agent_id, pg, i)
        for i, pg in enumerate(_list_of(f"{where} grants.notion.pages", notion.get("pages")))
    ]
    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ConfigError(f"{where}: 'description' must be text")
    given_version = version if version is not None else _require(raw, "version", where)
    return AgentVersion(
        agent_id=agent_id,
        version=_whole(where, "version", given_version, 1, 2**31 - 1),
        name=_text(where, "name", _require(raw, "name", where)),
        system_prompt=_text(where, "system_prompt", _require(raw, "system_prompt", where)),
        max_steps=_whole(where, "max_steps", _require(raw, "max_steps", where), *MAX_STEPS_RANGE),
        grants=Grants(notion_databases=databases, notion_pages=pages),
        follow_up=_parse_policy(
            f"{where} follow_up", raw.get("follow_up"), _FOLLOW_UP_RANGES, FollowUpPolicy
        ),
        approval=_parse_policy(
            f"{where} approval", raw.get("approval"), _APPROVAL_RANGES, ApprovalPolicy
        ),
        description=(description or "").strip(),
        specialists=_parse_specialists(where, raw.get("specialists")),
        audience=_parse_audience(where, raw.get("audience")),
        team=_parse_team(where, raw.get("team")),
        max_writes_per_turn=_parse_max_writes(where, raw.get("max_writes_per_turn")),
    )


def agent_definition(agent: AgentVersion) -> dict[str, Any]:
    """The agent as one YAML-shaped block, without ``version``.

    The inverse of ``parse_agent``: ``parse_agent(id, agent_definition(a),
    a.version) == a``. The database stores this shape.
    """
    databases = []
    for db in agent.grants.notion_databases:
        entry: dict[str, Any] = {
            "id": db.id,
            "name": db.name,
            "properties": {
                "title": db.title_prop,
                "status": db.status_prop,
                "done_values": list(db.done_values),
                "requested_by": db.requested_by_prop,
                "request_id": db.request_id_prop,
            },
        }
        property_ids = {
            role: getattr(db, attr)
            for role, attr in _PROPERTY_ID_FIELDS.items()
            if getattr(db, attr)
        }
        if property_ids:
            entry["property_ids"] = property_ids
        databases.append(entry)
    optional: dict[str, Any] = {"team": agent.team} if agent.team else {}
    if agent.max_writes_per_turn != DEFAULT_MAX_WRITES_PER_TURN:
        optional["max_writes_per_turn"] = agent.max_writes_per_turn
    definition: dict[str, Any] = {
        "name": agent.name,
        "description": agent.description,
        "system_prompt": agent.system_prompt,
        "max_steps": agent.max_steps,
        "grants": {
            "notion": {
                "databases": databases,
                "pages": [{"id": pg.id, "name": pg.name} for pg in agent.grants.notion_pages],
            }
        },
        "follow_up": dataclasses.asdict(agent.follow_up),
        "approval": dataclasses.asdict(agent.approval),
        "specialists": list(agent.specialists),
        **optional,
    }
    if agent.audience is not None:
        definition["audience"] = _audience_definition(agent.audience)
    return definition


def _audience_definition(audience: Audience) -> dict[str, Any]:
    block: dict[str, Any] = {"channels": list(audience.channels)}
    if audience.dm_everyone:
        block["dm"] = "everyone"
    elif audience.dm_user_groups:
        block["dm"] = {"user_groups": list(audience.dm_user_groups)}
    block["guests"] = audience.guests
    return block


def _id_list(where: str, key: str, raw: Any, pattern: re.Pattern[str], what: str) -> list[str]:
    if not isinstance(raw, list):
        raise ConfigError(f"{where}: '{key}' must be a list of {what}s")
    for item in raw:
        if not isinstance(item, str) or not pattern.fullmatch(item):
            raise ConfigError(f"{where}: {item!r} is not a {what}")
    if len(set(raw)) != len(raw):
        raise ConfigError(f"{where}: '{key}' lists an ID more than once")
    return list(raw)


def _parse_audience(agent_where: str, raw: Any) -> Audience | None:
    """The ``audience`` block, or None when the definition has none."""
    if raw is None:
        return None
    where = f"{agent_where} audience"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: expected a mapping with 'channels', 'dm' and 'guests'")
    unknown = set(raw) - {"channels", "dm", "guests"}
    if unknown:
        raise ConfigError(f"{where}: unknown field '{min(unknown)}'")
    channels = _id_list(
        f"{where}", "channels", raw.get("channels", []), CHANNEL_ID, "Slack channel ID"
    )
    dm = raw.get("dm")
    dm_everyone = False
    groups: list[str] = []
    if dm == "everyone":
        dm_everyone = True
    elif isinstance(dm, dict):
        extra = set(dm) - {"user_groups"}
        if extra:
            raise ConfigError(f"{where}: unknown field 'dm.{min(extra)}'")
        groups = _id_list(
            f"{where} dm",
            "user_groups",
            dm.get("user_groups"),
            _USER_GROUP_ID,
            "Slack user group ID",
        )
        if not groups:
            raise ConfigError(f"{where}: dm 'user_groups' must list at least one user group")
    elif dm is not None:
        raise ConfigError(f"{where}: 'dm' must be 'everyone' or a mapping with 'user_groups'")
    guests = raw.get("guests", False)
    if not isinstance(guests, bool):
        raise ConfigError(f"{where}: 'guests' must be true or false")
    if not channels and not dm_everyone and not groups:
        raise ConfigError(f"{where}: reaches nobody; list 'channels' or set 'dm'")
    return Audience(channels, dm_everyone, groups, guests)


def channel_audience_problems(
    channels: dict[str, str], agents: dict[str, AgentVersion]
) -> list[str]:
    """Channels mapped to an agent whose audience leaves them out.

    The tenant file routes a channel to an agent. If that agent's audience does not
    list the channel, every mention there would be refused, so the set is refused
    instead. An agent without an audience has the legacy default and is never listed.
    """
    problems = []
    for channel, agent_id in channels.items():
        agent = agents.get(agent_id)
        if (
            agent is not None
            and agent.audience is not None
            and channel not in agent.audience.channels
        ):
            problems.append(
                f"channel '{channel}' is mapped to agent '{agent_id}', whose audience does "
                "not include that channel"
            )
    return problems


MAX_TEAM_LENGTH = 64


def _parse_max_writes(where: str, raw: Any) -> int:
    """The ``max_writes_per_turn`` limit: a whole number from 1 to 10, or absent for the default."""
    if raw is None:
        return DEFAULT_MAX_WRITES_PER_TURN
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ConfigError(f"{where}: 'max_writes_per_turn' must be a whole number")
    if not MAX_WRITES_PER_TURN_RANGE[0] <= raw <= MAX_WRITES_PER_TURN_RANGE[1]:
        low, high = MAX_WRITES_PER_TURN_RANGE
        raise ConfigError(f"{where}: 'max_writes_per_turn' must be from {low} to {high}")
    return raw


def _parse_team(where: str, raw: Any) -> str:
    """The ``team`` label: a short single-line string, or absent."""
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise ConfigError(f"{where}: 'team' must be a text label such as IT")
    team = raw.strip()
    if not team:
        raise ConfigError(f"{where}: 'team' must not be empty; leave it out for no team")
    if len(team) > MAX_TEAM_LENGTH:
        raise ConfigError(f"{where}: 'team' must be at most {MAX_TEAM_LENGTH} characters")
    if not team.isprintable():
        raise ConfigError(f"{where}: 'team' must be one line of printable characters")
    return team


def _parse_specialists(where: str, raw: Any) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(s, str) for s in raw):
        raise ConfigError(f"{where}: 'specialists' must be a list of agent IDs")
    return list(raw)


def check_front_doors(agents: dict[str, AgentVersion]) -> None:
    """Check a set of agents: specialists exist, are not front doors, and are listed once.

    Runs on the YAML agents when a file is read, and on a tenant's published
    set whenever that set would change.
    """
    for agent_id, agent in agents.items():
        for specialist_id in agent.specialists:
            where = f"agent '{agent_id}' specialist '{specialist_id}'"
            if agent.specialists.count(specialist_id) > 1:
                raise ConfigError(f"{where}: listed more than once")
            specialist = agents.get(specialist_id)
            if specialist is None:
                raise ConfigError(f"{where}: is not a defined agent")
            if specialist_id == agent_id:
                raise ConfigError(f"{where}: an agent cannot be its own specialist")
            if not specialist.description:
                raise ConfigError(f"{where}: needs a non-empty 'description'")
            if specialist.specialists:
                raise ConfigError(
                    f"{where}: is itself a front door, and a specialist cannot hand off"
                )


def _parse_channels(raw: Any, agents: dict[str, AgentVersion]) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("agents config: 'channels' must map a Slack channel ID to an agent ID")
    channels = {str(channel): str(agent_id) for channel, agent_id in raw.items()}
    for channel, agent_id in channels.items():
        if agent_id not in agents:
            raise ConfigError(f"channel '{channel}': agent '{agent_id}' is not defined")
    return channels


def _parse_admins(tenant_id: str, raw: Any) -> tuple[str, ...]:
    """The ``admins`` list of Slack user IDs. Absent means the tenant has none."""
    if raw is None:
        return ()
    where = f"tenant '{tenant_id}' admins"
    if not isinstance(raw, list):
        raise ConfigError(f"{where}: expected a list of Slack user IDs")
    admins: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip() or item != item.strip():
            raise ConfigError(f"{where}: each entry must be a non-empty Slack user ID string")
        if item in admins:
            raise ConfigError(f"{where}: '{item}' is listed more than once")
        admins.append(item)
    return tuple(admins)


def parse_agents_config(raw: dict[str, Any]) -> AgentsConfig:
    if not isinstance(raw, dict):
        raise ConfigError("agents config: expected a mapping at the top level")
    agents_raw = _require(raw, "agents", "agents config")
    if not isinstance(agents_raw, dict):
        raise ConfigError("agents config: 'agents' must map each agent ID to its definition")
    agents = {aid: parse_agent(aid, a or {}) for aid, a in agents_raw.items()}
    default_agent = str(_require(raw, "default_agent", "agents config"))
    if default_agent not in agents:
        raise ConfigError(f"agents config: default_agent '{default_agent}' is not defined")
    tenant_id = str(_require(raw, "tenant_id", "agents config"))
    if not TENANT_ID_PATTERN.fullmatch(tenant_id):
        raise ConfigError(
            f"tenant_id {tenant_id!r} is invalid: use 1 to 63 lowercase letters, digits and "
            "hyphens, starting with a letter or digit"
        )
    check_front_doors(agents)
    channels = _parse_channels(raw.get("channels"), agents)
    if problems := channel_audience_problems(channels, agents):
        raise ConfigError(f"agents config: {problems[0]}")
    workspace = raw.get("notion_workspace_id")
    return AgentsConfig(
        tenant_id=tenant_id,
        slack_team_id=str(_require(raw, "slack_team_id", "agents config")),
        agents=agents,
        default_agent=default_agent,
        notion_workspace_id=str(workspace) if workspace not in (None, "") else None,
        secrets=_parse_secrets(tenant_id, raw.get("secrets")),
        channels=channels,
        admins=_parse_admins(tenant_id, raw.get("admins")),
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
