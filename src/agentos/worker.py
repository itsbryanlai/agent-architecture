"""The Temporal worker: runs both workflows and every activity.

Real clients are built here, at startup, one Slack client and one Notion
client for each tenant. The Slack token is resolved from the tenant's encrypted
credential row first, else the environment variable its config names; a missing
one fails with a message naming the tenant and the variable (never its value).
A tenant with no Notion token is served Slack-only.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from urllib.parse import urlparse

from psycopg_pool import AsyncConnectionPool
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.agent_health import (
    AgentHealthChecker,
    PostgresAgentHealthStore,
    run_health_refresh,
)
from agentos.agent_store import (
    PostgresAgentStore,
    published_tenants,
    startup_agent_failure,
)
from agentos.audience_pauses import PostgresPauseStore
from agentos.audience_watch import AudienceWatchActivities
from agentos.audit import PostgresAuditStore
from agentos.budgets import BudgetGuard, PostgresBudgetStore
from agentos.config import (
    EMBED_DIMENSIONS,
    PROVIDER_QWEN,
    TASK_QUEUE,
    AgentsConfig,
    ConfigError,
    Settings,
    TenantsConfig,
    load_settings,
    validate_worker_capacity,
)
from agentos.connectors.notion import RealNotionAPI
from agentos.content_store import PostgresContentStore
from agentos.credentials import PostgresCredentialStore, TenantCredentials
from agentos.crypto_keys import (
    KeyStore,
    TenantKeyError,
    build_key_store,
    check_keys,
    start_key_refresh,
    startup_key_failure,
)
from agentos.knowledge.embedder import Embedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.llm import AnthropicLLMClient, LLMClient, QwenLLMClient
from agentos.members import PostgresMemberStore
from agentos.retention import PostgresRetentionStore, RetentionActivities
from agentos.schema_version import require_current_schema
from agentos.slack_client import RealSlackPoster
from agentos.stores import (
    PostgresExternalWriteStore,
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
)
from agentos.switches import PostgresSwitchStore
from agentos.temporal_client import build_temporal_client
from agentos.tenant_store import PostgresTenantStore, TenantDirectory, startup_tenants
from agentos.tenants import RegistrySync, build_registry
from agentos.threads import PostgresOwnerPingStore, PostgresThreadStore
from agentos.unit_of_work import PostgresUnitOfWork
from agentos.usage import PostgresUsageStore, Prices, UsageRecorder, load_prices, utcnow
from agentos.workflows.audience_watch import AudienceWatchWorkflow
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
from agentos.workflows.retention import RetentionWorkflow
from agentos.workflows.task import TaskWorkflow


def build_llm(settings: Settings) -> LLMClient:
    """Build the model client for the provider chosen in settings.

    Anthropic is the default; Qwen is used only when the provider setting says
    so. Only the chosen provider's key is required.
    """
    if settings.llm_provider == PROVIDER_QWEN:
        return QwenLLMClient(
            settings.require("qwen_api_key"),
            settings.require("qwen_base_url"),
            settings.model,
            settings.qwen_thinking,
        )
    return AnthropicLLMClient(settings.require("anthropic_api_key"), settings.model)


def build_embedder(settings: Settings, tenants: TenantsConfig, http_client=None) -> Embedder | None:
    """Build the embedder, only when some published agent of any tenant has a Notion page grant.

    ``tenants`` carries each tenant's published agents (``published_tenants``).

    The embedder is the platform's own, shared by every tenant. Agents without
    knowledge need no embedding settings, so the Anthropic provider keeps
    working without them. Returns None when no agent has a page grant. When ``AGENTOS_EMBED_BASE_URL`` is set, that endpoint is used and no
    Qwen setting is needed (a local server needs no key, but the SDK requires a
    non-empty string). Otherwise the Qwen settings are used.
    """
    if not any(
        agent.grants.notion_pages
        for config in tenants.tenants.values()
        for agent in config.agents.values()
    ):
        return None
    if settings.embed_base_url:
        return OpenAICompatibleEmbedder(
            settings.embed_api_key or "not-needed",
            settings.embed_base_url,
            settings.embed_model,
            EMBED_DIMENSIONS,
            http_client=http_client,
        )
    return OpenAICompatibleEmbedder(
        settings.require("qwen_api_key"),
        settings.require("qwen_base_url"),
        settings.embed_model,
        EMBED_DIMENSIONS,
        http_client=http_client,
    )


async def warn_pages_without_model(store, tenant_ids, logger) -> int:
    """Log a warning per tenant that has indexed pages with no recorded embedding model.

    Such pages predate the model column. Search skips them until one indexing run
    (`index_knowledge --wait`) reads them again. Returns the total.
    """
    total = 0
    for tenant_id in tenant_ids:
        count = await store.count_pages_without_model(tenant_id)
        if count:
            logger.warning(
                "tenant_id=%s knowledge pages=%d have no embedding model and are not searched; "
                "run `uv run python -m agentos.index_knowledge --wait` once",
                tenant_id,
                count,
            )
        total += count
    return total


def startup_price_failure(
    prices: Prices, prices_path: str, models: list[str], at: datetime | None = None
) -> str | None:
    """A message naming each model that has no price entry in effect today, else None.

    A model with no price is recorded with no cost, and a budget then stops its scope
    because the spend cannot be measured, so the worker refuses to start without one.
    """
    when = at or utcnow()
    missing = [m for m in dict.fromkeys(models) if prices.entry_for(m, when) is None]
    if not missing:
        return None
    names = ", ".join(f"'{m}'" for m in missing)
    return (
        f"no price for model {names} in {prices_path} on {when.date()}. Add an entry for "
        "each (its header says how, and how to match a dated name) and start again."
    )


# How often the worker re-reads the tenants table. Short, because an activity for a
# tenant the worker does not know yet retries until a pass picks the tenant up.
TENANT_SYNC_SECONDS = 5.0


async def admit_new_tenant(
    config: AgentsConfig,
    keys: KeyStore,
    log: logging.Logger,
) -> str | None:
    """The start-up work for a tenant that appears while the worker runs.

    Its encryption key must be usable, or it is not served until it is. Its published
    agents' grants are checked by the health refresh once the tenant is served; a
    broken grant disables that agent, never the tenant or the worker.
    """
    problems = check_keys(keys, [config.tenant_id])
    if problems:
        return "; ".join(problems)
    return None


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("agentos.worker")
    settings = load_settings()
    try:
        validate_worker_capacity(settings)
    except ConfigError as err:
        raise SystemExit(str(err)) from err
    llm = build_llm(settings)
    prices = load_prices(settings.prices_path)
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    try:
        await require_current_schema(pool)
        directory = TenantDirectory(PostgresTenantStore(pool))
        tenants = await startup_tenants(directory)
        try:
            keys = await build_key_store(settings, pool)
        except TenantKeyError as err:
            raise SystemExit(str(err)) from err
        key_failure = startup_key_failure(keys, tenants.tenants)
        if key_failure:
            raise SystemExit(key_failure)
        credentials = TenantCredentials(PostgresCredentialStore(pool), keys)
        agent_store = PostgresAgentStore(pool)
        agent_failure = await startup_agent_failure(agent_store, tenants)
        if agent_failure:
            raise SystemExit(agent_failure)
        published = await published_tenants(agent_store, tenants)
        embedder = build_embedder(settings, published)
        price_failure = startup_price_failure(
            prices,
            settings.prices_path,
            [settings.model] + ([embedder.model] if embedder is not None else []),
        )
        if price_failure:
            raise SystemExit(price_failure)
        registry = await build_registry(
            tenants, settings, RealSlackPoster, RealNotionAPI, credentials.slack_bot_token
        )
        audit_store = PostgresAuditStore(pool)
        idempotency = PostgresIdempotencyStore(pool)
        members = PostgresMemberStore(pool)
        health = PostgresAgentHealthStore(pool)
        health_checker = AgentHealthChecker(
            registry,
            agent_store,
            health,
            audit_store,
            idempotency,
            members,
            uow=PostgresUnitOfWork(pool),
        )
        # A broken grant disables its agent with the reason; the worker starts and
        # serves every other agent and tenant. Platform-wide problems above still exit.
        for tenant_id in registry.tenants():
            log.info("tenant_id=%s checking grants", tenant_id)
            await health_checker.check_tenant(tenant_id, published.tenants[tenant_id].agents)
        # Provider and model name only; never a key.
        log.info("llm provider=%s model=%s", settings.llm_provider, settings.model)
        if embedder is not None:
            # Host and port only; never a key or a path.
            log.info(
                "embedder model=%s dimensions=%d host=%s",
                embedder.model,
                embedder.dimensions,
                urlparse(settings.embed_base_url or settings.qwen_base_url or "").netloc,
            )
        knowledge = PostgresKnowledgeStore(pool)
        if embedder is not None:
            await warn_pages_without_model(knowledge, registry.tenants(), log)
        budget_guard = BudgetGuard(PostgresBudgetStore(pool), PostgresUsageStore(pool))
        # The guard follows each recorded call, so its cached spend never lags this process's own.
        usage = UsageRecorder(PostgresUsageStore(pool), prices, listener=budget_guard)
        log.info("prices entries=%d path=%s", len(prices.entries), settings.prices_path)
        pauses = PostgresPauseStore(pool)
        switches = PostgresSwitchStore(pool)
        activities = AgentActivities(
            settings=settings,
            registry=registry,
            history=PostgresHistoryStore(pool),
            idem=idempotency,
            links=PostgresTaskLinkStore(pool),
            llm=llm,
            tool_outputs=PostgresToolOutputStore(pool),
            audit=audit_store,
            agents=agent_store,
            knowledge=knowledge,
            embedder=embedder,
            usage=usage,
            budgets=budget_guard,
            pauses=pauses,
            switches=switches,
            external_writes=PostgresExternalWriteStore(pool),
            threads=PostgresThreadStore(pool),
            owner_pings=PostgresOwnerPingStore(pool),
            content=PostgresContentStore(pool),
            health=health,
        )
        watch = AudienceWatchActivities(
            registry,
            agent_store,
            members,
            pauses,
            audit_store,
            idempotency,
        )
        workflows: list = [
            ConversationWorkflow,
            TaskWorkflow,
            AudienceWatchWorkflow,
            RetentionWorkflow,
        ]
        all_activities = (
            activities.all() + watch.all() + RetentionActivities(PostgresRetentionStore(pool)).all()
        )
        if embedder is not None:
            # Indexing runs only where some published agent has a page grant.
            workflows += [IndexingWorkflow, PageRefreshWorkflow]
            all_activities += IndexActivities(
                registry,
                knowledge,
                embedder,
                agent_store,
                usage,
                switches,
                budget_guard,
                idempotency,
            ).all()
        client = await build_temporal_client(settings, keys)
        tenant_sync = RegistrySync(
            directory,
            registry,
            RealSlackPoster,
            RealNotionAPI,
            admit=lambda config: admit_new_tenant(config, keys, log),
            slack_token=credentials.slack_bot_token,
            on_drop=credentials.forget,
        )
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=workflows,
            activities=all_activities,
            max_concurrent_activities=settings.max_concurrent_activities,
        )
        log.info("tenants=%s worker started on %s", ",".join(registry.tenants()), TASK_QUEUE)
        # A tenant added to, changed in or disabled in the database is picked up here,
        # without a restart. Its own start-up work (the key check) runs when it first
        # appears; see ``admit_new_tenant``.
        key_refresh = start_key_refresh(keys)
        refresher = asyncio.create_task(tenant_sync.run(TENANT_SYNC_SECONDS))
        # Unavailable agents are re-checked on the same cadence, bounded per agent, and
        # a newly published version is checked at once: recovery needs no restart.
        health_refresher = asyncio.create_task(
            run_health_refresh(health_checker, registry, agent_store, health, TENANT_SYNC_SECONDS)
        )
        try:
            await worker.run()
        finally:
            refresher.cancel()
            health_refresher.cancel()
            if key_refresh is not None:
                key_refresh.cancel()
                await asyncio.gather(key_refresh, health_refresher, return_exceptions=True)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
