"""The Temporal worker: runs both workflows and every activity.

Real clients are built here, at startup, one Slack client and one Notion
client for each tenant. Each one needs its secret from the environment
variable the tenant's config names, and fails with a message naming the tenant
and the missing variable (never its value) if it is not exported.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from urllib.parse import urlparse

from psycopg_pool import AsyncConnectionPool
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.agent_store import (
    PostgresAgentStore,
    published_tenants,
    startup_agent_failure,
    with_published_agents,
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
    Settings,
    TenantsConfig,
    load_settings,
)
from agentos.connectors.notion import NotionAPI, RealNotionAPI, check_grant, check_page_grant
from agentos.crypto_keys import LocalKeyStore, check_keys, startup_key_failure
from agentos.knowledge.embedder import Embedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.llm import AnthropicLLMClient, LLMClient, QwenLLMClient
from agentos.members import PostgresMemberStore
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
from agentos.tenants import RegistrySync, build_registry, require_tenant_secret
from agentos.usage import PostgresUsageStore, Prices, UsageRecorder, load_prices, utcnow
from agentos.workflows.audience_watch import AudienceWatchWorkflow
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
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


def build_embedder(settings: Settings, tenants: TenantsConfig) -> Embedder | None:
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
        )
    return OpenAICompatibleEmbedder(
        settings.require("qwen_api_key"),
        settings.require("qwen_base_url"),
        settings.embed_model,
        EMBED_DIMENSIONS,
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


async def check_grants_at_startup(
    api: NotionAPI, agents: AgentsConfig, log: logging.Logger
) -> str | None:
    """Check every published agent's Notion grants against the live schemas.

    ``agents`` carries the tenant's published agents (``with_published_agents``).

    Returns None when all is well, or a one-line failure summary when any grant
    has a problem (including a missing property ID); the caller must then exit
    rather than run a half-working agent. Notes, such as a property renamed in
    Notion but still found by its ID, are logged as warnings. Only the
    granted databases' schemas are read, with the agent's own token, and the
    agent never changes a schema. Logs name the tenant, agent and grant, and
    never a token or page content.
    """
    failed = 0
    missing_ids = False
    for agent in agents.agents.values():
        for grant in agent.grants.notion_databases:
            check = await check_grant(api, grant, require_ids=True)
            for note in check.notes:
                log.warning(
                    "tenant_id=%s agent=%s grant=%s: %s",
                    agents.tenant_id,
                    agent.agent_id,
                    grant.name,
                    note,
                )
            for problem in check.problems:
                log.error(
                    "tenant_id=%s agent=%s grant=%s: %s",
                    agents.tenant_id,
                    agent.agent_id,
                    grant.name,
                    problem,
                )
            failed += bool(check.problems)
            missing_ids = missing_ids or bool(grant.missing_property_ids())
        for page_grant in agent.grants.notion_pages:
            problems = await check_page_grant(api, page_grant)
            for problem in problems:
                log.error(
                    "tenant_id=%s agent=%s grant=%s: %s",
                    agents.tenant_id,
                    agent.agent_id,
                    page_grant.name,
                    problem,
                )
            failed += bool(problems)
    if not failed:
        return None
    summary = f"{failed} Notion grant(s) have problems; the worker will not start."
    if missing_ids:
        summary += (
            " Property IDs are missing: run `uv run python -m agentos.check_grants`"
            " and paste its property_ids block into the config."
        )
    return summary


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
# tenant the worker does not know yet fails for good.
TENANT_SYNC_SECONDS = 5.0


async def admit_new_tenant(
    config: AgentsConfig,
    keys: LocalKeyStore,
    agent_store: PostgresAgentStore,
    log: logging.Logger,
) -> str | None:
    """The start-up work for a tenant that appears while the worker runs.

    Its encryption key must be usable, or it is not served until it is. Its published
    agents' Notion grants are checked once and any problem is logged; a tenant that
    has just been added normally has none yet, and every publish checks its grants.
    """
    problems = check_keys(keys, [config.tenant_id])
    if problems:
        return "; ".join(problems)
    published = await with_published_agents(agent_store, config)
    if any(
        agent.grants.notion_databases or agent.grants.notion_pages
        for agent in published.agents.values()
    ):
        api = RealNotionAPI(require_tenant_secret(config, "notion_token_env"))
        log.info("tenant_id=%s checking grants", config.tenant_id)
        failure = await check_grants_at_startup(api, published, log)
        if failure:
            log.error("tenant_id=%s %s", config.tenant_id, failure)
    return None


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("agentos.worker")
    settings = load_settings()
    llm = build_llm(settings)
    prices = load_prices(settings.prices_path)
    pool = AsyncConnectionPool(settings.require("database_url"), open=False)
    await pool.open()
    try:
        await require_current_schema(pool)
        directory = TenantDirectory(PostgresTenantStore(pool))
        tenants = await startup_tenants(directory)
        keys = LocalKeyStore(settings.key_dir)
        key_failure = startup_key_failure(keys, tenants.tenants)
        if key_failure:
            raise SystemExit(key_failure)
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
        registry = build_registry(tenants, settings, RealSlackPoster, RealNotionAPI)
        failures = []
        for tenant_id in registry.tenants():
            runtime = registry.get(tenant_id)
            log.info("tenant_id=%s checking grants", tenant_id)
            failure = await check_grants_at_startup(
                runtime.notion_api, published.tenants[tenant_id], log
            )
            if failure:
                failures.append(f"tenant '{tenant_id}': {failure}")
        if failures:
            raise SystemExit("\n".join(failures))
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
        audit_store = PostgresAuditStore(pool)
        idempotency = PostgresIdempotencyStore(pool)
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
        )
        watch = AudienceWatchActivities(
            registry,
            agent_store,
            PostgresMemberStore(pool),
            pauses,
            audit_store,
            idempotency,
        )
        workflows: list = [ConversationWorkflow, TaskWorkflow, AudienceWatchWorkflow]
        all_activities = activities.all() + watch.all()
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
        client = await build_temporal_client(settings)
        tenant_sync = RegistrySync(
            directory,
            registry,
            RealSlackPoster,
            RealNotionAPI,
            admit=lambda config: admit_new_tenant(config, keys, agent_store, log),
        )
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=workflows,
            activities=all_activities,
        )
        log.info("tenants=%s worker started on %s", ",".join(registry.tenants()), TASK_QUEUE)
        # A tenant added to, changed in or disabled in the database is picked up here,
        # without a restart. Its own start-up work (key check, grant check) runs when
        # it first appears; see ``admit_new_tenant``.
        refresher = asyncio.create_task(tenant_sync.run(TENANT_SYNC_SECONDS))
        try:
            await worker.run()
        finally:
            refresher.cancel()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
