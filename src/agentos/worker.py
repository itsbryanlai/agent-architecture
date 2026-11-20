"""The Temporal worker: runs both workflows and every activity.

Real clients are built here, at startup, one Slack client and one Notion
client for each tenant. Both tokens are resolved from the tenant's encrypted
credential row first, else the environment variable its config names; a missing
Slack token fails with a message naming the tenant and the variable (never its
value). A tenant with no Notion token is served Slack-only. A token connected
in the console while the worker runs reaches it on the next directory refresh.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
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
from agentos.approval_policy import PostgresApprovalPolicyStore
from agentos.audience_pauses import PostgresPauseStore
from agentos.audience_watch import AudienceWatchActivities
from agentos.audit import PostgresAuditStore
from agentos.budgets import BudgetGuard, PostgresBudgetStore
from agentos.config import (
    EMBED_DIMENSIONS,
    PROVIDER_QWEN,
    TASK_QUEUE,
    TEST_TASK_QUEUE,
    AgentsConfig,
    ConfigError,
    Settings,
    TenantsConfig,
    load_settings,
    validate_worker_capacity,
)
from agentos.connectors.drive import DriveReader
from agentos.connectors.google_iam import GoogleError, accounts_from_settings
from agentos.connectors.notion import NotionAPI, RealNotionAPI
from agentos.content_store import PostgresContentStore
from agentos.creator_access import CreatorAccessActivities, PostgresCreatorAccessStore
from agentos.credentials import PostgresCredentialStore, TenantCredentials
from agentos.crypto_keys import (
    KeyStore,
    TenantKeyError,
    build_key_store,
    check_keys,
    start_key_refresh,
    startup_key_failure,
)
from agentos.drive_connections import PostgresDriveConnectionStore
from agentos.grant_picks import PostgresGrantPickStore
from agentos.knowledge.drive_indexing import DriveIndexActivities
from agentos.knowledge.embedder import Embedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.llm import AnthropicLLMClient, LLMClient, QwenLLMClient
from agentos.members import PostgresMemberStore
from agentos.retention import PostgresRetentionStore, RetentionActivities
from agentos.schedules import refresh_schedules_once, run_schedule_refresh
from agentos.schema_version import require_current_schema
from agentos.slack_client import RealSlackPoster, SlackPoster
from agentos.stores import (
    PostgresExternalWriteStore,
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
)
from agentos.switches import PostgresSwitchStore
from agentos.tasks import PostgresTaskStore
from agentos.temporal_client import build_temporal_client
from agentos.tenant_store import PostgresTenantStore, TenantDirectory, TenantSource, startup_tenants
from agentos.tenants import RegistrySync, TenantRegistry, build_registry
from agentos.test_sets import PostgresTestSetStore
from agentos.threads import PostgresOwnerPingStore, PostgresThreadStore
from agentos.unit_of_work import PostgresUnitOfWork
from agentos.usage import PostgresUsageStore, Prices, UsageRecorder, load_prices, utcnow
from agentos.workflows.audience_watch import AudienceWatchWorkflow
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.creator_access import CreatorAccessWorkflow
from agentos.workflows.drive_indexing import DriveIndexingWorkflow
from agentos.workflows.indexing import IndexingWorkflow
from agentos.workflows.page_refresh import PageRefreshWorkflow
from agentos.workflows.retention import RetentionWorkflow
from agentos.workflows.task import TaskWorkflow
from agentos.workflows.test_run import TestRunWorkflow


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
    """Build the embedder, only when some published agent of any tenant has a knowledge grant
    (a Notion page or a Drive folder/file).

    ``tenants`` carries each tenant's published agents (``published_tenants``).

    The embedder is the platform's own, shared by every tenant. Agents without
    knowledge need no embedding settings, so the Anthropic provider keeps
    working without them. Returns None when no agent has a knowledge grant. When
    ``AGENTOS_EMBED_BASE_URL`` is set, that endpoint is used and no Qwen setting is needed
    (a local server needs no key, but the SDK requires a non-empty string). Otherwise the
    Qwen settings are used.
    """
    if not any(
        agent.grants.notion_pages or agent.grants.drive
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


async def build_tenant_clients(
    tenants: TenantsConfig,
    directory: TenantSource,
    settings: Settings,
    credentials: TenantCredentials,
    keys: KeyStore,
    log: logging.Logger,
    slack_factory: Callable[[str], SlackPoster] = RealSlackPoster,
    notion_factory: Callable[[str], NotionAPI] = RealNotionAPI,
    drive: Callable[[AgentsConfig], Awaitable[DriveReader | None]] | None = None,
) -> tuple[TenantRegistry, RegistrySync]:
    """The worker's tenant clients at start-up, and the sync that keeps them current.

    The registry carries the directory's stored-but-disabled marks from the start, so a
    restart ends a disabled tenant's pending work cleanly before the first sync pass.
    Both resolve each tenant's tokens through ``TenantCredentials``: the encrypted
    credential row first, else the environment variable the tenant's settings name.
    A tenant connected only in the console is served with no variable exported, and
    a token connected or replaced while the worker runs rebuilds that tenant's
    client on the next sync, as a Slack reinstall does. The health check reads its
    Notion client from this registry, so it checks with the connected token too.
    ``drive`` resolves each tenant's Drive reader from its connection row, so a tenant
    admitted after start-up and a rebuilt runtime both carry it.
    """
    registry = await build_registry(
        tenants,
        settings,
        slack_factory,
        notion_factory,
        slack_token=credentials.slack_bot_token,
        notion_token=credentials.notion_token,
        drive=drive,
    )
    # The first sync pass is an interval away; until it lands, a stored-but-disabled
    # tenant would look not-yet-loaded and its pending work would retry, not end cleanly.
    registry.set_disabled(set(await directory.disabled_tenants()))
    sync = RegistrySync(
        directory,
        registry,
        slack_factory,
        notion_factory,
        admit=lambda config: admit_new_tenant(config, keys, log),
        slack_token=credentials.slack_bot_token,
        notion_token=credentials.notion_token,
        on_drop=credentials.forget,
        drive=drive,
    )
    return registry, sync


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
        # The deployment's Google client (None when Drive is not set up). A malformed key
        # fails at start-up, never at the first read, like the console's connect.
        try:
            google = accounts_from_settings(
                settings.google_project_id, settings.google_service_account_key
            )
        except GoogleError as err:
            raise SystemExit(str(err)) from err
        drive_connections = PostgresDriveConnectionStore(pool)
        price_failure = startup_price_failure(
            prices,
            settings.prices_path,
            [settings.model] + ([embedder.model] if embedder is not None else []),
        )
        if price_failure:
            raise SystemExit(price_failure)

        # The tenant's Drive reader comes from its connection row, resolved like its Notion
        # token: a tenant admitted after start-up and a rebuilt runtime both carry it.
        # Indexing and search read the row fresh besides, so a connect while the worker
        # runs reaches them on the next run.
        async def drive_reader(config: AgentsConfig) -> DriveReader | None:
            if google is None:
                return None
            row = await drive_connections.get(config.tenant_id)
            return DriveReader(google, row.service_account_email) if row is not None else None

        registry, tenant_sync = await build_tenant_clients(
            tenants, directory, settings, credentials, keys, log, drive=drive_reader
        )
        audit_store = PostgresAuditStore(pool)
        idempotency = PostgresIdempotencyStore(pool)
        members = PostgresMemberStore(pool)
        health = PostgresAgentHealthStore(pool)
        creator_access = PostgresCreatorAccessStore(pool)
        health_checker = AgentHealthChecker(
            registry,
            agent_store,
            health,
            audit_store,
            idempotency,
            members,
            uow=PostgresUnitOfWork(pool),
            creator_access=creator_access,
        )
        # A broken grant disables its agent with the reason; the worker starts and
        # serves every other agent and tenant. Platform-wide problems above still exit.
        # One tenant's check failing never skips the rest: it is logged, and the worker
        # starts; the health refresh checks that tenant's agents again on its cadence.
        for tenant_id in registry.tenants():
            log.info("tenant_id=%s checking grants", tenant_id)
            try:
                await health_checker.check_tenant(tenant_id, published.tenants[tenant_id].agents)
            except Exception as err:  # noqa: BLE001 - one tenant never stops start-up
                log.error(
                    "tenant_id=%s grant check failed error_type=%s", tenant_id, type(err).__name__
                )
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
            tasks=PostgresTaskStore(pool),
            policies=PostgresApprovalPolicyStore(pool),
            test_sets=PostgresTestSetStore(pool),
            google=google,
            drive_connections=drive_connections,
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
            CreatorAccessWorkflow,
        ]
        all_activities = (
            activities.all()
            + watch.all()
            + RetentionActivities(PostgresRetentionStore(pool)).all()
            + CreatorAccessActivities(
                registry,
                agent_store,
                PostgresGrantPickStore(pool),
                credentials,
                creator_access,
                health_checker,
            ).all()
        )
        if embedder is not None:
            # Indexing runs only where some published agent has a knowledge grant.
            workflows += [IndexingWorkflow, PageRefreshWorkflow, DriveIndexingWorkflow]
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
            all_activities += DriveIndexActivities(
                registry,
                knowledge,
                embedder,
                agent_store,
                google,
                drive_connections,
                usage,
                switches,
                budget_guard,
                idempotency,
            ).all()
        client = await build_temporal_client(settings, keys)
        # Every served tenant's nightly schedules (index, retention, creator-access) are
        # ensured here and kept current by the refresh below, so a tenant added by
        # "Add to Slack" needs no operator command. One tenant's failure is logged with
        # its ID and retried on the next pass; it never stops start-up or serving.
        ensured_schedules: dict[str, tuple[tuple[str, str], ...]] = {}
        await refresh_schedules_once(client, registry, agent_store, ensured_schedules)
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=workflows,
            activities=all_activities,
            max_concurrent_activities=settings.max_concurrent_activities,
        )
        # Test runs poll their own queue, so a burst of them never parks the turns and
        # tasks of the main queue. The same activities serve both; only TestRunWorkflow
        # is registered here, and it calls no write activity.
        test_worker = Worker(
            client,
            task_queue=TEST_TASK_QUEUE,
            workflows=[TestRunWorkflow],
            activities=all_activities,
            max_concurrent_activities=settings.max_concurrent_activities,
        )
        log.info(
            "tenants=%s worker started on %s and %s",
            ",".join(registry.tenants()),
            TASK_QUEUE,
            TEST_TASK_QUEUE,
        )
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
        # A newly admitted tenant, a reinstall or a changed page grant gets its schedules
        # on this cadence too; an unchanged tenant causes no schedule call (fingerprint).
        schedule_refresher = asyncio.create_task(
            run_schedule_refresh(
                client, registry, agent_store, ensured_schedules, TENANT_SYNC_SECONDS
            )
        )
        try:
            await asyncio.gather(worker.run(), test_worker.run())
        finally:
            refresher.cancel()
            health_refresher.cancel()
            schedule_refresher.cancel()
            stopped = [health_refresher, schedule_refresher]
            if key_refresh is not None:
                key_refresh.cancel()
                stopped.append(key_refresh)
            # Gathered before the pool closes, whether or not a key refresh runs.
            await asyncio.gather(*stopped, return_exceptions=True)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
