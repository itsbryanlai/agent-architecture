"""The Temporal worker: runs both workflows and every activity.

Real clients are built here, at startup. Each one needs its secret from the
environment and fails with a message naming the missing variable (never its
value) if it is not exported.
"""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

from psycopg_pool import AsyncConnectionPool
from temporalio.client import Client
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.config import (
    EMBED_DIMENSIONS,
    PROVIDER_QWEN,
    TASK_QUEUE,
    AgentsConfig,
    Settings,
    load_agents_config,
    load_settings,
)
from agentos.connectors.notion import NotionAPI, RealNotionAPI, check_grant, check_page_grant
from agentos.index_knowledge import granted_roots
from agentos.knowledge.embedder import Embedder, OpenAICompatibleEmbedder
from agentos.knowledge.indexing import IndexActivities
from agentos.knowledge.store import PostgresKnowledgeStore
from agentos.llm import AnthropicLLMClient, LLMClient, QwenLLMClient
from agentos.slack_client import RealSlackPoster
from agentos.stores import (
    PostgresHistoryStore,
    PostgresIdempotencyStore,
    PostgresTaskLinkStore,
    PostgresToolOutputStore,
)
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


def build_embedder(settings: Settings, agents: AgentsConfig) -> Embedder | None:
    """Build the embedder, only when some agent has a Notion page grant.

    Agents without knowledge need no embedding settings, so the Anthropic
    provider keeps working without them. Returns None when no agent has a page
    grant. When ``AGENTOS_EMBED_BASE_URL`` is set, that endpoint is used and no
    Qwen setting is needed (a local server needs no key, but the SDK requires a
    non-empty string). Otherwise the Qwen settings are used.
    """
    if not any(agent.grants.notion_pages for agent in agents.agents.values()):
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


async def check_grants_at_startup(
    api: NotionAPI, agents: AgentsConfig, log: logging.Logger
) -> str | None:
    """Check every agent's Notion grants against the live schemas.

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


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("agentos.worker")
    settings = load_settings()
    agents = load_agents_config(settings.config_path)
    llm = build_llm(settings)
    embedder = build_embedder(settings, agents)
    notion_api = RealNotionAPI(settings.require("notion_token"))
    failure = await check_grants_at_startup(notion_api, agents, log)
    if failure:
        raise SystemExit(failure)
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

    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        knowledge = PostgresKnowledgeStore(pool)
        activities = AgentActivities(
            settings=settings,
            agents=agents,
            history=PostgresHistoryStore(pool),
            idem=PostgresIdempotencyStore(pool),
            links=PostgresTaskLinkStore(pool),
            llm=llm,
            slack=RealSlackPoster(settings.require("slack_bot_token")),
            notion_api=notion_api,
            tool_outputs=PostgresToolOutputStore(pool),
            knowledge=knowledge,
            embedder=embedder,
        )
        workflows: list = [ConversationWorkflow, TaskWorkflow]
        all_activities = activities.all()
        if embedder is not None:
            # Indexing runs only where some agent has a page grant.
            workflows += [IndexingWorkflow, PageRefreshWorkflow]
            all_activities += IndexActivities(
                notion_api, knowledge, embedder, granted_roots(agents)
            ).all()
        client = await Client.connect(settings.temporal_address)
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=workflows,
            activities=all_activities,
        )
        log.info("tenant_id=%s worker started on %s", agents.tenant_id, TASK_QUEUE)
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
