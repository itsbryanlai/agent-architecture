"""The Temporal worker: runs both workflows and every activity.

Real clients are built here, at startup. Each one needs its secret from the
environment and fails with a message naming the missing variable (never its
value) if it is not exported.
"""

from __future__ import annotations

import asyncio
import logging

from psycopg_pool import AsyncConnectionPool
from temporalio.client import Client
from temporalio.worker import Worker

from agentos.activities import AgentActivities
from agentos.config import (
    PROVIDER_QWEN,
    TASK_QUEUE,
    Settings,
    load_agents_config,
    load_settings,
)
from agentos.connectors.notion import RealNotionAPI
from agentos.llm import AnthropicLLMClient, LLMClient, QwenLLMClient
from agentos.slack_client import RealSlackPoster
from agentos.stores import PostgresHistoryStore, PostgresIdempotencyStore, PostgresTaskLinkStore
from agentos.workflows.conversation import ConversationWorkflow
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


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("agentos.worker")
    settings = load_settings()
    agents = load_agents_config(settings.config_path)
    llm = build_llm(settings)
    # Provider and model name only; never a key.
    log.info("llm provider=%s model=%s", settings.llm_provider, settings.model)

    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        activities = AgentActivities(
            settings=settings,
            agents=agents,
            history=PostgresHistoryStore(pool),
            idem=PostgresIdempotencyStore(pool),
            links=PostgresTaskLinkStore(pool),
            llm=llm,
            slack=RealSlackPoster(settings.require("slack_bot_token")),
            notion_api=RealNotionAPI(settings.require("notion_token")),
        )
        client = await Client.connect(settings.temporal_address)
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow, TaskWorkflow],
            activities=activities.all(),
        )
        log.info("tenant_id=%s worker started on %s", agents.tenant_id, TASK_QUEUE)
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
