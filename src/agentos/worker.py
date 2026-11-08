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
from agentos.config import TASK_QUEUE, load_agents_config, load_settings
from agentos.connectors.notion import RealNotionAPI
from agentos.llm import AnthropicLLMClient
from agentos.slack_client import RealSlackPoster
from agentos.stores import PostgresHistoryStore, PostgresIdempotencyStore, PostgresTaskLinkStore
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    agents = load_agents_config(settings.config_path)

    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    try:
        activities = AgentActivities(
            settings=settings,
            agents=agents,
            history=PostgresHistoryStore(pool),
            idem=PostgresIdempotencyStore(pool),
            links=PostgresTaskLinkStore(pool),
            llm=AnthropicLLMClient(settings.require("anthropic_api_key"), settings.model),
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
        logging.getLogger("agentos.worker").info(
            "tenant_id=%s worker started on %s", agents.tenant_id, TASK_QUEUE
        )
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
