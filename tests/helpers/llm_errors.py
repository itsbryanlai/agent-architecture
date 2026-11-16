"""Helpers that build the activities with a scripted model, and a status error the SDK would raise."""

from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import Settings
from agentos.connectors.notion import FakeNotionAPI
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, runtime_for


class StatusError(Exception):
    """Stands in for an SDK APIStatusError, which carries ``status_code``."""

    def __init__(self, status_code: int):
        super().__init__(f"Error code: {status_code}")
        self.status_code = status_code


def make_activities(agents, llm):
    return AgentActivities(
        settings=Settings(),
        registry=TenantRegistry(
            {agents.tenant_id: runtime_for(agents, FakeSlack(), FakeNotionAPI())}
        ),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(agents),
    )
