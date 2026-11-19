"""A health store that flips between two steps: the race between a check and a write."""

from datetime import UTC, datetime

from agentos.agent_health import UNAVAILABLE, AgentHealth, InMemoryAgentHealthStore

FLIPPED_AT = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


class FlipsAfterReads(InMemoryAgentHealthStore):
    """Shows no row for the first reads, then unavailable: a health transition between two steps."""

    def __init__(self, quiet_reads: int):
        super().__init__()
        self.quiet_reads = quiet_reads
        self.reads = 0

    async def get(self, tenant_id, agent_id):
        self.reads += 1
        if self.reads > self.quiet_reads:
            await self.record(
                AgentHealth(
                    tenant_id, agent_id, 1, UNAVAILABLE, "grant 'IT requests': problem", FLIPPED_AT
                )
            )
        return await super().get(tenant_id, agent_id)
