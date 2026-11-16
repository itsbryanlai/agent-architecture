"""One Temporal time-skipping server per session, a task queue per test.

``TemporalScope`` hands a test a client that sends every workflow to that test's own task queue
and, when the test ends, terminates the workflows it started, so a later test never meets a
leftover run or its timers. A test that needs a server of its own (the encrypting data converter,
a worker restart) starts one itself and says so in a comment.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client, Interceptor, OutboundInterceptor, StartWorkflowInput
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos.config import TASK_QUEUE
from tests.helpers.child_starts import ChildStarts


@dataclass
class TemporalScope:
    """The shared server as one test sees it."""

    server: WorkflowEnvironment
    task_queue: str = field(default_factory=lambda: f"{TASK_QUEUE}-{uuid.uuid4().hex[:12]}")
    started: list[str] = field(default_factory=list)
    client: Client = None  # type: ignore[assignment]
    # A worker given ``interceptors=[scope.children]`` reports the child workflows it starts, so
    # a task that outlives its conversation is ended with the test as well.
    children: ChildStarts = field(default_factory=ChildStarts)

    def __post_init__(self) -> None:
        scope = self

        class Outbound(OutboundInterceptor):
            async def start_workflow(self, input: StartWorkflowInput):
                if input.task_queue == TASK_QUEUE:
                    input.task_queue = scope.task_queue
                scope.started.append(input.id)
                return await super().start_workflow(input)

        class Redirect(Interceptor):
            def intercept_client(self, next: OutboundInterceptor) -> OutboundInterceptor:
                return Outbound(next)

        config = self.server.client.config()
        config["interceptors"] = [*config.get("interceptors", []), Redirect()]
        self.client = Client(**config)

    async def sleep(self, duration) -> None:
        await self.server.sleep(duration)

    @contextmanager
    def auto_time_skipping_disabled(self):
        with self.server.auto_time_skipping_disabled():
            yield

    async def finish(self) -> None:
        """Terminate every workflow this test started, children included, that is still running."""
        for workflow_id in dict.fromkeys([*self.started, *self.children.started]):
            with suppress(RPCError):  # already closed: nothing to clean
                await self.client.get_workflow_handle(workflow_id).terminate()


_server: WorkflowEnvironment | None = None


def set_server(server: WorkflowEnvironment | None) -> None:
    """Called by the session fixture that starts the shared server."""
    global _server
    _server = server


def current_server() -> WorkflowEnvironment | None:
    return _server


def new_scope() -> TemporalScope:
    """A scope on the shared server, for a helper that cannot ask for a fixture."""
    assert _server is not None, "the session's Temporal server is not running"
    return TemporalScope(_server)


@workflow.defn
class _Canary:
    @workflow.run
    async def run(self) -> str:
        await workflow.sleep(timedelta(hours=1))
        return "skipped"


async def server_still_skips_time(server: WorkflowEnvironment) -> bool:
    """Whether the shared server still skips time. The test server counts time-skipping locks
    globally, so one awaited result that was cancelled at the wrong moment leaves it unable to
    skip for every test after; a one-hour timer that does not fire shows that."""
    queue = f"canary-{uuid.uuid4().hex[:12]}"
    async with Worker(server.client, task_queue=queue, workflows=[_Canary]):
        try:
            await asyncio.wait_for(
                server.client.execute_workflow(_Canary.run, id=f"canary-{queue}", task_queue=queue),
                timeout=5,
            )
        except TimeoutError:
            with suppress(RPCError):
                await server.client.get_workflow_handle(f"canary-{queue}").terminate()
            return False
    return True


async def replace_server_if_stuck() -> None:
    """Swap in a fresh server when the shared one can no longer skip time."""
    global _server
    if _server is not None and not await server_still_skips_time(_server):
        old, _server = _server, await WorkflowEnvironment.start_time_skipping()
        await old.shutdown()
