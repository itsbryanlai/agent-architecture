"""Whole flows under the production encrypting converter: every context decodes what it encoded.

The payload codec binds each payload to its workflow ID. These tests run the paths that
cross workflows (child start, signals, queries, continue-as-new, schedules) so a context
that decodes under a different ID than it encoded under fails here, not in production.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.api.common.v1 import Payload
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import TASK_QUEUE, Settings
from agentos.crypto_keys import LocalKeyStore, create_key
from agentos.llm import ScriptedLLMClient
from agentos.models import (
    ApprovalDecision,
    ConversationRef,
    DraftView,
    EditRequest,
    InboundMessage,
    LLMResponse,
    ToolCall,
)
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.temporal_client import tenant_data_converter
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, DB_ID, TENANT, eventually, make_fake_notion
from tests.test_payload_codec import all_payloads, is_sdk_patch_record

CHANNEL = "C_GENERAL"
THREAD = "1700000000.000100"


@workflow.defn(name="HopWorkflow", sandboxed=False)
class HopWorkflow:
    """Continues as new once, carrying a value, like the conversation's long-thread hop."""

    @workflow.run
    async def run(self, hops_left: int, carried: dict) -> dict:
        if hops_left:
            workflow.continue_as_new(args=[hops_left - 1, {**carried, "hop": hops_left}])
        return carried


@pytest.fixture
async def flow(tmp_path, agents):
    create_key(tmp_path / "keys", TENANT)
    converter = tenant_data_converter(LocalKeyStore(tmp_path / "keys"))
    env = await WorkflowEnvironment.start_time_skipping(data_converter=converter)
    llm = ScriptedLLMClient()
    slack = FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"})
    notion = make_fake_notion()
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(agents, slack, notion)}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(agents),
    )
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[ConversationWorkflow, TaskWorkflow, HopWorkflow],
        activities=acts.all(),
    ):
        yield env, llm, slack, notion, acts
    await env.shutdown()


async def history_payloads(client: Client, workflow_ids: list[str]) -> list[Payload]:
    found = []
    for workflow_id in workflow_ids:
        history = await client.get_workflow_handle(workflow_id).fetch_history()
        for event in history.events:
            found += [p for p in all_payloads(event) if not is_sdk_patch_record(event, p)]
    return found


async def test_draft_approval_write_and_follow_up_all_decode_and_stay_encrypted(flow):
    """Fails if a child start, signal, query, activity or follow-up decodes under another workflow ID."""
    env, llm, slack, notion, _ = flow
    llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_c", "create_item", {"title": "New monitor", "details": "Flickers."})
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Sent you an approval card.", tool_calls=[], stop_reason="end_turn"),
    )
    conversation_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    await env.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=conversation_id,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[
            InboundMessage("Ev1", ALICE, "order a monitor", CHANNEL, THREAD, "channel")
        ],
    )
    cards = await eventually(lambda: [p for p in slack.posts() if p["blocks"]])
    actions = next(b for b in cards[0]["blocks"] if b["type"] == "actions")
    value = json.loads(actions["elements"][0]["value"])
    task = env.client.get_workflow_handle(value["task_id"])

    async def draft_ready() -> bool:
        return await task.query("state") == "awaiting_approval"

    await eventually(draft_ready, timeout=20)
    draft = await task.query("draft", result_type=DraftView)
    assert draft.requester_user_id == ALICE

    await task.signal(TaskWorkflow.decision, ApprovalDecision(draft.nonce, ALICE, True))

    async def watching() -> bool:
        return await task.query("state") == "watching"

    await eventually(watching, timeout=20)
    assert len(notion.all_pages(DB_ID)) == 1
    page_id = notion.all_pages(DB_ID)[0]["id"]
    notion.set_status(page_id, "Status", "Done")
    await env.sleep(timedelta(minutes=15))
    assert await task.result() == "done"
    assert any("was marked Done" in p["text"] for p in slack.posts())

    payloads = await history_payloads(env.client, [conversation_id, value["task_id"]])
    assert payloads
    assert {p.metadata["encoding"] for p in payloads} == {b"binary/encrypted"}


async def test_a_flow_with_its_content_in_postgres_decodes_under_the_production_converter(flow):
    """Fails if a stored-content signal, step, draft reference, edit or notice cannot be read back."""
    env, llm, slack, notion, acts = flow
    llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[
                ToolCall("tu_s", "search_items", {"query": "monitor"}),
                ToolCall("tu_c", "create_item", {"title": "New monitor", "details": "Flickers."}),
            ],
            stop_reason="tool_use",
        ),
        LLMResponse(text="Sent you an approval card.", tool_calls=[], stop_reason="end_turn"),
    )
    conversation_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
    await acts.content.put_inbound(
        TENANT,
        "Ev1",
        {"kind": "message", "agent_id": "it-helper", "user_id": ALICE, "text": "order a monitor"},
    )
    await env.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
        id=conversation_id,
        task_queue=TASK_QUEUE,
        start_signal="message",
        start_signal_args=[InboundMessage("Ev1", ALICE, "", CHANNEL, THREAD, "channel", True)],
    )
    cards = await eventually(lambda: [p for p in slack.posts() if p["blocks"]])
    value = json.loads(
        next(b for b in cards[0]["blocks"] if b["type"] == "actions")["elements"][0]["value"]
    )
    task = env.client.get_workflow_handle(value["task_id"])
    await eventually(lambda: slack.updates())

    await acts.content.put_inbound(
        TENANT,
        "edit:1",
        {
            "kind": "edit",
            "task_id": value["task_id"],
            "fields": {"title": "Edited", "details": "d"},
        },
        delivered=True,
    )
    await task.signal("edit", EditRequest(value["nonce"], ALICE, {}, "edit:1"))

    async def redrawn() -> bool:
        return (await task.query("draft", result_type=DraftView)).revision == 2

    await eventually(redrawn, timeout=20)
    draft = await task.query("draft", result_type=DraftView)
    await task.signal(TaskWorkflow.decision, ApprovalDecision(draft.nonce, ALICE, True))

    async def watching() -> bool:
        return await task.query("state") == "watching"

    await eventually(watching, timeout=20)
    assert [
        p["properties"]["Name"]["title"][0]["text"]["content"] for p in notion.all_pages(DB_ID)
    ] == ["Edited"]
    assert any("Filed: Edited" in p["text"] for p in slack.posts())
    payloads = await history_payloads(env.client, [conversation_id, value["task_id"]])
    assert {p.metadata["encoding"] for p in payloads} == {b"binary/encrypted"}


async def test_continue_as_new_hop_decodes_its_carried_arguments(flow):
    """Fails if arguments carried by continue-as-new cannot be read by the next run."""
    env, *_ = flow
    result = await env.client.execute_workflow(
        "HopWorkflow",
        args=[2, {"a": 1}],
        id=ids.index_workflow_id(TENANT, "hop"),
        task_queue=TASK_QUEUE,
    )
    assert result == {"a": 1, "hop": 1}


async def test_scheduled_run_reads_the_input_the_schedule_encoded(tmp_path):
    """Fails if a schedule's timestamped workflow ID breaks decoding of its action input."""
    create_key(tmp_path / "keys", TENANT)
    converter = tenant_data_converter(LocalKeyStore(tmp_path / "keys"))
    # The time-skipping server has no schedules; this one is the local dev server.
    async with (
        await WorkflowEnvironment.start_local(data_converter=converter) as env,
        Worker(env.client, task_queue=TASK_QUEUE, workflows=[HopWorkflow]),
    ):
        action_id = ids.index_workflow_id(TENANT, "sched")
        handle = await env.client.create_schedule(
            ids.index_schedule_id(TENANT, "sched"),
            Schedule(
                action=ScheduleActionStartWorkflow(
                    "HopWorkflow", args=[0, {"x": 1}], id=action_id, task_queue=TASK_QUEUE
                ),
                spec=ScheduleSpec(),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
            trigger_immediately=True,
        )

        async def ran() -> bool:
            return bool((await handle.describe()).info.recent_actions)

        await eventually(ran, timeout=20)
        action = (await handle.describe()).info.recent_actions[0].action
        assert action.workflow_id.startswith(action_id + "-")
        run = env.client.get_workflow_handle(action.workflow_id)
        assert await run.result() == {"x": 1}
        await handle.delete()
