"""Per-tenant payload encryption: the codec, the key files and a real workflow."""

from __future__ import annotations

import base64
import os
import stat

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    WorkflowSerializationContext,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agentos import ids
from agentos.activities import AgentActivities
from agentos.config import TASK_QUEUE, Settings
from agentos.crypto_keys import (
    LocalKeyStore,
    TenantKeyError,
    check_keys,
    create_key,
    startup_key_failure,
)
from agentos.llm import ScriptedLLMClient
from agentos.models import ConversationRef, InboundMessage, LLMResponse
from agentos.payload_codec import NoTenantForPayload, TenantPayloadCodec
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.temporal_client import tenant_data_converter
from agentos.tenant_keys import main as tenant_keys_main
from agentos.tenants import TenantRegistry, runtime_for
from agentos.workflows.conversation import TURN_FAILED, ConversationWorkflow
from agentos.workflows.task import TaskWorkflow
from tests.conftest import ALICE, BOB, TENANT, eventually, make_fake_notion

CHANNEL = "C1"
THREAD = "1700000000.000100"
SECRET_TEXT = "please order me a zebra-striped monitor"


@pytest.fixture
def keys(tmp_path) -> LocalKeyStore:
    key_dir = tmp_path / "keys"
    for tenant in ("acme", "globex"):
        create_key(key_dir, tenant)
    return LocalKeyStore(key_dir)


def bound(codec: TenantPayloadCodec, workflow_id: str) -> TenantPayloadCodec:
    return codec.with_context(
        WorkflowSerializationContext(namespace="default", workflow_id=workflow_id)
    )


def json_payload(text: str) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=f'"{text}"'.encode())


async def test_round_trip_per_tenant(keys):
    codec = TenantPayloadCodec(keys)
    acme = bound(codec, ids.conversation_id("acme", "C1", "1.0"))
    globex = bound(codec, ids.task_id("globex", "u1"))
    original = json_payload("hello")

    (acme_1,) = await acme.encode([original])
    (acme_2,) = await acme.encode([original])
    (globex_1,) = await globex.encode([original])

    for encrypted, tenant in ((acme_1, "acme"), (acme_2, "acme"), (globex_1, "globex")):
        assert encrypted.metadata["encoding"] == b"binary/encrypted"
        assert encrypted.metadata["encryption-key-id"] == tenant.encode()
        assert b"hello" not in encrypted.data
    assert acme_1.data != acme_2.data  # a fresh nonce per call
    assert acme_1.data != globex_1.data  # a different key per tenant
    # Any codec decodes from the tag alone, bound or not.
    assert await codec.decode([acme_1, globex_1]) == [original, original]
    assert await acme.decode([acme_2]) == [original]


async def test_wrong_tenant_key_cannot_decode(keys):
    acme = bound(TenantPayloadCodec(keys), ids.conversation_id("acme", "C1", "1.0"))
    (encrypted,) = await acme.encode([json_payload("secret")])

    forged = Payload.FromString(encrypted.SerializeToString())
    forged.metadata["encryption-key-id"] = b"globex"  # decrypt with globex's key
    with pytest.raises(Exception) as err:
        await TenantPayloadCodec(keys).decode([forged])
    assert "secret" not in str(err.value)


async def test_encode_without_tenant_fails_closed(keys):
    codec = TenantPayloadCodec(keys)
    with pytest.raises(NoTenantForPayload):
        await codec.encode([json_payload("x")])
    # A context that carries no workflow ID, or no tenant-prefixed one, is unbound too.
    no_workflow = codec.with_context(
        ActivitySerializationContext(
            namespace="default",
            activity_id="a",
            activity_type="t",
            activity_task_queue="q",
            workflow_id=None,
            workflow_type=None,
            is_local=False,
        )
    )
    with pytest.raises(NoTenantForPayload):
        await no_workflow.encode([json_payload("x")])
    with pytest.raises(NoTenantForPayload):
        await bound(codec, "no-tenant-prefix").encode([json_payload("x")])


async def test_plaintext_payload_still_decodes(keys):
    plain = json_payload("from before encryption")
    assert await TenantPayloadCodec(keys).decode([plain]) == [plain]


def test_key_file_permissions_enforced(tmp_path):
    key_dir = tmp_path / "keys"
    path = create_key(key_dir, "acme")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(key_dir.stat().st_mode) == 0o700
    assert len(LocalKeyStore(key_dir).key("acme")) == 32

    os.chmod(path, 0o644)
    with pytest.raises(TenantKeyError) as wide:
        LocalKeyStore(key_dir).key("acme")
    assert str(path) in str(wide.value)
    assert "acme" in str(wide.value)

    with pytest.raises(TenantKeyError) as missing:
        LocalKeyStore(key_dir).key("globex")
    message = str(missing.value)
    assert str(key_dir / "globex.key") in message
    assert "globex" in message

    # A tenant ID is never turned into a path.
    with pytest.raises(TenantKeyError):
        LocalKeyStore(key_dir).key("../acme")


def test_key_never_in_repr_or_error(tmp_path):
    key_dir = tmp_path / "keys"
    path = create_key(key_dir, "acme")
    encoded = path.read_text().strip()
    store = LocalKeyStore(key_dir)
    store.key("acme")
    assert encoded not in repr(store)
    path.write_text(base64.b64encode(b"short").decode())
    with pytest.raises(TenantKeyError) as err:
        LocalKeyStore(key_dir).key("acme")
    assert "short" not in str(err.value)


def test_create_key_refuses_overwrite(tmp_path, capsys):
    key_dir = tmp_path / "keys"
    assert tenant_keys_main(["create", "acme"], key_dir=str(key_dir)) == 0
    first = (key_dir / "acme.key").read_bytes()
    assert capsys.readouterr().out.strip() == str(key_dir / "acme.key")

    assert tenant_keys_main(["create", "acme"], key_dir=str(key_dir)) == 1
    captured = capsys.readouterr()
    assert "already exists" in captured.err
    assert (key_dir / "acme.key").read_bytes() == first


def all_payloads(message) -> list[Payload]:
    """Every Payload message anywhere inside a protobuf message."""
    found: list[Payload] = []

    def walk(m) -> None:
        if isinstance(m, Payload):
            found.append(m)
            return
        for field, value in m.ListFields():
            if field.type != field.TYPE_MESSAGE:
                continue
            if field.is_repeated:
                items = value.values() if field.message_type.GetOptions().map_entry else value
                for item in items:
                    if hasattr(item, "ListFields"):
                        walk(item)
            else:
                walk(value)

    walk(message)
    return found


async def test_workflow_history_is_encrypted(tmp_path, agents):
    key_dir = tmp_path / "keys"
    create_key(key_dir, TENANT)
    converter: DataConverter = tenant_data_converter(LocalKeyStore(key_dir))

    llm = ScriptedLLMClient()
    llm.queue(LLMResponse(text="Done, nothing to file.", tool_calls=[], stop_reason="end_turn"))
    slack = FakeSlack(emails={ALICE: "alice@example.com", BOB: "bob@example.com"})
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(agents, slack, make_fake_notion())}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
    )
    env = await WorkflowEnvironment.start_time_skipping(data_converter=converter)
    try:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow, TaskWorkflow],
            activities=acts.all(),
        ):
            workflow_id = ids.conversation_id(TENANT, CHANNEL, THREAD)
            handle = await env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
                id=workflow_id,
                task_queue=TASK_QUEUE,
                start_signal="message",
                start_signal_args=[
                    InboundMessage(
                        event_id="Ev1",
                        user_id=ALICE,
                        text=SECRET_TEXT,
                        channel=CHANNEL,
                        thread_ts=THREAD,
                        channel_kind="channel",
                    )
                ],
            )
            await eventually(lambda: slack.updates())
            history = await handle.fetch_history()
    finally:
        await env.shutdown()

    payloads = [p for event in history.events for p in all_payloads(event)]
    assert payloads, "expected input and result payloads in the history"
    for payload in payloads:
        assert payload.metadata["encoding"] == b"binary/encrypted"
        assert payload.metadata["encryption-key-id"] == TENANT.encode()
    raw = b"".join(event.SerializeToString() for event in history.events)
    assert SECRET_TEXT.encode() not in raw
    assert b"Done, nothing to file." not in raw


def test_startup_refuses_without_tenant_keys(tmp_path):
    key_dir = tmp_path / "keys"
    create_key(key_dir, "acme")
    store = LocalKeyStore(key_dir)

    assert check_keys(store, ["acme"]) == []
    assert startup_key_failure(store, ["acme"]) is None

    problems = check_keys(store, ["acme", "globex"])
    assert len(problems) == 1
    assert "globex" in problems[0] and str(key_dir / "globex.key") in problems[0]
    failure = startup_key_failure(store, ["acme", "globex"])
    assert failure and "\n" not in failure
    assert "globex" in failure and "agentos.tenant_keys create" in failure

    os.chmod(key_dir / "acme.key", 0o640)
    bad = check_keys(LocalKeyStore(key_dir), ["acme"])
    assert len(bad) == 1 and "acme" in bad[0]


class MarkerError(Exception):
    """Non-retryable (status 401) with a distinctive message."""

    status_code = 401


FAILURE_MARKER = "zebra-marker-failure-text"


async def test_failure_messages_are_encrypted(tmp_path, agents):
    key_dir = tmp_path / "keys"
    create_key(key_dir, TENANT)
    converter = tenant_data_converter(LocalKeyStore(key_dir))

    llm = ScriptedLLMClient()
    llm.queue(MarkerError(FAILURE_MARKER))
    slack = FakeSlack(emails={ALICE: "alice@example.com"})
    acts = AgentActivities(
        settings=Settings(),
        registry=TenantRegistry({TENANT: runtime_for(agents, slack, make_fake_notion())}),
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=llm,
        tool_outputs=InMemoryToolOutputStore(),
    )
    env = await WorkflowEnvironment.start_time_skipping(data_converter=converter)
    try:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ConversationWorkflow, TaskWorkflow],
            activities=acts.all(),
        ):
            handle = await env.client.start_workflow(
                ConversationWorkflow.run,
                ConversationRef(TENANT, "it-helper", CHANNEL, THREAD),
                id=ids.conversation_id(TENANT, CHANNEL, THREAD),
                task_queue=TASK_QUEUE,
                start_signal="message",
                start_signal_args=[
                    InboundMessage(
                        event_id="Ev1",
                        user_id=ALICE,
                        text="hello",
                        channel=CHANNEL,
                        thread_ts=THREAD,
                        channel_kind="channel",
                    )
                ],
            )
            await eventually(lambda: slack.updates())
            history = await handle.fetch_history()
    finally:
        await env.shutdown()

    assert slack.updates()[0]["text"] == TURN_FAILED  # recovery path unchanged
    raw = b"".join(event.SerializeToString() for event in history.events)
    assert any(e.HasField("activity_task_failed_event_attributes") for e in history.events)
    assert FAILURE_MARKER.encode() not in raw
