"""Many tenants in one deployment: config, routing, and per-tenant clients."""

import hashlib
import hmac
import json
import secrets as token_source

import pytest
import yaml
from fastapi.testclient import TestClient
from slack_sdk.signature import SignatureVerifier
from temporalio.exceptions import ApplicationError

from agentos import ids
from agentos.activities import AgentActivities
from agentos.agent_store import seeded_agent_store
from agentos.audit import InMemoryAuditStore
from agentos.config import (
    ConfigError,
    Settings,
    TenantsConfig,
    load_tenants_config,
    parse_agents_config,
)
from agentos.connectors.notion import FakeNotionAPI
from agentos.gateway.app import create_app
from agentos.models import ConversationRef, InboundMessage, LLMResponse, ToolCall, TurnContext
from agentos.slack_client import FakeSlack
from agentos.stores import (
    InMemoryHistoryStore,
    InMemoryIdempotencyStore,
    InMemoryTaskLinkStore,
    InMemoryToolOutputStore,
)
from agentos.tenants import TenantRegistry, build_registry, runtime_for
from agentos.workflows.conversation import ConversationWorkflow
from tests.conftest import (
    ACME_WORKSPACE,
    ALICE,
    GLOBEX,
    GLOBEX_TEAM,
    GLOBEX_WORKSPACE,
    TEAM,
    TENANT,
    agents_dict,
    eventually,
    tenant_dict,
    title_prop,
    two_tenant_dicts,
)
from tests.test_gateway import FakeTemporalClient, im_event, post_event

PAGE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def write_tenants(directory, *raws: dict):
    for raw in raws:
        (directory / f"{raw['tenant_id']}.yaml").write_text(yaml.safe_dump(raw))


def test_directory_config_loads_each_tenant(tmp_path):
    write_tenants(tmp_path, *two_tenant_dicts())

    tenants = load_tenants_config(tmp_path)

    assert sorted(tenants.tenants) == [TENANT, GLOBEX]
    assert tenants.by_slack_team(TEAM) == TENANT
    assert tenants.by_slack_team(GLOBEX_TEAM) == GLOBEX
    assert tenants.by_slack_team("T_NOBODY") is None
    assert tenants.only_tenant() is None
    assert tenants.tenants[GLOBEX].notion_workspace_id == GLOBEX_WORKSPACE
    assert tenants.tenants[GLOBEX].secrets.notion_token_env == "NOTION_TOKEN_GLOBEX"


def test_single_file_config_still_loads(tmp_path):
    path = tmp_path / "agents.yaml"
    path.write_text(yaml.safe_dump(agents_dict()))

    tenants = load_tenants_config(path)

    assert tenants.only_tenant() == TENANT
    secrets = tenants.tenants[TENANT].secrets
    assert (secrets.slack_bot_token_env, secrets.notion_token_env) == (
        "SLACK_BOT_TOKEN",
        "NOTION_TOKEN",
    )
    assert secrets.notion_webhook_token_env == "NOTION_WEBHOOK_VERIFICATION_TOKEN"
    assert tenants.tenants[TENANT].notion_workspace_id is None


def test_duplicate_team_or_secret_names_rejected():
    acme, globex = two_tenant_dicts()

    same_team = {**globex, "slack_team_id": TEAM}
    with pytest.raises(ConfigError, match="slack_team_id") as err:
        TenantsConfig.of(parse_agents_config(acme), parse_agents_config(same_team))
    assert "acme" in str(err.value) and "globex" in str(err.value)

    same_secret = {
        **globex,
        "secrets": {**globex["secrets"], "notion_token_env": "NOTION_TOKEN_ACME"},
    }
    with pytest.raises(ConfigError, match="NOTION_TOKEN_ACME") as err:
        TenantsConfig.of(parse_agents_config(acme), parse_agents_config(same_secret))
    assert "acme" in str(err.value) and "globex" in str(err.value)

    no_secrets_a = {k: v for k, v in acme.items() if k != "secrets"}
    no_secrets_b = {k: v for k, v in globex.items() if k != "secrets"}
    with pytest.raises(ConfigError, match="default") as err:
        TenantsConfig.of(parse_agents_config(no_secrets_a), parse_agents_config(no_secrets_b))
    assert "acme" in str(err.value) and "globex" in str(err.value)

    same_workspace = {**globex, "notion_workspace_id": ACME_WORKSPACE}
    with pytest.raises(ConfigError, match="notion_workspace_id"):
        TenantsConfig.of(parse_agents_config(acme), parse_agents_config(same_workspace))

    with pytest.raises(ConfigError, match="tenant_id"):
        TenantsConfig.of(parse_agents_config(acme), parse_agents_config(acme))


def test_tenant_id_format_enforced(tmp_path):
    for bad in ("a:b", "a/b", "Acme", "", "a" * 64, "-acme"):
        path = tmp_path / "t.yaml"
        path.write_text(yaml.safe_dump({**agents_dict(), "tenant_id": bad}))
        with pytest.raises(ConfigError, match="tenant_id") as err:
            load_tenants_config(path)
        assert str(path) in str(err.value)
    for good in ("acme", "globex-2", "a" * 63):
        parse_agents_config({**agents_dict(), "tenant_id": good})


def test_webhook_path_with_bad_tenant_id_is_404(tmp_path, webhook_tokens):
    temporal = FakeTemporalClient()
    settings = Settings(notion_token_dir=str(tmp_path / "sub"))
    tenants = TenantsConfig.of(*(parse_agents_config(r) for r in two_tenant_dicts()))
    client = TestClient(
        create_app(settings, tenants, temporal, InMemoryTaskLinkStore(), SignatureVerifier("x"))
    )
    for bad in ("ACME", "a..b", "%2e%2e", "a_b", "a" * 64):
        resp = client.post(f"/notion/webhook/{bad}", json={"verification_token": "t"})
        assert resp.status_code == 404
    assert not (tmp_path / "sub").exists()
    assert temporal.signals == []


def test_missing_tenant_secret_names_variable(monkeypatch):
    tenants = TenantsConfig.of(*(parse_agents_config(r) for r in two_tenant_dicts()))
    values = {
        "SLACK_BOT_TOKEN_ACME": token_source.token_hex(8),
        "NOTION_TOKEN_ACME": token_source.token_hex(8),
        "SLACK_BOT_TOKEN_GLOBEX": token_source.token_hex(8),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("NOTION_TOKEN_GLOBEX", raising=False)

    with pytest.raises(ConfigError) as err:
        build_registry(tenants, Settings(), lambda t: FakeSlack(), lambda t: FakeNotionAPI())

    message = str(err.value)
    assert "NOTION_TOKEN_GLOBEX" in message and "globex" in message
    assert not any(value in message for value in values.values())


def test_registry_builds_each_tenants_clients_from_its_own_variables(monkeypatch):
    tenants = TenantsConfig.of(*(parse_agents_config(r) for r in two_tenant_dicts()))
    expected = {}
    for suffix in ("ACME", "GLOBEX"):
        for kind in ("SLACK_BOT_TOKEN", "NOTION_TOKEN"):
            expected[f"{kind}_{suffix}"] = f"{kind}-{suffix}-value"
            monkeypatch.setenv(f"{kind}_{suffix}", expected[f"{kind}_{suffix}"])
    seen = []

    registry = build_registry(
        tenants,
        Settings(),
        lambda value: seen.append(value) or FakeSlack(),
        lambda value: seen.append(value) or FakeNotionAPI(),
    )

    assert registry.tenants() == [TENANT, GLOBEX]
    assert sorted(seen) == sorted(expected.values())
    assert registry.get(TENANT).slack is not registry.get(GLOBEX).slack
    assert not any(value in repr(registry.get(TENANT)) for value in expected.values())


# --- the gateway ------------------------------------------------------------


def two_tenant_gateway(temporal, links=None, slack_secret="x"):
    tenants = TenantsConfig.of(*(parse_agents_config(r) for r in two_tenant_dicts()))
    app = create_app(
        Settings(),
        tenants,
        temporal,
        links or InMemoryTaskLinkStore(),
        SignatureVerifier(slack_secret),
        agents=seeded_agent_store(*tenants.tenants.values()),
    )
    return TestClient(app)


def test_events_route_to_their_tenant():
    secret = token_source.token_hex(16)
    temporal = FakeTemporalClient()
    client = two_tenant_gateway(temporal, slack_secret=secret)

    assert post_event(client, secret, im_event()).status_code == 200
    globex_event = im_event()
    globex_event["team_id"] = GLOBEX_TEAM
    assert post_event(client, secret, globex_event).status_code == 200

    acme_call, globex_call = temporal.signal_with_starts
    assert acme_call["arg"] == ConversationRef(TENANT, "it-helper", "D123", "1700000000.000200")
    assert acme_call["id"].startswith(f"{TENANT}:")
    assert globex_call["arg"] == ConversationRef(GLOBEX, "it-helper", "D123", "1700000000.000200")
    assert globex_call["id"] == ids.conversation_id(GLOBEX, "D123", "1700000000.000200")


def test_unknown_workspace_event_dropped(caplog):
    secret = token_source.token_hex(16)
    temporal = FakeTemporalClient()
    client = two_tenant_gateway(temporal, slack_secret=secret)
    event = im_event()
    event["team_id"] = "T_STRANGER"

    resp = post_event(client, secret, event)

    assert resp.status_code == 200
    assert temporal.signal_with_starts == []
    assert "ignored event from an unknown Slack workspace" in caplog.text
    assert "T_STRANGER" in caplog.text


# --- Notion webhooks ---------------------------------------------------------


def sign(body: bytes, token: str) -> str:
    return "sha256=" + hmac.new(token.encode(), body, hashlib.sha256).hexdigest()


def notion_post(client, path, payload, token):
    body = json.dumps(payload).encode()
    return client.post(
        path,
        content=body,
        headers={"Content-Type": "application/json", "X-Notion-Signature": sign(body, token)},
    )


def page_event(workspace_id: str | None = GLOBEX_WORKSPACE) -> dict:
    event = {"id": "evt-1", "type": "page.properties_updated"}
    event["entity"] = {"id": PAGE_ID, "type": "page"}
    if workspace_id is not None:
        event["workspace_id"] = workspace_id
    return event


@pytest.fixture
def webhook_tokens(monkeypatch):
    tokens = {"ACME": token_source.token_hex(16), "GLOBEX": token_source.token_hex(16)}
    for suffix, value in tokens.items():
        monkeypatch.setenv(f"NOTION_WEBHOOK_VERIFICATION_TOKEN_{suffix}", value)
    return tokens


def test_notion_webhook_per_tenant_path(webhook_tokens):
    temporal = FakeTemporalClient()
    links = InMemoryTaskLinkStore()
    links.links[(GLOBEX, PAGE_ID.replace("-", ""))] = f"{GLOBEX}:task:t1"
    client = two_tenant_gateway(temporal, links)

    ok = notion_post(client, f"/notion/webhook/{GLOBEX}", page_event(), webhook_tokens["GLOBEX"])
    assert ok.status_code == 200
    assert [(wf, name) for wf, name, _ in temporal.signals] == [
        (f"{GLOBEX}:task:t1", "external_update")
    ]

    temporal.signals.clear()
    wrong = notion_post(client, f"/notion/webhook/{GLOBEX}", page_event(), webhook_tokens["ACME"])
    assert wrong.status_code == 401
    assert temporal.signals == []

    missing = notion_post(client, "/notion/webhook/nobody", page_event(), webhook_tokens["GLOBEX"])
    assert missing.status_code == 404


def test_notion_webhook_workspace_mismatch_ignored(webhook_tokens, caplog):
    temporal = FakeTemporalClient()
    links = InMemoryTaskLinkStore()
    links.links[(GLOBEX, PAGE_ID.replace("-", ""))] = f"{GLOBEX}:task:t1"
    client = two_tenant_gateway(temporal, links)

    for workspace in (ACME_WORKSPACE, None):
        resp = notion_post(
            client,
            f"/notion/webhook/{GLOBEX}",
            page_event(workspace),
            webhook_tokens["GLOBEX"],
        )
        assert resp.status_code == 200
    assert temporal.signals == []
    assert temporal.signal_with_starts == []
    assert "another workspace" in caplog.text


def test_legacy_webhook_path_only_with_one_tenant(monkeypatch, webhook_tokens):
    one = TenantsConfig.of(parse_agents_config(tenant_dict(GLOBEX, GLOBEX_TEAM, "ws-g", "GLOBEX")))
    temporal = FakeTemporalClient()
    links = InMemoryTaskLinkStore()
    links.links[(GLOBEX, PAGE_ID.replace("-", ""))] = f"{GLOBEX}:task:t1"
    client = TestClient(
        create_app(
            Settings(),
            one,
            temporal,
            links,
            SignatureVerifier("x"),
            agents=seeded_agent_store(*one.tenants.values()),
        )
    )

    event = page_event("ws-g")
    assert (
        notion_post(client, "/notion/webhook", event, webhook_tokens["GLOBEX"]).status_code == 200
    )
    assert len(temporal.signals) == 1

    two = two_tenant_gateway(FakeTemporalClient(), links)
    resp = notion_post(two, "/notion/webhook", page_event(), webhook_tokens["GLOBEX"])
    assert resp.status_code == 404


# --- activities and workflows ---------------------------------------------------


async def test_turn_uses_its_tenants_clients_only(two_tenants):
    h = two_tenants
    h.notions[GLOBEX].add_page(
        "11111111-2222-3333-4444-555555555555", {"Name": title_prop("Globex laptop")}
    )
    h.llm.queue(
        LLMResponse(
            text=None,
            tool_calls=[ToolCall(id="tu_1", name="search_items", input={"query": "laptop"})],
            stop_reason="tool_use",
        ),
        LLMResponse(text="One request.", tool_calls=[], stop_reason="end_turn"),
    )
    msg = InboundMessage("Ev1", ALICE, "laptops?", "C1", "1700000000.000100", "channel")

    await h.client.start_workflow(
        ConversationWorkflow.run,
        ConversationRef(GLOBEX, "it-helper", "C1", "1700000000.000100"),
        id=ids.conversation_id(GLOBEX, "C1", "1700000000.000100"),
        task_queue="agentos-main",
        start_signal="message",
        start_signal_args=[msg],
    )
    await eventually(lambda: h.slacks[GLOBEX].updates())

    assert h.slacks[GLOBEX].updates()[0]["text"] == "One request."
    assert h.notions[GLOBEX].count("query_database") == 1
    assert h.slacks[TENANT].calls == []
    assert h.notions[TENANT].calls == []


async def test_unknown_tenant_activity_fails_non_retryable():
    config = parse_agents_config(agents_dict())
    registry = TenantRegistry({TENANT: runtime_for(config, FakeSlack(), FakeNotionAPI())})
    acts = AgentActivities(
        settings=Settings(),
        registry=registry,
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(config),
    )
    ctx = TurnContext(
        tenant_id="initech",
        agent_id="it-helper",
        agent_version=1,
        requester_user_id=ALICE,
        channel="C1",
        thread_ts="1.1",
        channel_kind="channel",
    )

    with pytest.raises(ApplicationError) as caught:
        await acts.post_placeholder(ctx, "key")
    assert caught.value.non_retryable is True
    with pytest.raises(ApplicationError) as caught:
        await acts.load_agent("initech", "it-helper")
    assert caught.value.non_retryable is True


def make_acts(*configs):
    registry = TenantRegistry(
        {c.tenant_id: runtime_for(c, FakeSlack(), FakeNotionAPI()) for c in configs}
    )
    return AgentActivities(
        settings=Settings(),
        registry=registry,
        history=InMemoryHistoryStore(),
        idem=InMemoryIdempotencyStore(),
        links=InMemoryTaskLinkStore(),
        llm=None,
        tool_outputs=InMemoryToolOutputStore(),
        audit=InMemoryAuditStore(),
        agents=seeded_agent_store(*configs),
    )


async def test_load_agent_one_argument_compat():
    acme, globex = (parse_agents_config(r) for r in two_tenant_dicts())

    single = make_acts(acme)
    assert (await single.load_agent("it-helper")).agent_id == "it-helper"
    assert (await single.load_agent(TENANT, "it-helper")).agent_id == "it-helper"

    several = make_acts(acme, globex)
    with pytest.raises(ApplicationError) as caught:
        await several.load_agent("it-helper")
    assert caught.value.non_retryable is True
    assert (await several.load_agent(GLOBEX, "it-helper")).agent_id == "it-helper"
