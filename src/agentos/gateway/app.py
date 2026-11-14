"""The ingress gateway.

It acknowledges every request quickly and hands the work to Temporal:

* ``POST /slack/events``: a DM or an @mention signal-with-starts the thread's
  ConversationWorkflow. Slack retries are harmless, because the workflow drops
  duplicate event IDs.
* ``POST /slack/interactivity``: Submit / Cancel clicks become a ``decision``
  signal to the task named on the card. The gateway passes on who clicked; the
  task workflow decides whether that person may approve. An Edit click opens a
  form prefilled from the task's current draft (only for the requester, only
  while the draft is editable); saving the form becomes an ``edit`` signal.
* ``POST /notion/webhook/{tenant_id}``: a page update becomes an
  ``external_update`` signal to the task watching that page. Polling covers
  anything the webhook misses. Events must carry a valid ``X-Notion-Signature``,
  made with that tenant's own verification token; the subscription handshake's
  token is written to an owner-only file, never logged. ``POST /notion/webhook``
  is an alias that works only while exactly one tenant is configured.

Every Slack request must carry a valid Slack signature. The gateway decides the
tenant from the request itself: the Slack ``team_id``, or the Notion webhook
path. It never takes the tenant from text a user wrote, and it drops (with a
log line holding IDs only) what it cannot map to a configured tenant. The
tenant is part of every workflow ID and log line.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Response
from temporalio.service import RPCError, RPCStatusCode

from agentos import ids
from agentos.agent_store import AgentStore, startup_agent_failure
from agentos.config import TASK_QUEUE, TENANT_ID_PATTERN, Settings, TenantsConfig, read_secret
from agentos.connectors.notion import normalize_id
from agentos.gateway.notion_signature import verify_notion_signature
from agentos.models import (
    EDITABLE_FIELDS,
    ApprovalDecision,
    ConversationRef,
    DraftView,
    EditRequest,
    ExternalUpdate,
    InboundMessage,
)
from agentos.slack_format import from_slack_text
from agentos.stores import TaskLinkStore

log = logging.getLogger("agentos.gateway")

LEADING_MENTION = re.compile(r"^\s*<@[A-Z0-9]+>\s*")
APPROVAL_ACTIONS = {"task_approve": True, "task_cancel": False}
# Page events that can change what a task or the knowledge index should see.
# Locking and unlocking a page change neither.
NOTION_PAGE_EVENTS = {
    "page.properties_updated",
    "page.content_updated",
    "page.created",
    "page.deleted",
    "page.undeleted",
    "page.moved",
}
EDIT_ACTION = "task_edit"
EDIT_CALLBACK = "task_edit"
FIELD_LABELS = {"title": "Title", "details": "Details", "comment": "Comment"}
MULTILINE_FIELDS = {"details", "comment"}


@dataclass
class GatewayDeps:
    settings: Settings
    tenants: TenantsConfig
    temporal_client: Any
    links: TaskLinkStore
    verifier: Any  # slack_sdk.signature.SignatureVerifier or compatible
    # A tenant's own SlackPoster, used to open the Edit form in that tenant's
    # workspace. Unset in tests that never open a form.
    slack_for: Callable[[str], Any] | None = None
    # The published agents. A Notion page event is acted on only when some
    # published agent of the tenant is granted pages. Unset in tests that never
    # send a page event.
    agents: AgentStore | None = None


def create_app(
    settings: Settings | None,
    tenants: TenantsConfig | None,
    temporal_client: Any,
    links: TaskLinkStore | None,
    verifier: Any,
    lifespan: Any = None,
    slack_for: Callable[[str], Any] | None = None,
    agents: AgentStore | None = None,
) -> FastAPI:
    """Build the gateway. Tests pass fakes; production fills deps on startup."""
    app = FastAPI(lifespan=lifespan)
    if settings is not None:
        app.state.deps = GatewayDeps(
            settings, tenants, temporal_client, links, verifier, slack_for, agents
        )

    def deps(request: Request) -> GatewayDeps:
        return request.app.state.deps

    async def verified_body(request: Request) -> bytes | None:
        body = await request.body()
        ok = deps(request).verifier.is_valid(
            body=body,
            timestamp=request.headers.get("X-Slack-Request-Timestamp"),
            signature=request.headers.get("X-Slack-Signature"),
        )
        return body if ok else None

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.post("/slack/events")
    async def slack_events(request: Request) -> Response:
        body = await verified_body(request)
        if body is None:
            return Response(status_code=401)
        payload = json.loads(body or b"{}")
        if payload.get("type") == "url_verification":
            return Response(
                content=json.dumps({"challenge": payload.get("challenge")}),
                media_type="application/json",
            )
        d = deps(request)
        if payload.get("type") != "event_callback":
            return Response(status_code=200)
        team_id = payload.get("team_id")
        tenant = d.tenants.by_slack_team(team_id)
        if tenant is None:
            log.warning("ignored event from an unknown Slack workspace team_id=%s", team_id)
            return Response(status_code=200)

        event = payload.get("event") or {}
        if event.get("bot_id") or event.get("subtype"):
            return Response(status_code=200)
        if event.get("type") == "app_mention":
            channel_kind = "channel"
        elif event.get("type") == "message" and event.get("channel_type") == "im":
            channel_kind = "im"
        else:
            return Response(status_code=200)

        channel = event["channel"]
        thread_ts = event.get("thread_ts") or event["ts"]
        msg = InboundMessage(
            event_id=payload.get("event_id") or f"{channel}:{event['ts']}",
            user_id=event["user"],
            text=from_slack_text(LEADING_MENTION.sub("", event.get("text") or "", count=1)),
            channel=channel,
            thread_ts=thread_ts,
            channel_kind=channel_kind,
        )
        workflow_id = ids.conversation_id(tenant, channel, thread_ts)
        # A running thread keeps its own agent: the start argument is ignored
        # when the workflow exists, and only the signal lands.
        config = d.tenants.tenants[tenant]
        agent_id = config.default_agent
        if channel_kind == "channel":
            agent_id = config.channels.get(channel, agent_id)
        await d.temporal_client.start_workflow(
            "ConversationWorkflow",
            ConversationRef(tenant, agent_id, channel, thread_ts),
            id=workflow_id,
            task_queue=TASK_QUEUE,
            start_signal="message",
            start_signal_args=[msg],
        )
        log.info(
            "tenant_id=%s agent_id=%s workflow_id=%s signalled message",
            tenant,
            agent_id,
            workflow_id,
        )
        return Response(status_code=200)

    @app.post("/slack/interactivity")
    async def slack_interactivity(request: Request) -> Response:
        body = await verified_body(request)
        if body is None:
            return Response(status_code=401)
        d = deps(request)
        form = parse_qs(body.decode("utf-8"))
        payload = json.loads((form.get("payload") or ["{}"])[0])
        team_id = (payload.get("team") or {}).get("id")
        tenant = d.tenants.by_slack_team(team_id)
        if tenant is None:
            log.warning("ignored interaction from an unknown Slack workspace team_id=%s", team_id)
            return Response(status_code=200)
        if payload.get("type") == "view_submission":
            return await _edit_submitted(d, tenant, payload)
        if payload.get("type") != "block_actions":
            return Response(status_code=200)
        clicker = (payload.get("user") or {}).get("id", "")
        for action in payload.get("actions") or []:
            if action.get("action_id") == EDIT_ACTION:
                await _open_edit_form(d, tenant, payload, action, clicker)
                continue
            approved = APPROVAL_ACTIONS.get(action.get("action_id"))
            if approved is None:
                continue
            try:
                value = json.loads(action.get("value") or "{}")
                task_id, nonce = value["task_id"], value["nonce"]
            except (ValueError, KeyError, TypeError):
                log.warning("tenant_id=%s ignored approval click with a bad value", tenant)
                continue
            if not task_id.startswith(f"{tenant}:"):
                log.warning("tenant_id=%s ignored approval click for another tenant", tenant)
                continue
            await _signal(
                d, tenant, task_id, "decision", ApprovalDecision(nonce, clicker, approved)
            )
        return Response(status_code=200)

    @app.post("/notion/webhook/{tenant_id}")
    async def notion_webhook(tenant_id: str, request: Request) -> Response:
        return await _notion_webhook(deps(request), tenant_id, request)

    @app.post("/notion/webhook")
    async def notion_webhook_single_tenant(request: Request) -> Response:
        # Kept so a subscription made before there were several tenants keeps
        # working: it is the only tenant's, while exactly one is configured.
        d = deps(request)
        tenant_id = d.tenants.only_tenant()
        if tenant_id is None:
            return Response(status_code=404)
        return await _notion_webhook(d, tenant_id, request)

    return app


async def _notion_webhook(d: GatewayDeps, tenant: str, request: Request) -> Response:
    """Handle one Notion webhook request for the tenant its URL names.

    The tenant comes from the path, never from the body. The signature is
    checked with that tenant's own verification token, read from the variable
    its config names, so another tenant's token cannot pass.
    """
    if not TENANT_ID_PATTERN.fullmatch(tenant):
        # Never look up, or build a file name from, a path that is not a tenant ID.
        return Response(status_code=404)
    config = d.tenants.get(tenant)
    if config is None:
        log.warning("Notion webhook for an unknown tenant tenant_id=%s", tenant)
        return Response(status_code=404)
    token_env = config.secrets.notion_webhook_token_env
    body = await request.body()
    signature = request.headers.get("X-Notion-Signature")
    try:
        payload = json.loads(body)
    except ValueError:
        return Response(status_code=400)
    if not isinstance(payload, dict):
        return Response(status_code=400)
    token = read_secret(token_env)
    if isinstance(payload.get("verification_token"), str) and "type" not in payload:
        # One-time subscription handshake, recognised by its body (a token
        # and no event type), whatever its headers. Notion's handshake can
        # carry a signature header, but it cannot be verified: its key is
        # the very token being delivered. The token goes to an owner-only
        # file, never to a log, and the operator pastes it into Notion.
        # Once a token is configured the handshake is closed, so nobody can
        # overwrite the file with a value of their own.
        if token:
            log.warning(
                "tenant_id=%s Notion webhook handshake refused: a verification token is "
                "already configured; unset %s and restart the gateway to accept a new "
                "subscription",
                tenant,
                token_env,
            )
            return Response(status_code=409)
        path = _write_token_file(
            _token_file(d.settings.notion_token_dir, tenant), str(payload["verification_token"])
        )
        log.warning(
            "tenant_id=%s Notion webhook verification token written to %s; "
            "paste it into the Notion integration and export it as %s",
            tenant,
            path,
            token_env,
        )
        return Response(status_code=200)
    if not token:
        log.warning("tenant_id=%s Notion webhook event refused: %s is not set", tenant, token_env)
        return Response(status_code=503)
    if not verify_notion_signature(body, signature, token):
        reason = "missing" if signature is None else "invalid"
        log.warning("tenant_id=%s Notion webhook event refused: %s signature", tenant, reason)
        return Response(status_code=401)
    if config.notion_workspace_id and payload.get("workspace_id") != config.notion_workspace_id:
        # A genuine event, but from a Notion workspace that is not this
        # tenant's (or that does not say which it is): never act on it.
        log.warning("tenant_id=%s Notion webhook event ignored: another workspace", tenant)
        return Response(status_code=200)
    if payload.get("type") in NOTION_PAGE_EVENTS:
        entity = payload.get("entity") or {}
        page_id = entity.get("id")
        if entity.get("type") == "page" and page_id:
            task_id = await d.links.task_for_page(tenant, normalize_id(page_id))
            if task_id:
                await _signal(
                    d,
                    tenant,
                    task_id,
                    "external_update",
                    ExternalUpdate(page_id=page_id, source="notion_webhook"),
                )
            if d.agents is None:
                raise RuntimeError("the gateway has no agent store to read page grants from")
            published = await d.agents.published_all(tenant)
            if any(a.grants.notion_pages for a in published.values()):
                # Signal-with-start: a burst of events for one page lands in
                # the one running refresh workflow.
                refresh_id = ids.page_refresh_id(tenant, page_id)
                await d.temporal_client.start_workflow(
                    "PageRefreshWorkflow",
                    args=[tenant, normalize_id(page_id)],
                    id=refresh_id,
                    task_queue=TASK_QUEUE,
                    start_signal="changed",
                )
                log.info(
                    "tenant_id=%s workflow_id=%s signalled changed event_type=%s",
                    tenant,
                    refresh_id,
                    payload.get("type"),
                )
    return Response(status_code=200)


def _token_file(directory: str, tenant: str) -> str:
    """The file that holds one tenant's Notion verification token."""
    return os.path.join(directory, f"notion-webhook-verification-token-{tenant}")


def _write_token_file(configured: str, token: str) -> str:
    """Write the verification token to an owner-only file; return its path."""
    path = os.path.expanduser(configured)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, mode=0o700, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(token)
    os.chmod(path, 0o600)
    return path


async def _open_edit_form(
    d: GatewayDeps, tenant: str, payload: dict, action: dict, clicker: str
) -> None:
    """Open the Edit form for a click by the requester on the current card.

    Nothing opens when the clicker is not the requester, the nonce is not the
    draft's current one, or the draft can no longer be edited.
    """
    try:
        value = json.loads(action.get("value") or "{}")
        task_id, nonce = value["task_id"], value["nonce"]
    except (ValueError, KeyError, TypeError):
        log.warning("tenant_id=%s ignored edit click with a bad value", tenant)
        return
    if not task_id.startswith(f"{tenant}:"):
        log.warning("tenant_id=%s ignored edit click for another tenant", tenant)
        return
    draft = await _query_draft(d, tenant, task_id)
    if draft is None or clicker != draft.requester_user_id or nonce != draft.nonce:
        log.info("tenant_id=%s workflow_id=%s edit click ignored", tenant, task_id)
        return
    if not draft.editable:
        log.info("tenant_id=%s workflow_id=%s draft not editable", tenant, task_id)
        return
    if d.slack_for is None:
        log.warning("tenant_id=%s edit form not opened: no Slack client", tenant)
        return
    await d.slack_for(tenant).open_view(payload.get("trigger_id", ""), _edit_view(task_id, draft))


async def _query_draft(d: GatewayDeps, tenant: str, task_id: str) -> DraftView | None:
    handle = d.temporal_client.get_workflow_handle(task_id)
    try:
        return await handle.query("draft", result_type=DraftView)
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            log.info("tenant_id=%s workflow_id=%s not running; edit dropped", tenant, task_id)
            return None
        raise


def _edit_view(task_id: str, draft: DraftView) -> dict:
    """The Edit form: one input per editable field, prefilled from the draft."""
    limits = EDITABLE_FIELDS[draft.tool_name]
    blocks = []
    for name, current in draft.fields.items():
        blocks.append(
            {
                "type": "input",
                "block_id": name,
                "label": {"type": "plain_text", "text": FIELD_LABELS.get(name, name)},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "value",
                    "initial_value": current,
                    "max_length": limits[name],
                    "multiline": name in MULTILINE_FIELDS,
                },
            }
        )
    return {
        "type": "modal",
        "callback_id": EDIT_CALLBACK,
        "private_metadata": json.dumps({"task_id": task_id, "nonce": draft.nonce}),
        "title": {"type": "plain_text", "text": "Edit request"},
        "submit": {"type": "plain_text", "text": "Save"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": blocks,
    }


async def _edit_submitted(d: GatewayDeps, tenant: str, payload: dict) -> Response:
    """Handle the saved Edit form: reject empty values, else signal ``edit``."""
    view = payload.get("view") or {}
    if view.get("callback_id") != EDIT_CALLBACK:
        return Response(status_code=200)
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
        task_id, nonce = meta["task_id"], meta["nonce"]
    except (ValueError, KeyError, TypeError):
        log.warning("tenant_id=%s ignored edit form with bad metadata", tenant)
        return Response(status_code=200)
    if not task_id.startswith(f"{tenant}:"):
        log.warning("tenant_id=%s ignored edit form for another tenant", tenant)
        return Response(status_code=200)
    values = (view.get("state") or {}).get("values") or {}
    fields: dict[str, str] = {}
    errors: dict[str, str] = {}
    for block_id, actions in values.items():
        raw = (actions.get("value") or {}).get("value")
        text = raw.strip() if isinstance(raw, str) else ""
        if not text:
            errors[block_id] = "This can't be empty."
        fields[block_id] = text
    if errors:
        return Response(
            content=json.dumps({"response_action": "errors", "errors": errors}),
            media_type="application/json",
        )
    clicker = (payload.get("user") or {}).get("id", "")
    await _signal(d, tenant, task_id, "edit", EditRequest(nonce, clicker, fields))
    return Response(status_code=200)


async def _signal(d: GatewayDeps, tenant: str, workflow_id: str, name: str, arg: Any) -> None:
    handle = d.temporal_client.get_workflow_handle(workflow_id)
    try:
        await handle.signal(name, arg)
        log.info("tenant_id=%s workflow_id=%s signalled %s", tenant, workflow_id, name)
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            # A stale card or an item whose task has finished.
            log.info(
                "tenant_id=%s workflow_id=%s not running; %s dropped", tenant, workflow_id, name
            )
            return
        raise


@asynccontextmanager
async def _real_lifespan(app: FastAPI):
    """Build the real dependencies when the server starts, not at import."""
    from psycopg_pool import AsyncConnectionPool
    from slack_sdk.signature import SignatureVerifier

    from agentos.agent_store import PostgresAgentStore
    from agentos.config import load_settings, load_tenants_config
    from agentos.crypto_keys import LocalKeyStore, startup_key_failure
    from agentos.slack_client import RealSlackPoster
    from agentos.stores import PostgresTaskLinkStore
    from agentos.temporal_client import build_temporal_client
    from agentos.tenants import build_slack_clients

    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    tenants = load_tenants_config(settings.config_path)
    key_failure = startup_key_failure(LocalKeyStore(settings.key_dir), tenants.tenants)
    if key_failure:
        raise SystemExit(key_failure)
    verifier = SignatureVerifier(settings.require("slack_signing_secret"))
    client = await build_temporal_client(settings)
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    agent_store = PostgresAgentStore(pool)
    try:
        agent_failure = await startup_agent_failure(agent_store, tenants)
        if agent_failure:
            raise SystemExit(agent_failure)
        slack_clients = build_slack_clients(tenants, RealSlackPoster)
        app.state.deps = GatewayDeps(
            settings,
            tenants,
            client,
            PostgresTaskLinkStore(pool),
            verifier,
            slack_clients.__getitem__,
            agent_store,
        )
        yield
    finally:
        await pool.close()


app = create_app(None, None, None, None, None, lifespan=_real_lifespan)
