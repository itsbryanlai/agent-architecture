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
* ``POST /notion/webhook``: a page update becomes an ``external_update`` signal
  to the task watching that page. Polling covers anything the webhook misses.
  Events must carry a valid ``X-Notion-Signature``; the subscription
  handshake's verification token is written to an owner-only file, never logged.

Every Slack request must carry a valid Slack signature. Events from any Slack
workspace other than the configured one are dropped. The tenant comes from
configuration and is part of every workflow ID and log line.
"""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Response
from temporalio.service import RPCError, RPCStatusCode

from agentos import ids
from agentos.config import TASK_QUEUE, AgentsConfig, Settings
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
    agents: AgentsConfig
    temporal_client: Any
    links: TaskLinkStore
    verifier: Any  # slack_sdk.signature.SignatureVerifier or compatible
    slack: Any = None  # a SlackPoster; used to open the Edit form


def create_app(
    settings: Settings | None,
    agents: AgentsConfig | None,
    temporal_client: Any,
    links: TaskLinkStore | None,
    verifier: Any,
    lifespan: Any = None,
    slack: Any = None,
) -> FastAPI:
    """Build the gateway. Tests pass fakes; production fills deps on startup."""
    app = FastAPI(lifespan=lifespan)
    if settings is not None:
        app.state.deps = GatewayDeps(settings, agents, temporal_client, links, verifier, slack)

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
        tenant = d.agents.tenant_id
        if payload.get("type") != "event_callback":
            return Response(status_code=200)
        if payload.get("team_id") != d.agents.slack_team_id:
            log.warning("tenant_id=%s ignored event from another Slack workspace", tenant)
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
            text=LEADING_MENTION.sub("", event.get("text") or "", count=1),
            channel=channel,
            thread_ts=thread_ts,
            channel_kind=channel_kind,
        )
        workflow_id = ids.conversation_id(tenant, channel, thread_ts)
        await d.temporal_client.start_workflow(
            "ConversationWorkflow",
            ConversationRef(tenant, d.agents.default_agent, channel, thread_ts),
            id=workflow_id,
            task_queue=TASK_QUEUE,
            start_signal="message",
            start_signal_args=[msg],
        )
        log.info("tenant_id=%s workflow_id=%s signalled message", tenant, workflow_id)
        return Response(status_code=200)

    @app.post("/slack/interactivity")
    async def slack_interactivity(request: Request) -> Response:
        body = await verified_body(request)
        if body is None:
            return Response(status_code=401)
        d = deps(request)
        tenant = d.agents.tenant_id
        form = parse_qs(body.decode("utf-8"))
        payload = json.loads((form.get("payload") or ["{}"])[0])
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

    @app.post("/notion/webhook")
    async def notion_webhook(request: Request) -> Response:
        d = deps(request)
        tenant = d.agents.tenant_id
        body = await request.body()
        signature = request.headers.get("X-Notion-Signature")
        try:
            payload = json.loads(body)
        except ValueError:
            return Response(status_code=400)
        if not isinstance(payload, dict):
            return Response(status_code=400)
        if isinstance(payload.get("verification_token"), str) and "type" not in payload:
            # One-time subscription handshake, recognised by its body (a token
            # and no event type), whatever its headers. Notion's handshake can
            # carry a signature header, but it cannot be verified: its key is
            # the very token being delivered. The token goes to an owner-only
            # file, never to a log, and the operator pastes it into Notion.
            # Once a token is configured the handshake is closed, so nobody can
            # overwrite the file with a value of their own.
            if d.settings.notion_webhook_token:
                log.warning(
                    "tenant_id=%s Notion webhook handshake refused: a verification token is "
                    "already configured; unset NOTION_WEBHOOK_VERIFICATION_TOKEN and restart "
                    "the gateway to accept a new subscription",
                    tenant,
                )
                return Response(status_code=409)
            path = _write_token_file(
                d.settings.notion_token_file, str(payload["verification_token"])
            )
            log.warning(
                "tenant_id=%s Notion webhook verification token written to %s; "
                "paste it into the Notion integration and export it as "
                "NOTION_WEBHOOK_VERIFICATION_TOKEN",
                tenant,
                path,
            )
            return Response(status_code=200)
        token = d.settings.notion_webhook_token
        if not token:
            log.warning(
                "tenant_id=%s Notion webhook event refused: NOTION_WEBHOOK_VERIFICATION_TOKEN "
                "is not set",
                tenant,
            )
            return Response(status_code=503)
        if not verify_notion_signature(body, signature, token):
            reason = "missing" if signature is None else "invalid"
            log.warning("tenant_id=%s Notion webhook event refused: %s signature", tenant, reason)
            return Response(status_code=401)
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
                if any(a.grants.notion_pages for a in d.agents.agents.values()):
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

    return app


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
    await d.slack.open_view(payload.get("trigger_id", ""), _edit_view(task_id, draft))


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
    from temporalio.client import Client

    from agentos.config import load_agents_config, load_settings
    from agentos.slack_client import RealSlackPoster
    from agentos.stores import PostgresTaskLinkStore

    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    agents = load_agents_config(settings.config_path)
    verifier = SignatureVerifier(settings.require("slack_signing_secret"))
    client = await Client.connect(settings.temporal_address)
    pool = AsyncConnectionPool(settings.database_url, open=False)
    await pool.open()
    slack = RealSlackPoster(settings.require("slack_bot_token"))
    app.state.deps = GatewayDeps(
        settings, agents, client, PostgresTaskLinkStore(pool), verifier, slack
    )
    try:
        yield
    finally:
        await pool.close()


app = create_app(None, None, None, None, None, lifespan=_real_lifespan)
