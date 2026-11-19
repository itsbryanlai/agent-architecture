"""The ingress gateway.

It acknowledges every request quickly and hands the work to Temporal, without putting what
people wrote into Temporal history:

* ``POST /slack/events``: a DM or an @mention is stored in Postgres (``inbound_events``) and
  acknowledged at once, whether or not Temporal is reachable. A relay loop in this process
  (``agentos.gateway.relay``) then signal-with-starts the thread's ConversationWorkflow with
  the event ID, retrying with a growing delay until Temporal takes it; the workflow reads
  the text from Postgres. Slack retries are harmless: an event ID stores one row, and the
  workflow drops an event ID it has seen. If the event cannot be stored, the gateway answers
  with an error so that Slack sends it again.
  A channel shared with another organisation (``channel_shared``) starts an
  ``AudienceWatchWorkflow`` (one per Slack event ID, never reused), which pauses the
  agents that answer in that channel. People joining a channel start nothing.
* ``POST /slack/interactivity``: Submit / Cancel clicks become a ``decision``
  signal to the task named on the card. The gateway passes on who clicked; the
  task workflow decides whether that person may approve. An Edit click opens a
  form prefilled from the task's current draft (only for the requester, only
  while the draft is editable); saving the form stores the values in Postgres and
  becomes an ``edit`` signal that names them. Slack does not retry a click, so a click is
  handled before the reply, within ``CLICK_TIMEOUT_SECONDS``; when Temporal or the
  database does not answer in time, the person is told to try again (an ephemeral
  message, or an error on the form) and nothing is half done.
* ``POST /notion/webhook/{tenant_id}``: a page update becomes an
  ``external_update`` signal to the task watching that page. Polling covers
  anything the webhook misses. Events must carry a valid ``X-Notion-Signature``,
  made with that tenant's own verification token; the subscription handshake's
  token is stored encrypted in the database as pending, confirmed by a host
  command, and never logged. ``POST /notion/webhook``
  is an alias that works only while exactly one tenant is configured.

Every Slack request must carry a valid Slack signature. The gateway decides the
tenant from the request itself: the Slack ``team_id``, or the Notion webhook
path. It never takes the tenant from text a user wrote, and it drops (with a
log line holding IDs only) what it cannot map to a configured tenant. The
tenant is part of every workflow ID and log line.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Response
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from agentos import ids
from agentos.agent_store import AgentStore, startup_agent_failure
from agentos.config import TASK_QUEUE, TENANT_ID_PATTERN, Settings, TenantsConfig, read_secret
from agentos.connectors.notion import normalize_id
from agentos.content_store import ContentStore, InMemoryContentStore
from agentos.crypto_keys import TenantKeyError
from agentos.gateway.notion_signature import verify_notion_signature
from agentos.models import (
    EDITABLE_FIELDS,
    ApprovalDecision,
    AudienceEvent,
    DraftView,
    EditRequest,
    ExternalUpdate,
    field_error,
)
from agentos.notion_webhook import WebhookTokens
from agentos.slack_format import from_slack_text
from agentos.stores import TaskLinkStore
from agentos.switches import WRITES_OFF_TEXT, SwitchStore, read_state
from agentos.tenant_store import TenantSource, as_source

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
STALE_EDIT = "This request changed after you opened the form. Close it and click Edit again."
FIELD_LABELS = {"title": "Title", "details": "Details", "comment": "Comment"}
MULTILINE_FIELDS = {"details", "comment"}
# Slack wants an answer to a click within 3 seconds and does not repeat it. The click's work
# gets this long, and the "try again" notice a little less than what is left.
CLICK_TIMEOUT_SECONDS = 2.0
NOTICE_TIMEOUT_SECONDS = 0.7
TRY_AGAIN = "That didn't go through. Please try again in a moment."


@dataclass
class GatewayDeps:
    settings: Settings
    tenants: TenantSource
    temporal_client: Any
    links: TaskLinkStore
    verifier: Any  # slack_sdk.signature.SignatureVerifier or compatible
    # A tenant's own SlackPoster, used to open the Edit form in that tenant's
    # workspace and to answer a tenant that is not set up yet. Unset in tests that
    # never post. Async: the token is resolved from the tenant's credential row.
    slack_for: Callable[[str], Awaitable[Any]] | None = None
    # The published agents. A Notion page event is acted on only when some
    # published agent of the tenant is granted pages. Unset in tests that never
    # send a page event.
    agents: AgentStore | None = None
    # The kill switches, read afresh at every Submit. None means none is ever off.
    switches: SwitchStore | None = None
    # Where inbound events and saved edits are kept, and the draft revisions an Edit form shows.
    content: ContentStore | None = None
    # Called after an event was stored, so the relay delivers it without waiting for its next pass.
    on_inbound: Callable[[], None] | None = None
    # The Notion webhook verification tokens, encrypted per tenant. None keeps the
    # local file flow for a gateway built without a database.
    webhook_tokens: WebhookTokens | None = None
    # The app's name from the manifest, for the not-set-up reply; "This app" without one.
    app_name: str = "This app"


def create_app(
    settings: Settings | None,
    tenants: TenantsConfig | TenantSource | None,
    temporal_client: Any,
    links: TaskLinkStore | None,
    verifier: Any,
    lifespan: Any = None,
    slack_for: Callable[[str], Awaitable[Any]] | None = None,
    agents: AgentStore | None = None,
    switches: SwitchStore | None = None,
    content: ContentStore | None = None,
    webhook_tokens: WebhookTokens | None = None,
    app_name: str = "This app",
) -> FastAPI:
    """Build the gateway. Tests pass fakes; production fills deps on startup."""
    app = FastAPI(lifespan=lifespan)
    if settings is not None:
        app.state.deps = GatewayDeps(
            settings,
            as_source(tenants),
            temporal_client,
            links,
            verifier,
            slack_for,
            agents,
            switches,
            content or InMemoryContentStore(),
            webhook_tokens=webhook_tokens,
            app_name=app_name,
        )

    def deps(request: Request) -> GatewayDeps:
        return request.app.state.deps

    async def verified_body(request: Request) -> bytes | Response:
        """The body of a correctly signed request, else the refusing response."""
        body = await _read_limited_body(request)
        if body is None:
            return Response(status_code=413)
        ok = deps(request).verifier.is_valid(
            body=body,
            timestamp=request.headers.get("X-Slack-Request-Timestamp"),
            signature=request.headers.get("X-Slack-Signature"),
        )
        return body if ok else Response(status_code=401)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.post("/slack/events")
    async def slack_events(request: Request) -> Response:
        body = await verified_body(request)
        if isinstance(body, Response):
            return body
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
        tenant = await d.tenants.for_slack_team(team_id)
        if tenant is None:
            log.warning("ignored event from an unknown Slack workspace team_id=%s", team_id)
            return Response(status_code=200)

        event = payload.get("event") or {}
        watched = _audience_event(tenant, payload, event)
        if watched is not None:
            await _start_audience_watch(d, watched)
            return Response(status_code=200)
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
        event_id = payload.get("event_id") or f"{channel}:{event['ts']}"
        config = await d.tenants.get(tenant)
        if config is None:
            return Response(status_code=200)
        # A running thread keeps its own agent: the agent named here is the start argument,
        # which is ignored when the workflow exists.
        agent_id = config.default_agent
        if channel_kind == "channel":
            agent_id = config.channels.get(channel, agent_id)
        if d.content is None:
            raise RuntimeError("the gateway has no content store to keep inbound events in")
        inbound = {
            "kind": "message",
            "agent_id": agent_id,
            "user_id": event["user"],
            "text": from_slack_text(LEADING_MENTION.sub("", event.get("text") or "", count=1)),
            "channel": channel,
            "thread_ts": thread_ts,
            "channel_kind": channel_kind,
        }
        if agent_id is None:
            # A tenant installed but not set up yet: one fixed reply, no model call and
            # no Notion call. The event is stored already delivered, so a Slack retry
            # posts nothing and the relay never hands it to a workflow.
            stored = await d.content.put_inbound(tenant, event_id, inbound, delivered=True)
            if stored:
                await _tell_not_set_up(d, tenant, channel, thread_ts)
            log.info(
                "tenant_id=%s event_id=%s stored=%s not-set-up reply", tenant, event_id, stored
            )
            return Response(status_code=200)
        stored = await d.content.put_inbound(tenant, event_id, inbound)
        if d.on_inbound is not None:
            d.on_inbound()
        log.info(
            "tenant_id=%s agent_id=%s event_id=%s stored=%s inbound event",
            tenant,
            agent_id,
            event_id,
            stored,
        )
        return Response(status_code=200)

    @app.post("/slack/interactivity")
    async def slack_interactivity(request: Request) -> Response:
        body = await verified_body(request)
        if isinstance(body, Response):
            return body
        d = deps(request)
        form = parse_qs(body.decode("utf-8"))
        payload = json.loads((form.get("payload") or ["{}"])[0])
        team_id = (payload.get("team") or {}).get("id")
        tenant = await d.tenants.for_slack_team(team_id)
        if tenant is None:
            log.warning("ignored interaction from an unknown Slack workspace team_id=%s", team_id)
            return Response(status_code=200)
        if payload.get("type") == "view_submission":
            try:
                async with asyncio.timeout(CLICK_TIMEOUT_SECONDS):
                    return await _edit_submitted(d, tenant, payload)
            except Exception as err:  # noqa: BLE001 - the form stays open with a message
                log.warning(
                    "tenant_id=%s edit form not saved error_type=%s", tenant, type(err).__name__
                )
                return _form_errors({_first_block(payload): TRY_AGAIN})
        if payload.get("type") != "block_actions":
            return Response(status_code=200)
        clicker = (payload.get("user") or {}).get("id", "")
        try:
            async with asyncio.timeout(CLICK_TIMEOUT_SECONDS):
                await _handle_actions(d, tenant, payload, clicker)
        except Exception as err:  # noqa: BLE001 - tell the person, the click is not repeated
            log.warning("tenant_id=%s click not handled error_type=%s", tenant, type(err).__name__)
            await _tell_try_again(d, tenant, payload, clicker)
        return Response(status_code=200)

    @app.post("/notion/webhook/{tenant_id}")
    async def notion_webhook(tenant_id: str, request: Request) -> Response:
        return await _notion_webhook(deps(request), tenant_id, request)

    @app.post("/notion/webhook")
    async def notion_webhook_single_tenant(request: Request) -> Response:
        # Kept so a subscription made before there were several tenants keeps
        # working: it is the only tenant's, while exactly one is configured.
        d = deps(request)
        tenant_id = await d.tenants.only_tenant()
        if tenant_id is None:
            return Response(status_code=404)
        return await _notion_webhook(d, tenant_id, request)

    return app


async def _handle_actions(d: GatewayDeps, tenant: str, payload: dict, clicker: str) -> None:
    """Act on the buttons of one click. A failure propagates: the click is told to try again."""
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
        if approved and await _writes_off(d, tenant):
            # Refused before the workflow hears of the click: the card stays as it
            # is, so the requester can Submit again once writes are back on.
            await _tell_writes_off(d, tenant, payload)
            continue
        await _signal(d, tenant, task_id, "decision", ApprovalDecision(nonce, clicker, approved))


def _form_errors(errors: dict[str, str]) -> Response:
    return Response(
        content=json.dumps({"response_action": "errors", "errors": errors}),
        media_type="application/json",
    )


def _first_block(payload: dict) -> str:
    values = ((payload.get("view") or {}).get("state") or {}).get("values") or {}
    return next(iter(values), "")


async def _tell_try_again(d: GatewayDeps, tenant: str, payload: dict, clicker: str) -> None:
    """Show the clicker, and only them, that their click did not go through."""
    channel = (payload.get("channel") or {}).get("id") or (payload.get("container") or {}).get(
        "channel_id"
    )
    thread_ts = (payload.get("message") or {}).get("thread_ts")
    if d.slack_for is None or not channel or not clicker:
        return
    try:
        async with asyncio.timeout(NOTICE_TIMEOUT_SECONDS):
            slack = await d.slack_for(tenant)
            await slack.post_ephemeral(channel, clicker, TRY_AGAIN, thread_ts)
    except Exception as err:  # noqa: BLE001 - nothing more can be done for this click
        log.warning(
            "tenant_id=%s try-again notice not shown error_type=%s", tenant, type(err).__name__
        )


def _audience_event(tenant: str, payload: dict, event: dict) -> AudienceEvent | None:
    """The audience event a Slack event is, or None.

    Only a channel being shared with another organisation (``channel_shared``) is one.
    Who joins a channel is the admins' responsibility, so ``member_joined_channel`` falls
    through to the ordinary handling, which ignores it.
    """
    channel = event.get("channel")
    if event.get("type") != "channel_shared" or not isinstance(channel, str) or not channel:
        return None
    event_id = payload.get("event_id") or f"shared:{channel}:{event.get('event_ts')}"
    return AudienceEvent(tenant, event_id, channel)


async def _start_audience_watch(d: GatewayDeps, event: AudienceEvent) -> None:
    """Start the workflow for one audience event; a repeat of the event starts nothing."""
    workflow_id = ids.audience_event_workflow_id(event.tenant_id, event.event_id)
    try:
        await d.temporal_client.start_workflow(
            "AudienceWatchWorkflow",
            event,
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        log.info(
            "tenant_id=%s workflow_id=%s audience event already handled",
            event.tenant_id,
            workflow_id,
        )
        return
    log.info(
        "tenant_id=%s workflow_id=%s started audience watch",
        event.tenant_id,
        workflow_id,
    )


# Slack events are a few KB and an interactivity payload (a form submission) well
# under 100 KB; Notion events are smaller still. 1 MiB leaves ample room and keeps
# an unauthenticated caller from making the gateway hold a large body.
MAX_WEBHOOK_BODY = 1024 * 1024

# A Notion verification token is a short opaque string.
MAX_VERIFICATION_TOKEN = 1024


async def _read_limited_body(request: Request) -> bytes | None:
    """The request body, or None when it is over ``MAX_WEBHOOK_BODY``.

    Read in chunks and abandoned at the limit, so an oversized body is never
    held whole; this runs before a signature check or any parsing.
    """
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_WEBHOOK_BODY:
        return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_WEBHOOK_BODY:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def _notion_webhook(d: GatewayDeps, tenant: str, request: Request) -> Response:
    """Handle one Notion webhook request for the tenant its URL names.

    The tenant comes from the path, never from the body. The signature is
    checked with that tenant's own verification token: the variable its config
    names when set, else the token confirmed in the database, so another
    tenant's token cannot pass.
    """
    if not TENANT_ID_PATTERN.fullmatch(tenant):
        # Never look up, or build a file name from, a path that is not a tenant ID.
        return Response(status_code=404)
    config = await d.tenants.get(tenant)
    if config is None:
        log.warning("Notion webhook for an unknown tenant tenant_id=%s", tenant)
        return Response(status_code=404)
    token_env = config.secrets.notion_webhook_token_env
    body = await _read_limited_body(request)
    if body is None:
        return Response(status_code=413)
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
        # the very token being delivered. With a token store the token is
        # encrypted under the tenant's payload key and stored pending; the
        # operator prints it once with agentos.notion_webhook show, pastes it
        # into Notion and confirms it. Without one (a gateway built without a
        # database) it goes to an owner-only file as before. Either way it
        # never reaches a log. The handshake is closed once a token is
        # configured, and while a token is pending or active: the insert
        # cannot replace one, and the file is created exclusively and never
        # followed through a symlink. The operator clears the token to start
        # over.
        if set(payload) != {"verification_token"} or not (
            0 < len(payload["verification_token"]) <= MAX_VERIFICATION_TOKEN
        ):
            log.warning("tenant_id=%s Notion webhook handshake refused: malformed body", tenant)
            return Response(status_code=400)
        if token:
            log.warning(
                "tenant_id=%s Notion webhook handshake refused: a verification token is "
                "already configured; unset %s and restart the gateway to accept a new "
                "subscription",
                tenant,
                token_env,
            )
            return Response(status_code=409)
        if d.webhook_tokens is not None:
            try:
                stored = await d.webhook_tokens.accept_handshake(
                    tenant, payload["verification_token"]
                )
            except TenantKeyError:
                log.warning(
                    "tenant_id=%s Notion webhook handshake refused: the tenant has no usable "
                    "encryption key",
                    tenant,
                )
                return Response(status_code=503)
            if not stored:
                log.warning(
                    "tenant_id=%s Notion webhook handshake refused: a verification token is "
                    "already pending or active; clear it with: "
                    "uv run python -m agentos.notion_webhook clear --tenant %s",
                    tenant,
                    tenant,
                )
                return Response(status_code=409)
            log.warning(
                "tenant_id=%s Notion webhook verification token pending; print it once with: "
                "uv run python -m agentos.notion_webhook show --tenant %s",
                tenant,
                tenant,
            )
            return Response(status_code=200)
        try:
            path = _write_token_file(
                _token_file(d.settings.notion_token_dir, tenant), payload["verification_token"]
            )
        except FileExistsError:
            log.warning(
                "tenant_id=%s Notion webhook handshake refused: the token file already "
                "exists; delete it to accept a new subscription",
                tenant,
            )
            return Response(status_code=409)
        except OSError:
            # A symlink at the path (O_NOFOLLOW) or a path we cannot write.
            log.warning(
                "tenant_id=%s Notion webhook handshake refused: the token file cannot be "
                "created safely",
                tenant,
            )
            return Response(status_code=409)
        log.warning(
            "tenant_id=%s Notion webhook verification token written to %s; "
            "paste it into the Notion integration and export it as %s",
            tenant,
            path,
            token_env,
        )
        return Response(status_code=200)
    if not token and d.webhook_tokens is not None:
        token = await d.webhook_tokens.active(tenant)
    if not token:
        log.warning(
            "tenant_id=%s Notion webhook event refused: no active verification token "
            "(%s is not set)",
            tenant,
            token_env,
        )
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
    """Create the owner-only token file and write the token; return its path.

    Raises ``FileExistsError`` when the path exists (a symlink counts, and is
    never followed) so an existing token is never replaced.
    """
    path = os.path.expanduser(configured)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, mode=0o700, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(token)
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
    fields = draft.fields or await _stored_fields(d, tenant, task_id, draft)
    if not fields:
        log.warning("tenant_id=%s workflow_id=%s edit form not opened: no draft", tenant, task_id)
        return
    slack = await d.slack_for(tenant)
    await slack.open_view(payload.get("trigger_id", ""), _edit_view(task_id, draft, fields))


async def _stored_fields(d: GatewayDeps, tenant: str, task_id: str, draft: DraftView) -> dict:
    """The editable values of the draft revision a task with a stored payload names."""
    if not draft.revision or d.content is None:
        return {}
    stored = await d.content.get_draft(tenant, task_id, draft.revision) or {}
    return {k: stored[k] for k in EDITABLE_FIELDS.get(draft.tool_name, {}) if k in stored}


async def _query_draft(d: GatewayDeps, tenant: str, task_id: str) -> DraftView | None:
    handle = d.temporal_client.get_workflow_handle(task_id)
    try:
        return await handle.query("draft", result_type=DraftView)
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            log.info("tenant_id=%s workflow_id=%s not running; edit dropped", tenant, task_id)
            return None
        raise


def _edit_view(task_id: str, draft: DraftView, fields: dict) -> dict:
    """The Edit form: one input per editable field, prefilled from the draft."""
    limits = EDITABLE_FIELDS[draft.tool_name]
    blocks = []
    for name, current in fields.items():
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
    """Handle the saved Edit form: keep the form open on a bad value, else signal ``edit``.

    A value is bad when it is empty or does not fit once escaped, and the whole form is
    stale when the draft has moved on. Only a valid edit reaches the workflow.
    """
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
        return _form_errors(errors)
    draft = await _query_draft(d, tenant, task_id)
    if draft is None:
        errors = {next(iter(fields), ""): "This request is no longer open."}
    elif nonce != draft.nonce:
        errors = {next(iter(fields), ""): STALE_EDIT}
    elif not draft.editable:
        errors = {next(iter(fields), ""): "This request can no longer be edited."}
    else:
        # The workflow checks every field again; this is the same check, shown here.
        for name, value in fields.items():
            known = name in EDITABLE_FIELDS[draft.tool_name]
            if known and (message := field_error(draft.tool_name, name, value)):
                errors[name] = message
    if errors:
        return _form_errors(errors)
    clicker = (payload.get("user") or {}).get("id", "")
    if draft is not None and draft.revision:
        # The values stay in Postgres; the workflow is told which row holds them.
        if d.content is None:
            raise RuntimeError("the gateway has no content store to keep an edit in")
        edit_id = ids.edit_event_id(uuid.uuid4().hex)
        await d.content.put_inbound(
            tenant, edit_id, {"kind": "edit", "task_id": task_id, "fields": fields}, delivered=True
        )
        await _signal(d, tenant, task_id, "edit", EditRequest(nonce, clicker, {}, edit_id))
    else:
        await _signal(d, tenant, task_id, "edit", EditRequest(nonce, clicker, fields))
    return Response(status_code=200)


async def _writes_off(d: GatewayDeps, tenant: str) -> bool:
    """Has an admin turned writes off for the tenant? Read now; a failed read fails the click."""
    if d.switches is None:
        return False
    return (await read_state(d.switches, tenant)).writes_off


async def _tell_writes_off(d: GatewayDeps, tenant: str, payload: dict) -> None:
    """Answer a refused Submit in the card's conversation. IDs only in the log."""
    log.info("tenant_id=%s submit refused: writes are turned off", tenant)
    channel = (payload.get("channel") or {}).get("id") or (payload.get("container") or {}).get(
        "channel_id"
    )
    thread_ts = (payload.get("message") or {}).get("ts")
    if d.slack_for is None or not channel:
        return
    try:
        slack = await d.slack_for(tenant)
        await slack.post_message(channel, WRITES_OFF_TEXT, thread_ts=thread_ts)
    except Exception as err:  # noqa: BLE001 - the refusal stands even if the notice fails
        log.warning(
            "tenant_id=%s writes-off notice not posted error_type=%s", tenant, type(err).__name__
        )


def not_set_up_text(console_url: str | None, app_name: str = "This app") -> str:
    """The fixed reply of a tenant that is installed but has no agent yet."""
    if console_url:
        return (
            f"{app_name} is installed but not set up yet. "
            f"An admin can finish setup at {console_url}."
        )
    return (
        f"{app_name} is installed but not set up yet. An admin can finish setup in the web console."
    )


async def _tell_not_set_up(d: GatewayDeps, tenant: str, channel: str, thread_ts: str) -> None:
    """Post the fixed not-set-up reply once per event; best effort, like the other notices."""
    if d.slack_for is None:
        return
    try:
        slack = await d.slack_for(tenant)
        await slack.post_message(
            channel, not_set_up_text(d.settings.console_url, d.app_name), thread_ts=thread_ts
        )
    except Exception as err:  # noqa: BLE001 - the event is already answered; the notice may fail
        log.warning(
            "tenant_id=%s not-set-up reply not posted error_type=%s", tenant, type(err).__name__
        )


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
    from agentos.config import load_settings
    from agentos.content_store import PostgresContentStore
    from agentos.credentials import PostgresCredentialStore, TenantCredentials
    from agentos.crypto_keys import (
        TenantKeyError,
        build_key_store,
        start_key_refresh,
        startup_key_failure,
    )
    from agentos.gateway.relay import InboundRelay
    from agentos.notion_webhook import PostgresWebhookTokenStore, WebhookTokens
    from agentos.schema_version import require_current_schema
    from agentos.slack_client import RealSlackPoster
    from agentos.slack_install import DEFAULT_MANIFEST_PATH, app_name_from_manifest
    from agentos.stores import PostgresTaskLinkStore
    from agentos.switches import PostgresSwitchStore
    from agentos.temporal_client import build_temporal_client
    from agentos.tenant_store import PostgresTenantStore, TenantDirectory, startup_tenants

    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    verifier = SignatureVerifier(settings.require("slack_signing_secret"))
    pool = AsyncConnectionPool(settings.require("database_url"), open=False, **settings.pool_args())
    await pool.open()
    agent_store = PostgresAgentStore(pool)
    relay_task: asyncio.Task | None = None
    key_refresh: asyncio.Task | None = None
    try:
        await require_current_schema(pool)
        directory = TenantDirectory(PostgresTenantStore(pool))
        tenants = await startup_tenants(directory)
        try:
            keys = await build_key_store(settings, pool)
        except TenantKeyError as err:
            raise SystemExit(str(err)) from err
        key_failure = startup_key_failure(keys, tenants.tenants)
        if key_failure:
            raise SystemExit(key_failure)
        agent_failure = await startup_agent_failure(agent_store, tenants)
        if agent_failure:
            raise SystemExit(agent_failure)
        client = await build_temporal_client(settings, keys)
        credentials = TenantCredentials(PostgresCredentialStore(pool), keys)
        slack_clients: dict[str, tuple[str, RealSlackPoster]] = {}

        async def slack_for(tenant_id: str) -> RealSlackPoster:
            # Built on first use from the tenant's resolved token and rebuilt when the
            # credential changes, so a tenant added while the gateway runs gets its own
            # client; a tenant whose token is unavailable fails its own form, not the
            # gateway.
            config = directory.latest().get(tenant_id)
            if config is None:
                raise KeyError(tenant_id)
            token = await credentials.slack_bot_token(config)
            entry = slack_clients.get(tenant_id)
            if entry is None or entry[0] != token:
                entry = (token, RealSlackPoster(token, tenant_id=tenant_id))
                slack_clients[tenant_id] = entry
            return entry[1]

        # A tenant whose Slack token is unavailable is refused at start-up, as before.
        for config in tenants.tenants.values():
            await slack_for(config.tenant_id)
        content = PostgresContentStore(pool)
        relay = InboundRelay(content, directory, client)
        app.state.deps = GatewayDeps(
            settings,
            directory,
            client,
            PostgresTaskLinkStore(pool),
            verifier,
            slack_for,
            agent_store,
            switches=PostgresSwitchStore(pool),
            content=content,
            on_inbound=relay.wake,
            webhook_tokens=WebhookTokens(PostgresWebhookTokenStore(pool), keys),
            app_name=app_name_from_manifest(DEFAULT_MANIFEST_PATH),
        )
        # Events stored while the gateway was down, or while Temporal was, are delivered now.
        relay_task = asyncio.create_task(relay.run(), name="inbound-relay")
        key_refresh = start_key_refresh(keys)
        yield
    finally:
        if relay_task is not None:
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)
        if key_refresh is not None:
            key_refresh.cancel()
            await asyncio.gather(key_refresh, return_exceptions=True)
        await pool.close()


app = create_app(None, None, None, None, None, lifespan=_real_lifespan)
