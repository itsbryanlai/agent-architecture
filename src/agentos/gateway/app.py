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
  An uninstall (``app_uninstalled``, or ``tokens_revoked`` naming bot tokens) disables
  the workspace's tenant and deletes its Slack credential in one unit of work, audited
  ``uninstall``; the Notion credential is kept, and a repeat event changes nothing more.
* ``POST /slack/interactivity``: Submit / Cancel clicks become a ``decision``
  signal to the task named on the card. The gateway passes on who clicked; the
  task workflow decides whether that person may approve. An Edit click opens a
  form prefilled from the task's current draft (only for the requester, only
  while the draft is editable); saving the form stores the values in Postgres and
  becomes an ``edit`` signal that names them. A 👍/👎 click on an answer records the
  signed clicker's vote in Postgres (``answer_feedback``) and audits it as ``feedback``;
  a 👎 also opens a form for an optional one-line reason. No workflow hears of a vote:
  feedback is the user's action, not the agent's. Slack does not retry a click, so a click is
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
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Response
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from agentos import ids
from agentos.agent_store import AgentStore, startup_agent_failure
from agentos.audit import AuditStore
from agentos.config import TASK_QUEUE, TENANT_ID_PATTERN, Settings, TenantsConfig, read_secret
from agentos.connectors.drive import fill_markup_error
from agentos.connectors.google_iam import GoogleAccounts
from agentos.connectors.notion import normalize_id
from agentos.content_store import ContentStore, InMemoryContentStore
from agentos.credentials import SLACK_BOT, TenantCredentials
from agentos.crypto_keys import TenantKeyError
from agentos.drive_connections import DriveConnectionStore
from agentos.feedback import (
    FEEDBACK_ACTIONS,
    FEEDBACK_CALLBACK,
    REASON_MAX,
    THANKS,
    AnswerFeedback,
    FeedbackStore,
)
from agentos.gateway.notion_signature import verify_notion_signature
from agentos.models import (
    EDITABLE_FIELDS,
    ApprovalDecision,
    AudienceEvent,
    AuditEvent,
    DraftView,
    EditRequest,
    ExternalUpdate,
    NudgeRequest,
    blank_limits,
    field_error,
    row_limits,
)
from agentos.notion_webhook import WebhookTokens
from agentos.slack_format import from_slack_text
from agentos.stores import TaskLinkStore
from agentos.switches import WRITES_OFF_TEXT, SwitchStore, read_state
from agentos.tenant_store import TenantSource, TenantStore, as_source
from agentos.unit_of_work import InMemoryUnitOfWork, UnitOfWork

log = logging.getLogger("agentos.gateway")

LEADING_MENTION = re.compile(r"^\s*<@[A-Z0-9]+>\s*")
APPROVAL_ACTIONS = {"task_approve": True, "task_cancel": False}
# The Slack events that end a tenant: the app was uninstalled, or its bot token was
# revoked. Both disable the tenant and delete its Slack credential; Notion is kept.
UNINSTALL_EVENTS = {"app_uninstalled", "tokens_revoked"}
UNINSTALL_IDENTITY = "system:uninstall"
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
NUDGE_ACTION = "task_nudge"
STALE_EDIT = "This request changed after you opened the form. Close it and click Edit again."
FIELD_LABELS = {"title": "Title", "details": "Details", "comment": "Comment", "text": "New text"}
MULTILINE_FIELDS = {"details", "comment", "text"}
# Slack wants an answer to a click within 3 seconds and does not repeat it. The click's work
# gets this long, and the "try again" notice a little less than what is left.
CLICK_TIMEOUT_SECONDS = 2.0
NOTICE_TIMEOUT_SECONDS = 0.7
TRY_AGAIN = "That didn't go through. Please try again in a moment."


def _utcnow() -> datetime:
    return datetime.now(UTC)


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
    # The uninstall flow's stores: the tenants table (a disabled row is still found by
    # its Slack team), the credentials, the audit log and the unit of work that commits
    # the disable, the token deletion and the audit together. None (a gateway built
    # without a database) ignores uninstall events with a log line.
    tenant_store: TenantStore | None = None
    credentials: TenantCredentials | None = None
    audit: AuditStore | None = None
    uow: UnitOfWork | None = None
    # The app's name from the manifest, for the not-set-up reply; "This app" without one.
    app_name: str = "This app"
    # Where answer votes and their reasons are kept. A vote records into ``audit`` in the
    # same ``uow`` transaction (both above, shared with the uninstall flow); create_app
    # defaults ``uow`` for the in-memory stores. None in tests that never click a button.
    feedback: FeedbackStore | None = None
    # When a vote is recorded; tests inject a fixed clock.
    clock: Callable[[], datetime] = _utcnow
    # The uninstall flow's Drive stores (D-45): the connection row is deleted with the
    # disable, the tenant's Google account best effort under the same tenant lock, row
    # or no row. None (a gateway without Drive configured) leaves uninstall as it was.
    drive: DriveConnectionStore | None = None
    google: GoogleAccounts | None = None


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
    feedback: FeedbackStore | None = None,
    audit: AuditStore | None = None,
    uow: UnitOfWork | None = None,
    clock: Callable[[], datetime] = _utcnow,
    tenant_store: TenantStore | None = None,
    credentials: TenantCredentials | None = None,
    drive: DriveConnectionStore | None = None,
    google: GoogleAccounts | None = None,
) -> FastAPI:
    """Build the gateway. Tests pass fakes; production fills deps on startup."""
    app = FastAPI(lifespan=lifespan)
    if settings is not None:
        # A vote and its audit event commit together; the in-memory stores roll back as one
        # unit. Stores in Postgres need a PostgresUnitOfWork, passed by the real lifespan.
        if uow is None and feedback is not None:
            uow = InMemoryUnitOfWork(*[s for s in (feedback, audit) if s is not None])
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
            feedback=feedback,
            audit=audit,
            uow=uow,
            clock=clock,
            tenant_store=tenant_store,
            credentials=credentials,
            drive=drive,
            google=google,
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
        event = payload.get("event") or {}
        if event.get("type") in UNINSTALL_EVENTS:
            # Resolved through the store, not the directory: a repeat or out-of-order
            # event arrives after the tenant is disabled, and must change nothing more.
            await _uninstall(d, team_id, payload, event)
            return Response(status_code=200)
        tenant = await d.tenants.for_slack_team(team_id)
        if tenant is None:
            log.warning("ignored event from an unknown Slack workspace team_id=%s", team_id)
            return Response(status_code=200)

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
                    if (payload.get("view") or {}).get("callback_id") == FEEDBACK_CALLBACK:
                        return await _feedback_reason_submitted(d, tenant, payload)
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
        if action.get("action_id") == NUDGE_ACTION:
            await _nudge_clicked(d, tenant, action, clicker)
            continue
        vote = FEEDBACK_ACTIONS.get(action.get("action_id"))
        if vote is not None:
            await _feedback_clicked(d, tenant, payload, action, clicker, vote)
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


async def _uninstall(d: GatewayDeps, team_id: str | None, payload: dict, event: dict) -> None:
    """Disable the workspace's tenant and delete its Slack credential, once per event.

    In one unit of work: the tenant row disabled, its ``slack_bot`` credential deleted,
    its Drive connection row deleted (D-45) and one ``uninstall`` audit event (the event
    type in ``tool``, the team ID in ``target``, never a token). The Notion credential is
    kept: a reinstall resumes the tenant with its data. The tenant's Google service
    account is deleted at Google under the same tenant lock, best effort: a refusal is
    logged with the tenant ID and never blocks the uninstall (a reconnect finds the same
    account by its deterministic ID). The delete is tried whenever Google is configured,
    row or no row, targeting the row's stored email else the derived account. The event's
    audit key is authoritative: a retry of an event already processed changes nothing even
    after a reinstall re-enabled the tenant and stored a fresh token, while a new uninstall
    event (a new ``event_id``) still acts.
    An out-of-order event with a different ID finds the tenant already disabled with no
    credential and changes nothing more. Both checks sit inside the unit's tenant lock,
    so two events arriving together still record one uninstall. ``tokens_revoked`` acts
    only when bot tokens were revoked; an unknown team is ignored with a log line.
    """
    kind = event.get("type")
    if kind == "tokens_revoked" and not (event.get("tokens") or {}).get("bot"):
        log.info("team_id=%s tokens_revoked names no bot token; ignored", team_id)
        return
    if d.tenant_store is None or d.credentials is None or d.audit is None or d.uow is None:
        log.warning("team_id=%s uninstall event ignored: the gateway has no tenant store", team_id)
        return
    ref = await d.tenant_store.ref_for_slack_team(team_id or "")
    if ref is None:
        log.warning("ignored uninstall event from an unknown Slack workspace team_id=%s", team_id)
        return
    tenant = ref.tenant_id
    event_key = ids.audit_control_key(
        tenant, "uninstall", payload.get("event_id") or uuid.uuid4().hex
    )
    async with d.uow.begin(tenant) as conn:
        if await d.audit.get(tenant, event_key, conn=conn) is not None:
            log.info(
                "tenant_id=%s team_id=%s uninstall event already processed; %s changes nothing",
                tenant,
                team_id,
                kind,
            )
            return
        row = await d.tenant_store.load(tenant)
        if row is None:
            return
        deleted = await d.credentials.delete(tenant, SLACK_BOT, conn=conn)
        if not row.enabled and not deleted:
            log.info(
                "tenant_id=%s team_id=%s already uninstalled; %s changes nothing",
                tenant,
                team_id,
                kind,
            )
            return
        await d.tenant_store.set_enabled(tenant, False, conn=conn)
        drive_row = None if d.drive is None else await d.drive.get(tenant, conn=conn)
        if drive_row is not None:
            await d.drive.delete(tenant, conn=conn)
        await d.audit.record(
            AuditEvent(
                tenant_id=tenant,
                event_key=event_key,
                kind="uninstall",
                agent_id=None,
                agent_version=None,
                requester_user_id=UNINSTALL_IDENTITY,
                identity=UNINSTALL_IDENTITY,
                tool=kind,
                target=team_id,
            ),
            conn=conn,
        )
        if d.google is not None:
            email = (
                drive_row.service_account_email
                if drive_row is not None
                else d.google.account_email(ids.drive_account_id(tenant))
            )
            try:
                await d.google.delete_account(email)
            except Exception as err:  # noqa: BLE001 - the uninstall stands even if Google refuses
                log.warning(
                    "tenant_id=%s drive account not deleted at Google error_type=%s",
                    tenant,
                    type(err).__name__,
                )
            else:
                log.info("tenant_id=%s drive account deleted at Google", tenant)
    log.info("tenant_id=%s team_id=%s uninstalled event=%s", tenant, team_id, kind)


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


async def _nudge_clicked(d: GatewayDeps, tenant: str, action: dict, clicker: str) -> None:
    """Signal the task whose stale notice offered a nudge.

    Like an approval click, the gateway passes on who clicked and the button's nonce;
    the workflow accepts it only from the requester on the live offer. Nothing is
    written by the click itself: the nudge drafts an approval card first.
    """
    try:
        value = json.loads(action.get("value") or "{}")
        task_id, nonce = value["task_id"], value["nonce"]
    except (ValueError, KeyError, TypeError):
        log.warning("tenant_id=%s ignored nudge click with a bad value", tenant)
        return
    if not task_id.startswith(f"{tenant}:"):
        log.warning("tenant_id=%s ignored nudge click for another tenant", tenant)
        return
    await _signal(d, tenant, task_id, "nudge", NudgeRequest(nonce, clicker))


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
    if draft.tool_name == "append_row":
        # An append's editable fields are its draft's columns — one input each, empty for a
        # cell the row left out — the same set apply_edit checks against.
        row = stored.get("row", {})
        return {column: str(row.get(column, "")) for column in stored.get("columns", [])}
    if draft.tool_name == "create_doc":
        # A create-from-template's editable fields are the title and its draft's blanks —
        # one input each, empty for a blank the values left out — the same set apply_edit
        # checks against. A blank named title shares the title's input (the Doc's name).
        values = stored.get("values", {})
        fields = {"title": str(stored.get("title", ""))}
        fields.update(
            {
                blank: str(values.get(blank, ""))
                for blank in stored.get("blanks", [])
                if blank != "title"
            }
        )
        return fields
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
    if draft.tool_name == "append_row":
        limits = row_limits(list(fields))
    elif draft.tool_name == "create_doc":
        limits = blank_limits(list(fields))
    else:
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
                    "multiline": name in MULTILINE_FIELDS
                    or draft.tool_name == "append_row"
                    or (draft.tool_name == "create_doc" and name != "title"),
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

    A value is bad when it is empty (an append's cell excepted: empty clears it) or does
    not fit once escaped, and the whole form is stale when the draft has moved on. Only a
    valid edit reaches the workflow.
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
    for block_id, actions in values.items():
        raw = (actions.get("value") or {}).get("value")
        fields[block_id] = raw.strip() if isinstance(raw, str) else ""
    draft = await _query_draft(d, tenant, task_id)
    errors: dict[str, str] = {}
    if draft is None:
        errors = {next(iter(fields), ""): "This request is no longer open."}
    elif nonce != draft.nonce:
        errors = {next(iter(fields), ""): STALE_EDIT}
    elif not draft.editable:
        errors = {next(iter(fields), ""): "This request can no longer be edited."}
    else:
        # The workflow checks every field again; this is the same check, shown here.
        dynamic = draft.tool_name in ("append_row", "create_doc")
        blanks: list[str] = []
        if draft.tool_name == "create_doc" and draft.revision and d.content is not None:
            stored = await d.content.get_draft(tenant, task_id, draft.revision) or {}
            blanks = list(stored.get("blanks", []))
        for name, value in fields.items():
            # An empty cell clears the column / blank; a create's title and every other
            # field needs a value.
            titled = draft.tool_name == "create_doc" and name == "title"
            if not value and (not dynamic or titled):
                errors[name] = "This can't be empty."
            known = dynamic or name in EDITABLE_FIELDS[draft.tool_name]
            if known and (message := field_error(draft.tool_name, name, value)):
                errors[name] = message
            if draft.tool_name == "create_doc" and (message := fill_markup_error(value, blanks)):
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


def _feedback_ref(tenant: str, action: dict) -> tuple[str, str, str, int] | None:
    """The answer a feedback button names, or None when the value is bad or another tenant's.

    The value carries references only (the conversation ID, the answer's ts, the agent and
    version that answered). The voter is never read from it: that is the signed request's user.
    """
    try:
        value = json.loads(action.get("value") or "{}")
        conversation_id = value["conversation_id"]
        answer_ts = value["answer_ts"]
        agent_id = value["agent_id"]
        agent_version = value["agent_version"]
    except (ValueError, KeyError, TypeError):
        log.warning("tenant_id=%s ignored feedback click with a bad value", tenant)
        return None
    if not isinstance(conversation_id, str) or not conversation_id.startswith(f"{tenant}:"):
        log.warning("tenant_id=%s ignored feedback click for another tenant", tenant)
        return None
    if not (
        isinstance(answer_ts, str)
        and answer_ts
        and isinstance(agent_id, str)
        and agent_id
        and isinstance(agent_version, int)
        and not isinstance(agent_version, bool)
        and agent_version >= 1
    ):
        log.warning("tenant_id=%s ignored feedback click with a bad value", tenant)
        return None
    return conversation_id, answer_ts, agent_id, agent_version


async def _feedback_clicked(
    d: GatewayDeps, tenant: str, payload: dict, action: dict, clicker: str, vote: str
) -> None:
    """Record the signed clicker's vote on the answer the button names, and acknowledge it.

    A second click replaces the voter's earlier vote. Every click is audited as its own event
    (the key carries the vote's time), and the vote and its event commit in one transaction, so
    a failed audit insert leaves no vote behind. A 👎 also opens the reason form. The vote stands
    even when the form or the ephemeral thanks cannot be shown; a failed store write propagates
    and the click is told to try again.
    """
    ref = _feedback_ref(tenant, action)
    if ref is None or not clicker:
        return
    conversation_id, answer_ts, agent_id, agent_version = ref
    if d.feedback is None:
        raise RuntimeError("the gateway has no feedback store to keep a vote in")
    if d.uow is None:
        raise RuntimeError("the gateway has no unit of work to commit a vote with its audit event")
    at = d.clock()
    async with d.uow.begin(tenant) as conn:
        await d.feedback.record_vote(
            AnswerFeedback(
                tenant,
                conversation_id,
                answer_ts,
                clicker,
                agent_id,
                agent_version,
                vote,
                None,
                at,
            ),
            conn=conn,
        )
        if d.audit is not None:
            await d.audit.record(
                AuditEvent(
                    tenant_id=tenant,
                    event_key=ids.audit_feedback_key(conversation_id, answer_ts, clicker, vote, at),
                    kind="feedback",
                    agent_id=agent_id,
                    agent_version=agent_version,
                    requester_user_id=clicker,
                    identity=f"user:{clicker}",
                    conversation_id=conversation_id,
                    tool="feedback",
                    target=vote,
                ),
                conn=conn,
            )
    if vote == "down":
        if d.slack_for is None:
            log.warning("tenant_id=%s feedback form not opened: no Slack client", tenant)
        else:
            try:
                slack = await d.slack_for(tenant)
                await slack.open_view(
                    payload.get("trigger_id", ""), _reason_view(conversation_id, answer_ts)
                )
            except Exception as err:  # noqa: BLE001 - the vote stands without the form
                log.warning(
                    "tenant_id=%s feedback form not opened error_type=%s",
                    tenant,
                    type(err).__name__,
                )
    await _thank_voter(d, tenant, payload, clicker)


def _reason_view(conversation_id: str, answer_ts: str) -> dict:
    """The 👎 reason form: one optional line. The voter is the signed submitter, not metadata."""
    return {
        "type": "modal",
        "callback_id": FEEDBACK_CALLBACK,
        "private_metadata": json.dumps(
            {"conversation_id": conversation_id, "answer_ts": answer_ts}
        ),
        "title": {"type": "plain_text", "text": "Feedback"},
        "submit": {"type": "plain_text", "text": "Send"},
        "close": {"type": "plain_text", "text": "Cancel"},
        "blocks": [
            {
                "type": "input",
                "block_id": "reason",
                "optional": True,
                "label": {"type": "plain_text", "text": "What was wrong? (optional)"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "value",
                    "max_length": REASON_MAX,
                },
            }
        ],
    }


async def _feedback_reason_submitted(d: GatewayDeps, tenant: str, payload: dict) -> Response:
    """Store the optional reason on the submitter's 👎; the vote stands either way.

    An empty reason stores nothing; an over-long one keeps the form open with a message.
    A reason reaches only a vote that is still a 👎: one changed to a 👍 meanwhile keeps none.
    """
    view = payload.get("view") or {}
    try:
        meta = json.loads(view.get("private_metadata") or "{}")
        conversation_id, answer_ts = meta["conversation_id"], meta["answer_ts"]
    except (ValueError, KeyError, TypeError):
        log.warning("tenant_id=%s ignored feedback form with bad metadata", tenant)
        return Response(status_code=200)
    if not isinstance(conversation_id, str) or not conversation_id.startswith(f"{tenant}:"):
        log.warning("tenant_id=%s ignored feedback form for another tenant", tenant)
        return Response(status_code=200)
    if not isinstance(answer_ts, str) or not answer_ts:
        log.warning("tenant_id=%s ignored feedback form with bad metadata", tenant)
        return Response(status_code=200)
    values = (view.get("state") or {}).get("values") or {}
    raw = ((values.get("reason") or {}).get("value") or {}).get("value")
    reason = raw.strip() if isinstance(raw, str) else ""
    if len(reason) > REASON_MAX:
        return _form_errors({"reason": f"Keep it to {REASON_MAX} characters or fewer."})
    voter = (payload.get("user") or {}).get("id", "")
    if reason and voter:
        if d.feedback is None:
            raise RuntimeError("the gateway has no feedback store to keep a reason in")
        await d.feedback.set_reason(tenant, conversation_id, answer_ts, voter, reason)
    return Response(status_code=200)


async def _thank_voter(d: GatewayDeps, tenant: str, payload: dict, clicker: str) -> None:
    """Acknowledge the vote to the voter alone; best effort, the vote is already stored."""
    channel = (payload.get("channel") or {}).get("id") or (payload.get("container") or {}).get(
        "channel_id"
    )
    thread_ts = (payload.get("message") or {}).get("thread_ts")
    if d.slack_for is None or not channel:
        return
    try:
        async with asyncio.timeout(NOTICE_TIMEOUT_SECONDS):
            slack = await d.slack_for(tenant)
            await slack.post_ephemeral(channel, clicker, THANKS, thread_ts)
    except Exception as err:  # noqa: BLE001 - the vote stands even if the thanks fails
        log.warning(
            "tenant_id=%s feedback thanks not shown error_type=%s", tenant, type(err).__name__
        )


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
    from agentos.audit import PostgresAuditStore
    from agentos.config import load_settings
    from agentos.connectors.google_iam import GoogleError, accounts_from_settings
    from agentos.content_store import PostgresContentStore
    from agentos.credentials import PostgresCredentialStore, TenantCredentials
    from agentos.crypto_keys import (
        TenantKeyError,
        build_key_store,
        start_key_refresh,
        startup_key_failure,
    )
    from agentos.drive_connections import PostgresDriveConnectionStore
    from agentos.feedback import PostgresFeedbackStore
    from agentos.gateway.relay import InboundRelay
    from agentos.notion_webhook import PostgresWebhookTokenStore, WebhookTokens
    from agentos.schema_version import require_current_schema
    from agentos.slack_client import RealSlackPoster
    from agentos.slack_install import DEFAULT_MANIFEST_PATH, app_name_from_manifest
    from agentos.stores import PostgresTaskLinkStore
    from agentos.switches import PostgresSwitchStore
    from agentos.temporal_client import build_temporal_client
    from agentos.tenant_store import PostgresTenantStore, TenantDirectory, startup_tenants
    from agentos.unit_of_work import PostgresUnitOfWork

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
        tenant_store = PostgresTenantStore(pool)
        directory = TenantDirectory(tenant_store)
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
        # Drive (D-45): configured only with both deployment settings; a malformed key
        # stops start-up with a plain message, never at the first uninstall.
        try:
            google = accounts_from_settings(
                settings.google_project_id, settings.google_service_account_key
            )
        except GoogleError as err:
            raise SystemExit(str(err)) from err
        drive = None if google is None else PostgresDriveConnectionStore(pool)
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
            feedback=PostgresFeedbackStore(pool),
            tenant_store=tenant_store,
            credentials=credentials,
            audit=PostgresAuditStore(pool),
            uow=PostgresUnitOfWork(pool),
            drive=drive,
            google=google,
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
